#include "sai_redis.h"
#include "sairedis.h"
#include "sai_redis_idempotent_internal.h"
#include "meta/sai_serialize.h"
#include "meta/saiattributelist.h"

extern std::shared_ptr<swss::RedisClient>          g_redisRestoreClient;
/* The in memory mapping for quick lookup:  ATTR2OID and DEFAULT_ATTR2OID */
static std::unordered_map<std::string, sai_object_id_t> attr2oid;

/*
 * The in memory mapping for quick lookup:
 * (str_object_type + ":" + serialized_object_id) to owner
 */
static std::unordered_map<std::string, std::string> oid2owner;

static sai_object_id_t redis_attr_to_oid_db_lookup(
     _In_ const std::string &attrFvStr)
{
    auto oid_map = g_redisRestoreClient->hgetall(attrFvStr);
    if (oid_map.size() > 1)
    {
        SWSS_LOG_ERROR("RESTORE: db lookup key: Fields: %s has %d keys",
                attrFvStr.c_str(), oid_map.size());
        return SAI_NULL_OBJECT_ID;
    }
    else if (oid_map.size() == 1)
    {
        for (auto &kv: oid_map)
        {
            sai_object_id_t objectId;

            //key is in format of str_object_type + ":" + serialized_object_id
            auto key = kv.first;
            size_t found = key.find(':');
            if (found != std::string::npos)
            {
                sai_deserialize_object_id(key.substr(found+1), objectId);
                SWSS_LOG_INFO("RESTORE: db lookup key: %s, fields: %s", key.c_str(), attrFvStr.c_str());
                return objectId;
            }
            else
            {
                SWSS_LOG_ERROR("RESTORE: Invalid OID: %s for fields %s", key.c_str(), attrFvStr.c_str());
                return SAI_NULL_OBJECT_ID;
            }
        }
    }
    SWSS_LOG_INFO("RESTORE: db lookup return empty for fields: %s", attrFvStr.c_str());
    return SAI_NULL_OBJECT_ID;
}

void redis_attr_to_oid_map_restore(void)
{
    sai_object_id_t object_id;

    for (const auto &attrFvStr: g_redisRestoreClient->keys(DEFAULT_ATTR2OID_PREFIX + "*"))
    {
        object_id = redis_attr_to_oid_db_lookup(attrFvStr);
        if (object_id != SAI_NULL_OBJECT_ID)
        {
            attr2oid[attrFvStr] = object_id;
        }
    }

    for (const auto &attrFvStr: g_redisRestoreClient->keys(ATTR2OID_PREFIX + "*"))
    {
        object_id = redis_attr_to_oid_db_lookup(attrFvStr);
        if (object_id != SAI_NULL_OBJECT_ID)
        {
            attr2oid[attrFvStr] = object_id;
        }
    }
}

static sai_object_id_t redis_attr_to_oid_map_lookup(
    _In_ const std::string &attrFvStr)
{
    if(attr2oid.find(attrFvStr) == attr2oid.end())
    {
        return SAI_NULL_OBJECT_ID;
    }

    return attr2oid[attrFvStr];
}

static void redis_attr_to_oid_map_insert(
    _In_ const std::string &attrFvStr, sai_object_id_t object_id)
{
    attr2oid[attrFvStr] = object_id;
}

static void redis_attr_to_oid_map_erase(
    _In_ const std::string &attrFvStr)
{
    attr2oid.erase(attrFvStr);
}

void redis_oid_to_owner_map_restore(void)
{
    for (const auto &ownerKey: g_redisRestoreClient->keys(OBJ_OWNER_PREFIX + std::string("*")))
    {
        auto ptr_owner_str = g_redisRestoreClient->hget(ownerKey, "owner");
        if (ptr_owner_str != NULL)
        {
            auto key = ownerKey.substr(strlen(OBJ_OWNER_PREFIX));
            oid2owner[key] = (*ptr_owner_str);
            SWSS_LOG_INFO("RESTORE: owner map restore: %s, owner: %s", key.c_str(), ptr_owner_str->c_str());
        }
    }
}

// key = str_object_type + ":" + serialized_object_id;
static std::string redis_oid_to_owner_map_lookup(
    _In_ const std::string &key)
{
    if(oid2owner.find(key) == oid2owner.end())
    {
        return "";
    }
    return oid2owner[key];
}

static void redis_oid_to_owner_map_insert(
    _In_ const std::string &key,
    _In_ const std::string &owner)
{
    oid2owner[key] = owner;
}

static void redis_oid_to_owner_map_erase(
    _In_ const std::string &key)
{
    oid2owner.erase(key);
}


// If same attributes provided for object create, the original OID should be returned
// without generating new OID nor sending create request down.
sai_status_t redis_idempotent_create(
        _In_ sai_object_type_t object_type,
        _Out_ sai_object_id_t* object_id,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    std::string str_object_type = sai_serialize_object_type(object_type);
    std::vector<swss::FieldValueTuple> entry = SaiAttributeList::serialize_attr_list(
            object_type,
            attr_count,
            attr_list,
            false);

    if (entry.size() == 0)
    {
        // make sure that we put object into db
        // even if there are no attributes set
        swss::FieldValueTuple null("NULL", "NULL");

        entry.push_back(null);
        SWSS_LOG_DEBUG("Empty attribute list for key creation of type %s", str_object_type.c_str());
    }

    // Join fv in order is critical for upgrade and restart, for people might re-arrange attribute in new version of code
    // which should be avoided though, also individulal attribute set may change the order too.
    // It is assumed that for each individual entry, the internal order would not change before and after restart.
    std::string fvStr = joinOrderedFieldValues(entry);
    std::string key;

    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        auto ptr_object_id_str = g_redisRestoreClient->hget("SWITCH", "switch_oid");
        if (ptr_object_id_str != NULL)
        {
            sai_deserialize_object_id(*ptr_object_id_str, *object_id);

            key = str_object_type + ":" + *ptr_object_id_str;

            SWSS_LOG_DEBUG("RESTORE: skipping generic create key: %s, fields: %s", key.c_str(), fvStr.c_str());
            return SAI_STATUS_SUCCESS;
        }
    }
    else
    {
        std::string attrFvStr = ATTR2OID_PREFIX + fvStr;

        *object_id = redis_attr_to_oid_map_lookup(attrFvStr);
        if (*object_id == SAI_NULL_OBJECT_ID)
        {
            attrFvStr = DEFAULT_ATTR2OID_PREFIX + fvStr;
            *object_id = redis_attr_to_oid_map_lookup(attrFvStr);
        }
        if (*object_id != SAI_NULL_OBJECT_ID)
        {
            SWSS_LOG_INFO("RESTORE: skipping generic create key: %s:%s, fields: %s",
                    str_object_type.c_str(), sai_serialize_object_id(*object_id).c_str(), fvStr.c_str());
            return SAI_STATUS_SUCCESS;
        }
    }

    // on create vid is put in db by syncd
    *object_id = redis_create_virtual_object_id(object_type, switch_id);

    if (*object_id == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_ERROR("failed to create %s, with switch id: %s",
                sai_serialize_object_type(object_type).c_str(),
                sai_serialize_object_id(switch_id).c_str());

        return SAI_STATUS_INSUFFICIENT_RESOURCES;
    }

    std::string serialized_object_id = sai_serialize_object_id(*object_id);
    key = str_object_type + ":" + serialized_object_id;
    SWSS_LOG_DEBUG("generic create key: %s, fields: %lu", key.c_str(), entry.size());

    if (g_record)
    {
        recordLine("c|" + key + "|" + fvStr);
    }

    g_asicState->set(key, entry, "create");

    SWSS_LOG_DEBUG("RESTORE: generic create key: %s, fields: %s", key.c_str(), fvStr.c_str());
    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        // Attributes for switch create contains running time function pointers which will be changed after restart
        g_redisRestoreClient->hset("SWITCH", "switch_oid", serialized_object_id);
    }
    else
    {
        redis_attr_to_oid_map_insert(ATTR2OID_PREFIX + fvStr, *object_id);
        g_redisRestoreClient->hset(ATTR2OID_PREFIX + fvStr, key, "NULL");
        g_redisRestoreClient->hmset(OID2ATTR_PREFIX + key, entry.begin(), entry.end());
        if (g_objectOwner != "")
        {
            redis_oid_to_owner_map_insert(key, g_objectOwner);
            g_redisRestoreClient->hset(OBJ_OWNER_PREFIX + key, "owner", g_objectOwner);
        }
    }

    // we assume create will always succeed which may not be true
    // we should make this synchronous call
    return SAI_STATUS_SUCCESS;
}

// It is for non-sai_object_id_t object create only
sai_status_t internal_redis_idempotent_create(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key,   // in format of str_object_type + ":" + serialized_object_id
        _In_ const std::vector<swss::FieldValueTuple> &attr_entry)
{
    SWSS_LOG_ENTER();

    std::string fvStr = joinFieldValues(attr_entry);

    std::string restoreKey = OID2ATTR_PREFIX + obj_key;

    // For non-sai_object_id_t object, the key may be used directly.
    auto exist = g_redisRestoreClient->exists(restoreKey);
    if (exist > 0)
    {
        SWSS_LOG_INFO("RESTORE: skipping generic create key: %s, fields: %s", obj_key.c_str(), fvStr.c_str());
        return SAI_STATUS_SUCCESS;
    }

    g_redisRestoreClient->hmset(OID2ATTR_PREFIX + obj_key,  attr_entry.begin(), attr_entry.end());

    if (g_record)
    {
        recordLine("c|" + obj_key + "|" + fvStr);
    }

    g_asicState->set(obj_key, attr_entry, "create");

    // we assume create will always succeed which may not be true
    // we should make this synchronous call
    return SAI_STATUS_SUCCESS;
}

// Perform set processing, only push data down when delta exists.
// obj_key: in format of str_object_type + ":" + serialized_object_id
sai_status_t internal_redis_idempotent_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key,
        _In_ const std::vector<swss::FieldValueTuple> &attr_entry)
{
    SWSS_LOG_ENTER();

    // For set, there is only one entry.
    const swss::FieldValueTuple &fv =  attr_entry[0];

    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        auto ptr_object_id_str = g_redisRestoreClient->hget("SWITCH", fvField(fv));

        if (ptr_object_id_str != NULL && fvValue(fv) == *ptr_object_id_str)
        {
            SWSS_LOG_INFO("RESTORE: skipping generic set key: %s, %s:%s",
                    obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
            return SAI_STATUS_SUCCESS;
        }
        else
        {
            g_redisRestoreClient->hset("SWITCH", fvField(fv), fvValue(fv));
            g_asicState->set(obj_key, attr_entry, "set");
            return SAI_STATUS_SUCCESS;
        }
    }

    std::string restoreKey = OID2ATTR_PREFIX + obj_key;
    std::map<std::string, std::string> attr_map;
    g_redisRestoreClient->hgetall(restoreKey, std::inserter(attr_map, attr_map.end()));

    if (attr_map.size() > 0)
    {
        auto it = attr_map.begin();
        while (it != attr_map.end())
        {
            if (fvField(fv) == it->first)
            {
                break;
            }
            it++;
        }
        // If no change to attribute value, just return success.
        if (it != attr_map.end() && it->second == fvValue(fv))
        {
            SWSS_LOG_INFO("RESTORE: skipping generic set key: %s, %s:%s no change",
                    obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
            return SAI_STATUS_SUCCESS;
        }

        // If it is default object created by libsai, don't check reverse mapping.
        std::string defaultObjKey = DEFAULT_OBJ_PREFIX + obj_key;
        auto default_exist = g_redisRestoreClient->exists(defaultObjKey);

        // TODO: Use more generic method like sai_metadata_is_object_type_oid(object_type)
        if (object_type != SAI_OBJECT_TYPE_FDB_ENTRY &&
            object_type != SAI_OBJECT_TYPE_NEIGHBOR_ENTRY &&
            object_type != SAI_OBJECT_TYPE_ROUTE_ENTRY &&
            default_exist == 0)
        {
            auto owner_str = redis_oid_to_owner_map_lookup(obj_key);
            SET_OBJ_OWNER(owner_str);

            std::string fvStr = joinOrderedFieldValues(attr_map);
            std::string attrFvStr = ATTR2OID_PREFIX + fvStr;

            sai_object_id_t objectId;
            objectId = redis_attr_to_oid_map_lookup(attrFvStr);

            if (objectId == SAI_NULL_OBJECT_ID)
            {
                UNSET_OBJ_OWNER();
                SWSS_LOG_ERROR("RESTORE_DB: generic set obj_key: %s, %s:%s, failed to find reverse map of %s ",
                        obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str(), attrFvStr.c_str());
                return SAI_STATUS_ITEM_NOT_FOUND;
            }
            // Clean old attributes to obj_key mapping.
            g_redisRestoreClient->hdel(attrFvStr, obj_key);
            redis_attr_to_oid_map_erase(attrFvStr);

            // Save default attributes to OID mapping if not done yet
            std::string defaultKey = DEFAULT_OID2ATTR_PREFIX + obj_key;
            auto oid_exist = g_redisRestoreClient->exists(defaultKey);
            if (oid_exist == 0)
            {
                // attribute changed from the original create, save the default mapping.
                // Here we assume no need to save default mapping for route/neighbor/fdb, double check!
                g_redisRestoreClient->hmset(defaultKey, attr_map.begin(), attr_map.end());
                g_redisRestoreClient->hset(DEFAULT_ATTR2OID_PREFIX + fvStr, obj_key, "NULL");
                redis_attr_to_oid_map_insert(DEFAULT_ATTR2OID_PREFIX + fvStr, objectId);
            }

            // Update or insert the attribute value and attributes to OID map
            attr_map[fvField(fv)] = fvValue(fv);
            fvStr = joinOrderedFieldValues(attr_map);
            attrFvStr = ATTR2OID_PREFIX + fvStr;
            g_redisRestoreClient->hset(attrFvStr, obj_key, "NULL");
            redis_attr_to_oid_map_insert(attrFvStr, objectId);
            SWSS_LOG_DEBUG("RESTORE_DB: generic set key: %s, %s:%s",
                    obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());

            UNSET_OBJ_OWNER();
        }
    }
    else
    {
        // For objects created by ASIC/SDK/LibSAI, they will reach here.
        // Add an entry in DEFAULT_OBJ_PREFIX table.
        // It is an indication that this object is not created by orchagent.
        // Only the first attribute set is logged.
        g_redisRestoreClient->hset(DEFAULT_OBJ_PREFIX + obj_key, fvField(fv), fvValue(fv));
    }
    // Update the attribute in obj_key to attributes map
    g_redisRestoreClient->hset(restoreKey, fvField(fv), fvValue(fv));

    if (g_record)
    {
        recordLine("s|" + obj_key + "|" + joinFieldValues(attr_entry));
    }
    g_asicState->set(obj_key, attr_entry, "set");
    return SAI_STATUS_SUCCESS;
}


// obj_key: in format of str_object_type + ":" + serialized_object_id
sai_status_t internal_redis_idempotent_remove(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key)
{
    SWSS_LOG_ENTER();

    std::string restoreKey;
    restoreKey = OID2ATTR_PREFIX + obj_key;

    std::map<std::string, std::string> attr_map;
    g_redisRestoreClient->hgetall(restoreKey, std::inserter(attr_map, attr_map.end()));

    if (attr_map.size() > 0)
    {
        SWSS_LOG_DEBUG("RESTORE_DB: generic remove key: %s", obj_key.c_str());
        g_redisRestoreClient->del(restoreKey);

        // TODO: Use more generic method like sai_metadata_is_object_type_oid(object_type)
        // For object with key type of sai_object_id_t, there is reverse mapping from
        // attributes to OID.
        if (object_type != SAI_OBJECT_TYPE_FDB_ENTRY &&
            object_type != SAI_OBJECT_TYPE_NEIGHBOR_ENTRY &&
            object_type != SAI_OBJECT_TYPE_ROUTE_ENTRY)
        {
            // If it is default object created by libsai, don't check reverse mapping.
            std::string defaultObjKey = DEFAULT_OBJ_PREFIX + obj_key;
            auto exist = g_redisRestoreClient->exists(defaultObjKey);
            if (exist > 0)
            {
                g_redisRestoreClient->del(defaultObjKey);
            }
            else
            {
                auto owner_str = redis_oid_to_owner_map_lookup(obj_key);
                if (owner_str != "")
                {
                    SET_OBJ_OWNER(owner_str);
                    g_redisRestoreClient->del(OBJ_OWNER_PREFIX + obj_key);
                    redis_oid_to_owner_map_erase(obj_key);
                }

                std::string attrFvStr = ATTR2OID_PREFIX + joinOrderedFieldValues(attr_map);
                sai_object_id_t object_id = redis_attr_to_oid_map_lookup(attrFvStr);
                if (object_id == SAI_NULL_OBJECT_ID)
                {
                    UNSET_OBJ_OWNER();
                    SWSS_LOG_ERROR("RESTORE_DB: generic remove key: %s failed to find ATTR2OID mapping for",
                            obj_key.c_str(), attrFvStr.c_str());
                    return SAI_STATUS_ITEM_NOT_FOUND;
                }
                g_redisRestoreClient->del(attrFvStr);
                redis_attr_to_oid_map_erase(attrFvStr);

                // Also check if there is default attributes OID mapping for this object
                std::string defaultKey = DEFAULT_OID2ATTR_PREFIX + obj_key;
                std::map<std::string, std::string> default_attr_map;

                g_redisRestoreClient->hgetall(defaultKey, std::inserter(default_attr_map, default_attr_map.end()));
                if (default_attr_map.size() > 0)
                {
                    attrFvStr = DEFAULT_ATTR2OID_PREFIX + joinOrderedFieldValues(default_attr_map);
                    object_id = redis_attr_to_oid_map_lookup(attrFvStr);
                    if (object_id == SAI_NULL_OBJECT_ID)
                    {
                        UNSET_OBJ_OWNER();
                        SWSS_LOG_ERROR("RESTORE_DB: generic remove key: %s failed to find DEFAULT_ATTR2OID mapping for",
                                obj_key.c_str(), attrFvStr.c_str());
                        return SAI_STATUS_ITEM_NOT_FOUND;
                    }
                    g_redisRestoreClient->del(attrFvStr);
                    redis_attr_to_oid_map_erase(attrFvStr);
                    // Assuming no need to save default mapping for route/neighbor/fdb, double check!
                    g_redisRestoreClient->del(defaultKey);
                }
                UNSET_OBJ_OWNER();
            }
        }
    }
    else
    {
        // Here ASIC db is being checked for the existence of the object.
        // For those default objects from ASIC, no entry for them in
        // restore DB if there is no set history on it.
        // Each orchagent actually already skipped the default object removal,
        // Double check here anyway.
        auto default_exist = g_redisClient->exists("ASIC_STATE:" + obj_key);
        if (default_exist == 0)
        {
            // Potentially there could be race condition, a sequnce of remove/create/remove for same object.
            // The second remove may arrives here just after first remove is processed by syncd.
            // Ignoring such case with assumption that feedback path implementation is comming, also no such
            // operation for default objects yet.
            SWSS_LOG_INFO("RESTORE_DB: generic remove key: %s, already done in ASIC DB", obj_key.c_str());
            return SAI_STATUS_SUCCESS;
        }
    }

    if (g_record)
    {
        recordLine("r|" + obj_key);
    }
    g_asicState->del(obj_key, "remove");

    return SAI_STATUS_SUCCESS;
}