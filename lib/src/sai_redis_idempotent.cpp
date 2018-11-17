#include "sai_redis.h"
#include "sairedis.h"
#include "sai_redis_idempotent_internal.h"
#include "meta/sai_serialize.h"
#include "meta/saiattributelist.h"

/* The in memory mapping for quick lookup:  ATTR2OID and DEFAULT_ATTR2OID */
static std::unordered_map<std::string, sai_object_id_t> attr2oid;
/* The in memory mapping for quick lookup:  OID2ATTR and DEFAULT_OID2ATTR */
static std::unordered_map<std::string, std::vector<swss::FieldValueTuple>> oid2attr;
/*
 * The in memory mapping for quick lookup:
 * (str_object_type + ":" + serialized_object_id) to owner
 */
static std::unordered_map<std::string, std::string> oid2owner;

static sai_object_id_t redis_attr_to_oid_db_lookup(
     _In_ const std::string &attrFvStr)
{
    auto oid_map = g_redisClient->hgetall(attrFvStr);
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

    for (const auto &attrFvStr: g_redisClient->keys(DEFAULT_ATTR2OID_PREFIX + "*"))
    {
        object_id = redis_attr_to_oid_db_lookup(attrFvStr);
        if (object_id != SAI_NULL_OBJECT_ID)
        {
            attr2oid[attrFvStr] = object_id;
        }
    }

    for (const auto &attrFvStr: g_redisClient->keys(ATTR2OID_PREFIX + "*"))
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
    for (const auto &ownerKey : g_redisClient->keys(OBJ_OWNER_PREFIX + std::string("*")))
    {
        auto ptr_owner_str = g_redisClient->hget(ownerKey, "owner");
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

static inline std::vector<swss::FieldValueTuple> redis_oid_to_atrr_db_lookup(
    _In_ const std::string &object_id_str)
{
    std::vector<swss::FieldValueTuple> values;
    auto fvs = g_redisClient->hgetall(object_id_str);

    for (auto &fv: fvs)
    {
        values.emplace_back(fv.first, fv.second);
    }
    return values;
}

void redis_oid_to_attr_map_restore(void)
{
    // For objects created by libsai/SDK
    for (const auto &object_id_str : g_redisClient->keys(DEFAULT_OBJ_PREFIX + std::string("*")))
    {
        oid2attr[object_id_str] = redis_oid_to_atrr_db_lookup(object_id_str);
    }

    // For saving objects with initial attributes list
    for (const auto &object_id_str : g_redisClient->keys(DEFAULT_OID2ATTR_PREFIX + std::string("*")))
    {
        oid2attr[object_id_str] = redis_oid_to_atrr_db_lookup(object_id_str);
    }

    // object to latest attributes
    for (const auto &object_id_str : g_redisClient->keys(OID2ATTR_PREFIX + std::string("*")))
    {
        oid2attr[object_id_str] = redis_oid_to_atrr_db_lookup(object_id_str);
    }
}

static std::vector<swss::FieldValueTuple> redis_oid_to_attr_map_lookup(
    _In_ const std::string object_id_str)
{
    if(oid2attr.find(object_id_str) == oid2attr.end())
    {
        return std::vector<swss::FieldValueTuple>();
    }

    return oid2attr[object_id_str];
}

static inline void redis_oid_to_attr_map_insert(
    _In_ const std::string object_id_str,
    _In_ const std::vector<swss::FieldValueTuple> &fvs)
{
    oid2attr[object_id_str] = fvs;
}

static inline void redis_oid_to_attr_map_erase(
    _In_ const std::string object_id_str)
{
    oid2attr.erase(object_id_str);
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
        auto ptr_object_id_str = g_redisClient->hget("RESTORE_SWITCH", "switch_oid");
        if (ptr_object_id_str != NULL)
        {
            sai_deserialize_object_id(*ptr_object_id_str, *object_id);

            SWSS_LOG_DEBUG("RESTORE: skipping generic create key: %s:%s, fields: %s",
                    str_object_type.c_str(), ptr_object_id_str->c_str(), fvStr.c_str());
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

    std::vector<swss::KeyOpFieldsValuesTuple> vkco;
    std::vector<swss::FieldValueTuple> fvs;

    SWSS_LOG_DEBUG("RESTORE: generic create key: %s, fields: %s", key.c_str(), fvStr.c_str());
    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        // Attributes for switch create contains running time function pointers which will be changed after restart
        fvs.emplace_back("switch_oid", serialized_object_id);
        vkco.emplace_back("RESTORE_SWITCH", "HSET", fvs);
        g_redisClient->hset("RESTORE_SWITCH", "switch_oid", serialized_object_id);
    }
    else
    {
        redis_attr_to_oid_map_insert(ATTR2OID_PREFIX + fvStr, *object_id);
        fvs.emplace_back(key, "NULL");
        vkco.emplace_back(ATTR2OID_PREFIX + fvStr, "HSET", fvs);

        redis_oid_to_attr_map_insert(OID2ATTR_PREFIX + key, entry);
        vkco.emplace_back(OID2ATTR_PREFIX + key, "HMSET", entry);

        if (g_objectOwner != "")
        {
            redis_oid_to_owner_map_insert(key, g_objectOwner);

            fvs.clear();
            fvs.emplace_back("owner", g_objectOwner);
            vkco.emplace_back(OBJ_OWNER_PREFIX + key, "HSET", fvs);
        }
    }
    g_asicState->set(key, entry, "create", EMPTY_PREFIX, vkco);
    // we assume create will always succeed which may not be true
    // we should make this synchronous call
    return SAI_STATUS_SUCCESS;
}

// It is for non-sai_object_id_t object create only.
// May not be necessary in future, since orchagent did the filtering partially for router, neighbor, nexthop,
sai_status_t internal_redis_idempotent_create(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key,   // in format of str_object_type + ":" + serialized_object_id
        _In_ const std::vector<swss::FieldValueTuple> &attr_entry)
{
    SWSS_LOG_ENTER();

    std::string fvStr = joinFieldValues(attr_entry);

    std::string restoreKey = OID2ATTR_PREFIX + obj_key;

    // For non-sai_object_id_t object, the key may be used directly.
    std::vector<swss::FieldValueTuple> fvs = redis_oid_to_attr_map_lookup(restoreKey);
    if (fvs.size() > 0)
    {
        SWSS_LOG_INFO("RESTORE: skipping generic create key: %s, fields: %s", obj_key.c_str(), fvStr.c_str());
        return SAI_STATUS_SUCCESS;
    }

    redis_oid_to_attr_map_insert(restoreKey, attr_entry);
    std::vector<swss::KeyOpFieldsValuesTuple> vkco;
    vkco.emplace_back(restoreKey, "HMSET", attr_entry);

    if (g_record)
    {
        recordLine("c|" + obj_key + "|" + fvStr);
    }

    g_asicState->set(obj_key, attr_entry, "create", EMPTY_PREFIX, vkco);

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

    std::vector<swss::KeyOpFieldsValuesTuple> vkco;
    std::vector<swss::FieldValueTuple> fvs;
    // For set, there is only one entry.
    const swss::FieldValueTuple &fv =  attr_entry[0];

    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        auto ptr_object_id_str = g_redisClient->hget("RESTORE_SWITCH", fvField(fv));

        if (ptr_object_id_str != NULL && fvValue(fv) == *ptr_object_id_str)
        {
            SWSS_LOG_INFO("RESTORE: skipping generic set key: %s, %s:%s",
                    obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
            return SAI_STATUS_SUCCESS;
        }
        else
        {
            vkco.emplace_back("RESTORE_SWITCH", "HMSET", attr_entry);
            g_asicState->set(obj_key, attr_entry, "set", EMPTY_PREFIX, vkco);
            return SAI_STATUS_SUCCESS;
        }
    }

    std::string defaultObjKey = DEFAULT_OBJ_PREFIX + obj_key;
    std::string restoreKey = OID2ATTR_PREFIX + obj_key;
    auto current_fvs = redis_oid_to_attr_map_lookup(restoreKey);
    if (current_fvs.size() > 0)
    {
        auto it = current_fvs.begin();
        while (it != current_fvs.end())
        {
            if (fvField(fv) == fvField(*it))
            {
                break;
            }
            it++;
        }
        // If no change to attribute value, just return success.
        if (it != current_fvs.end() && fvValue(*it) == fvValue(fv))
        {
            SWSS_LOG_INFO("RESTORE: skipping generic set key: %s, %s:%s no change",
                    obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
            return SAI_STATUS_SUCCESS;
        }

        // If it is default object created by libsai, don't check reverse mapping.
        auto default_obj_fvs = redis_oid_to_attr_map_lookup(defaultObjKey);

#if 0
        auto info = sai_metadata_get_object_type_info(object_type);
        if (!info->isnonobjectid && default_obj_fvs.size() == 0)
        {

        }
#endif
        // TODO: Use more generic method like sai_metadata_is_object_type_oid(object_type)
        if (object_type != SAI_OBJECT_TYPE_FDB_ENTRY &&
            object_type != SAI_OBJECT_TYPE_NEIGHBOR_ENTRY &&
            object_type != SAI_OBJECT_TYPE_ROUTE_ENTRY &&
            default_obj_fvs.size() == 0)
        {
            auto owner_str = redis_oid_to_owner_map_lookup(obj_key);
            SET_OBJ_OWNER(owner_str);

            std::string fvStr = joinOrderedFieldValues(current_fvs);
            std::string attrFvStr = ATTR2OID_PREFIX + fvStr;

            sai_object_id_t objectId = redis_attr_to_oid_map_lookup(attrFvStr);
            if (objectId == SAI_NULL_OBJECT_ID)
            {
                UNSET_OBJ_OWNER();
                SWSS_LOG_ERROR("RESTORE_DB: generic set obj_key: %s, %s:%s, failed to find reverse map of %s ",
                        obj_key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str(), attrFvStr.c_str());
                return SAI_STATUS_ITEM_NOT_FOUND;
            }

            // Clean old attributes to obj_key mapping.
            vkco.emplace_back(attrFvStr, "DEL", fvs);
            redis_attr_to_oid_map_erase(attrFvStr);

            // Save default attributes to OID mapping if not done yet
            std::string defaultKey = DEFAULT_OID2ATTR_PREFIX + obj_key;
            auto default_fvs = redis_oid_to_attr_map_lookup(defaultKey);
            if (default_fvs.size() == 0)
            {
                // attribute changed from the original create, save the default mapping.
                // Here we assume no need to save default mapping for route/neighbor/fdb, double check!
                vkco.emplace_back(defaultKey, "HMSET", current_fvs);
                redis_oid_to_attr_map_insert(defaultKey, current_fvs);

                vkco.emplace_back(DEFAULT_ATTR2OID_PREFIX + fvStr, "HSET",
                    std::vector<swss::FieldValueTuple>{swss::FieldValueTuple(obj_key, "NULL")});
                redis_attr_to_oid_map_insert(DEFAULT_ATTR2OID_PREFIX + fvStr, objectId);
            }

            auto it_fv = current_fvs.begin();
            // Update existing fv
            while (it_fv != current_fvs.end())
            {
                if (fvField(fv) == it_fv->first)
                {
                    it_fv->second = fvValue(fv);
                    break;
                }
                it_fv++;
            }
            // insert new fv
            if (it_fv == current_fvs.end())
            {
                current_fvs.push_back(fv);
            }

            fvStr = joinOrderedFieldValues(current_fvs);
            attrFvStr = ATTR2OID_PREFIX + fvStr;

            fvs.clear();
            fvs.emplace_back(obj_key, "NULL");
            vkco.emplace_back(attrFvStr, "HSET", fvs);
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

        assert(attr_entry.size() == 1);
        vkco.emplace_back(defaultObjKey, "HSET", attr_entry);
        redis_oid_to_attr_map_insert(defaultObjKey, attr_entry);
    }
    // Update the attribute in obj_key to attributes map
    redis_oid_to_attr_map_insert(restoreKey, current_fvs);
    vkco.emplace_back(restoreKey, "HMSET", attr_entry);

    if (g_record)
    {
        recordLine("s|" + obj_key + "|" + joinFieldValues(attr_entry));
    }
    g_asicState->set(obj_key, attr_entry, "set", EMPTY_PREFIX, vkco);
    return SAI_STATUS_SUCCESS;
}

// obj_key: in format of str_object_type + ":" + serialized_object_id
sai_status_t internal_redis_idempotent_remove(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key)
{
    SWSS_LOG_ENTER();

    std::vector<swss::KeyOpFieldsValuesTuple> vkco;
    std::vector<swss::FieldValueTuple> fvs;

    std::string restoreKey;
    restoreKey = OID2ATTR_PREFIX + obj_key;

    fvs = redis_oid_to_attr_map_lookup(restoreKey);
    if (fvs.size() > 0)
    {
        SWSS_LOG_INFO("RESTORE_DB: generic remove key: %s", obj_key.c_str());
        // TODO: Use more generic method like sai_metadata_is_object_type_oid(object_type)
        // For object with key type of sai_object_id_t, there is reverse mapping from
        // attributes to OID.
        if (object_type != SAI_OBJECT_TYPE_FDB_ENTRY &&
            object_type != SAI_OBJECT_TYPE_NEIGHBOR_ENTRY &&
            object_type != SAI_OBJECT_TYPE_ROUTE_ENTRY)
        {
            // If it is default object created by libsai, don't check reverse mapping.
            std::string defaultObjKey = DEFAULT_OBJ_PREFIX + obj_key;
            auto tmp_fvs = redis_oid_to_attr_map_lookup(defaultObjKey);
            if (tmp_fvs.size() > 0)
            {
                vkco.emplace_back(defaultObjKey, "DEL", tmp_fvs);
                redis_oid_to_attr_map_erase(defaultObjKey);
            }
            else
            {
                auto owner_str = redis_oid_to_owner_map_lookup(obj_key);
                if (owner_str != "")
                {
                    SET_OBJ_OWNER(owner_str);
                    vkco.emplace_back(OBJ_OWNER_PREFIX + obj_key, "DEL", fvs);
                    redis_oid_to_owner_map_erase(obj_key);
                }

                std::string attrFvStr = ATTR2OID_PREFIX + joinOrderedFieldValues(fvs);
                sai_object_id_t object_id = redis_attr_to_oid_map_lookup(attrFvStr);
                if (object_id == SAI_NULL_OBJECT_ID)
                {
                    UNSET_OBJ_OWNER();
                    SWSS_LOG_ERROR("RESTORE_DB: generic remove key: %s failed to find ATTR2OID mapping for",
                            obj_key.c_str(), attrFvStr.c_str());
                    return SAI_STATUS_ITEM_NOT_FOUND;
                }

                vkco.emplace_back(attrFvStr, "DEL", fvs);
                redis_attr_to_oid_map_erase(attrFvStr);

                // Also check if there is default attributes OID mapping for this object
                std::string defaultKey = DEFAULT_OID2ATTR_PREFIX + obj_key;
                auto defalut_fvs = redis_oid_to_attr_map_lookup(defaultKey);
                if (defalut_fvs.size() > 0)
                {
                    attrFvStr = DEFAULT_ATTR2OID_PREFIX + joinOrderedFieldValues(defalut_fvs);
                    object_id = redis_attr_to_oid_map_lookup(attrFvStr);
                    if (object_id == SAI_NULL_OBJECT_ID)
                    {
                        UNSET_OBJ_OWNER();
                        SWSS_LOG_ERROR("RESTORE_DB: generic remove key: %s failed to find DEFAULT_ATTR2OID mapping for",
                                obj_key.c_str(), attrFvStr.c_str());
                        return SAI_STATUS_ITEM_NOT_FOUND;
                    }
                    vkco.emplace_back(attrFvStr, "DEL", fvs);
                    redis_attr_to_oid_map_erase(attrFvStr);
                    // Assuming no need to save default mapping for route/neighbor/fdb, double check!
                    vkco.emplace_back(defaultKey, "DEL", fvs);
                    redis_oid_to_attr_map_erase(defaultKey);
                }
                UNSET_OBJ_OWNER();
            }
        }
        vkco.emplace_back(restoreKey, "DEL", fvs);
        redis_oid_to_attr_map_erase(restoreKey);
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
    g_asicState->del(obj_key, "remove", EMPTY_PREFIX, vkco);

    return SAI_STATUS_SUCCESS;
}