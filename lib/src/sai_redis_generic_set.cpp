#include "sai_redis.h"
#include "sairedis.h"
#include "meta/sai_serialize.h"
#include "meta/saiattributelist.h"

sai_status_t internal_redis_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serialized_object_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::vector<swss::FieldValueTuple> entry = SaiAttributeList::serialize_attr_list(
            object_type,
            1,
            attr,
            false);

    std::string str_object_type = sai_serialize_object_type(object_type);

    std::string key = str_object_type + ":" + serialized_object_id;

    SWSS_LOG_DEBUG("generic set key: %s, fields: %lu", key.c_str(), entry.size());

    if (g_idempotent)
    {
        // For set, there is only one entry.
        swss::FieldValueTuple &fv =  entry[0];

        if (object_type == SAI_OBJECT_TYPE_SWITCH)
        {
            auto ptr_object_id_str = g_redisRestoreClient->hget("SWITCH", fvField(fv));

            if (ptr_object_id_str == NULL || fvValue(fv) == *ptr_object_id_str)
            {
                SWSS_LOG_INFO("RESTORE: skipping generic set key: %s, %s:%s",
                        key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
                return SAI_STATUS_SUCCESS;
            }
            else
            {
                g_redisRestoreClient->hset("SWITCH", fvField(fv), fvValue(fv));
                g_asicState->set(key, entry, "set");
                return SAI_STATUS_SUCCESS;
            }
        }

        std::string restoreKey = OID2ATTR_PREFIX + key;
        auto attr_map = g_redisRestoreClient->hgetallordered(restoreKey);

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
                        key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());
                return SAI_STATUS_SUCCESS;
            }

            // If it is default object created by libsai, don't check reverse mapping.
            std::string defaultObjKey = DEFAULT_OBJ_PREFIX + key;
            auto default_exist = g_redisRestoreClient->exists(defaultObjKey);

            // TODO: Use more generic method like sai_metadata_is_object_type_oid(object_type)
            if (object_type != SAI_OBJECT_TYPE_FDB_ENTRY &&
                object_type != SAI_OBJECT_TYPE_NEIGHBOR_ENTRY &&
                object_type != SAI_OBJECT_TYPE_ROUTE_ENTRY &&
                default_exist == 0)
            {
                auto owner_str = redis_oid_to_owner_map_lookup(key);
                SET_OBJ_OWNER(owner_str);

                std::string fvStr = joinOrderedFieldValues(attr_map);
                std::string attrFvStr = ATTR2OID_PREFIX + fvStr;

                sai_object_id_t objectId;
                objectId = redis_attr_to_oid_map_lookup(attrFvStr);

                if (objectId == SAI_NULL_OBJECT_ID)
                {
                    UNSET_OBJ_OWNER();
                    SWSS_LOG_ERROR("RESTORE_DB: generic set key: %s, %s:%s, failed to find reverse map of %s ",
                            key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str(), attrFvStr.c_str());
                    return SAI_STATUS_ITEM_NOT_FOUND;
                }
                // Clean old attributes to key mapping.
                g_redisRestoreClient->hdel(attrFvStr, key);
                redis_attr_to_oid_map_erase(attrFvStr);

                // Save default attributes to OID mapping if not done yet
                std::string defaultKey = DEFAULT_OID2ATTR_PREFIX + key;
                auto oid_exist = g_redisRestoreClient->exists(defaultKey);
                if (oid_exist == 0)
                {
                    // attribute changed from the original create, save the default mapping.
                    // Here we assume no need to save default mapping for route/neighbor/fdb, double check!
                    g_redisRestoreClient->hmset(defaultKey, attr_map);
                    g_redisRestoreClient->hset(DEFAULT_ATTR2OID_PREFIX + fvStr, key, "NULL");
                    redis_attr_to_oid_map_insert(DEFAULT_ATTR2OID_PREFIX + fvStr, objectId);
                }

                // Update or insert the attribute value and attributes to OID map
                attr_map[fvField(fv)] = fvValue(fv);
                fvStr = joinOrderedFieldValues(attr_map);
                attrFvStr = ATTR2OID_PREFIX + fvStr;
                g_redisRestoreClient->hset(attrFvStr, key, "NULL");
                redis_attr_to_oid_map_insert(attrFvStr, objectId);
                SWSS_LOG_DEBUG("RESTORE_DB: generic set key: %s, %s:%s",
                        key.c_str(), fvField(fv).c_str(), fvValue(fv).c_str());

                UNSET_OBJ_OWNER();
            }
        }
        else
        {
            // For objects created by ASIC/SDK/LibSAI, they will reach here.
            // Add an entry in DEFAULT_OBJ_PREFIX table.
            // It is an indication that this object is not created by orchagent.
            // Only the first attribute set is logged.
            g_redisRestoreClient->hset(DEFAULT_OBJ_PREFIX + key, fvField(fv), fvValue(fv));
        }
        // Update the attribute in key to attributes map
        g_redisRestoreClient->hset(restoreKey, fvField(fv), fvValue(fv));
    }

    if (g_record)
    {
        recordLine("s|" + key + "|" + joinFieldValues(entry));
    }
    g_asicState->set(key, entry, "set");
    return SAI_STATUS_SUCCESS;
}

sai_status_t internal_redis_bulk_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::vector<std::string> &serialized_object_ids,
        _In_ const sai_attribute_t *attr_list,
        _In_ const sai_status_t *object_statuses)
{
    SWSS_LOG_ENTER();

    std::string str_object_type = sai_serialize_object_type(object_type);

    std::vector<swss::FieldValueTuple> entries;
    std::vector<swss::FieldValueTuple> entriesWithStatus;

    /*
     * We are recording all entries and their statuses, but we send to sairedis
     * only those that succeeded metadata check, since only those will be
     * executed on syncd, so there is no need with bothering decoding statuses
     * on syncd side.
     */

    for (size_t idx = 0; idx < serialized_object_ids.size(); ++idx)
    {
        std::vector<swss::FieldValueTuple> entry =
            SaiAttributeList::serialize_attr_list(object_type, 1, &attr_list[idx], false);

        std::string str_attr = joinFieldValues(entry);

        std::string str_status = sai_serialize_status(object_statuses[idx]);

        std::string joined = str_attr + "|" + str_status;

        swss::FieldValueTuple fvt(serialized_object_ids[idx] , joined);

        entriesWithStatus.push_back(fvt);

        if (object_statuses[idx] != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_WARN("skipping %s since status is %s",
                    serialized_object_ids[idx].c_str(),
                    str_status.c_str());

            continue;
        }

        swss::FieldValueTuple fvtNoStatus(serialized_object_ids[idx] , str_attr);

        entries.push_back(fvtNoStatus);
    }

    /*
     * We are adding number of entries to actualy add ':' to be compatible
     * with previous
     */

    if (g_record)
    {
        std::string joined;

        for (const auto &e: entriesWithStatus)
        {
            // ||obj_id|attr=val|attr=val|status||obj_id|attr=val|attr=val|status

            joined += "||" + fvField(e) + "|" + fvValue(e);
        }

        /*
         * Capital 'S' stads for bulk SET operation.
         */

        recordLine("S|" + str_object_type + joined);
    }

    std::string key = str_object_type + ":" + std::to_string(entries.size());

    if (entries.size())
    {
        g_asicState->set(key, entries, "bulkset");
    }

    return SAI_STATUS_SUCCESS;
}


sai_status_t redis_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_object_id = sai_serialize_object_id(object_id);

    return internal_redis_generic_set(
            object_type,
            str_object_id,
            attr);
}

sai_status_t redis_generic_set_fdb_entry(
        _In_ const sai_fdb_entry_t *fdb_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_fdb_entry = sai_serialize_fdb_entry(*fdb_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_FDB_ENTRY,
            str_fdb_entry,
            attr);
}

sai_status_t redis_generic_set_neighbor_entry(
        _In_ const sai_neighbor_entry_t* neighbor_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_neighbor_entry = sai_serialize_neighbor_entry(*neighbor_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_NEIGHBOR_ENTRY,
            str_neighbor_entry,
            attr);
}

sai_status_t redis_generic_set_route_entry(
        _In_ const sai_route_entry_t* route_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_route_entry = sai_serialize_route_entry(*route_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_ROUTE_ENTRY,
            str_route_entry,
            attr);
}
