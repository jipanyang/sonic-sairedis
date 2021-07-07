#ifndef __SAIREDIS_IDEMPOTENT_INTERNAL__
#define __SAIREDIS_IDEMPOTENT_INTERNAL__
#include "sai_redis_idempotent.h"

extern "C" {
#include "sai.h"
}


extern sai_status_t redis_idempotent_create(
        _In_ sai_object_type_t object_type,
        _Out_ sai_object_id_t* object_id,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list);

extern sai_status_t internal_redis_idempotent_create(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key,   // in format of str_object_type + ":" + serialized_object_id
        _In_ const std::vector<swss::FieldValueTuple> &attr_entry);

extern sai_status_t internal_redis_idempotent_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key,   // in format of str_object_type + ":" + serialized_object_id
        _In_ const std::vector<swss::FieldValueTuple> &attr_entry);

extern sai_status_t internal_redis_idempotent_remove(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &obj_key);

extern void redis_attr_to_oid_map_restore(void);
extern void redis_oid_to_owner_map_restore(void);
extern void redis_oid_to_attr_map_restore(void);

extern std::string joinOrderedFieldValues(
        _In_ const std::vector<swss::FieldValueTuple> &values);

extern std::string joinOrderedFieldValues(
        _In_ const std::map<std::string, std::string> &map);

#endif // __SAIREDIS_IDEMPOTENT_INTERNAL__
