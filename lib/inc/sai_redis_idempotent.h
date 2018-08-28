#ifndef __SAI_REDIS_IDEMPOTENT__
#define __SAI_REDIS_IDEMPOTENT__

/*
 * Object created with certain set of attributes, but the attributes may get changed later.
 * The inital value of ATTR2OID and OID2ATTR mapping saved in DEFAULT table.
 */
#define DEFAULT_ATTR2OID_PREFIX    ("DEFAULT_ATTR2OID_" + (g_objectOwner))
#define DEFAULT_OID2ATTR_PREFIX    "DEFAULT_OID2ATTR_"

/*
 * For objects created by asic SDK/Libsai and changed by orchagent.
 */
#define DEFAULT_OBJ_PREFIX    "DEFAULT_OBJ_"

/*
 * The mapping between OID and attributes
 */
#define ATTR2OID_PREFIX    ("ATTR2OID_" + (g_objectOwner))
#define OID2ATTR_PREFIX    "OID2ATTR_"

#define OBJ_OWNER_PREFIX   "OBJ_OWNER_"
/*
 * For certain type of object, multiple objects may be created with the exact
 * same atrributes. The macro is to specify owner of the objects so they may be
 * uniquely identified with attributes + owner.
 * By default, no owner is specified for object.
 */
// TODO: make SET_OBJ_OWNER scoped class object to avoid explicit UNSET
#define SET_OBJ_OWNER(owner) ({    \
    g_objectOwner = owner;         \
})

#define UNSET_OBJ_OWNER() ({       \
    g_objectOwner = "";            \
})

// Whether to enable idempotent SAI redis operation
extern bool g_idempotent;

/*
 * The owner of the create/set/remove request,
 * set by each orchagent sub application to handle the case of same attributes
 * mapped to multiple OIDs when needed.
 * For most of the objects, it should be empty.
 */
extern std::string g_objectOwner;

#endif // __SAI_REDIS_IDEMPOTENT__
