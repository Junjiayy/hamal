package types

const EventTypeInsert = "insert" // 插入事件
const EventTypeUpdate = "update" // 更新事件
const EventTypeDelete = "delete" // 删除事件

const ConditionTypeAnd = "and" // 多条件与关系
const ConditionTypeOr = "or"   // 多条件或关系

const InnerSyncTypeDefaultKey = "innerSyncTypeDefaultKey"

const SyncTypeCopy = "copy"   // 可复制的字段全部拷贝到目标数据源作为一条新的记录
const SyncTypeJoin = "join"   // 可复制的字段全部拷贝到目标数据源中符合条件的记录，作为这条记录的某个对象字段
const SyncTypeInner = "inner" // 只允许复制一个字段，复制到目标数据源符合记录的某个字段，这个字段一定是一个数组

const identifyIdColumnSeparator = "-" // 标识ID字段分隔符

const ReaderTypeWeb = "web" // web 类型读取器

const DataSourceMysql = "mysql"      // mysql 类型数据源
const DataSourceElasticSearch = "es" // es 类型数据源

const TimestampCreatedAt = "created_at" // 创建时间戳
const TimestampUpdatedAt = "updated_at" // 更新时间戳
