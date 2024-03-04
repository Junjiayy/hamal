package types

type (
	Reader interface {
		Read() (*BinlogParams, error)
		Complete(params *BinlogParams) error
		Close() error
	}

	Writer interface {
		Type() string
		Insert(params *SyncParams, values interface{}) error
		Update(params *SyncParams, values interface{}) error
		Delete(params *SyncParams) error
	}

	Filter interface {
		InsertEventRecord(params *SyncParams, updatedColumns []string) error
		FilterColumns(params *SyncParams, columns []string) ([]string, error, bool)
	}
)

// 读取器构造函数集合
var _readers map[string]func(i int, conf interface{}) (Reader, error)

// RegisterReaderConstructor 注册读取器构造函数
func RegisterReaderConstructor(name string, fn func(i int, conf interface{}) (Reader, error)) {
	_readers[name] = fn
}

// GetReaderConstructor 获取读取器构造函数
func GetReaderConstructor(name string) func(i int, conf interface{}) (Reader, error) {
	return _readers[name]
}
