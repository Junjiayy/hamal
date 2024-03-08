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
