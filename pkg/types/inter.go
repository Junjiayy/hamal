package types

type (
	Filter interface {
		InsertEventRecord(params *SyncParams, updatedColumns []string) error
		FilterColumns(params *SyncParams, columns []string) ([]string, error, bool)
	}
)
