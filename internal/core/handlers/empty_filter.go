package handlers

import "github.com/Junjiayy/hamal/pkg/types"

type emptyFilter struct{}

func (e *emptyFilter) InsertEventRecord(params *types.SyncParams, updatedColumns []string) error {
	return nil
}

func (e *emptyFilter) FilterColumns(params *types.SyncParams, columns []string) ([]string, error, bool) {
	return columns, nil, true
}
