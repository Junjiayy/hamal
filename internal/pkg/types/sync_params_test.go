package types

import (
	"testing"
)

func TestSyncParams_GetUpdateValues(t *testing.T) {
	wg := NewSyncWaitGroup()

	rule := SyncRule{
		Database: "test", Table: "tests", PrimaryKey: "id",
		LockColumns: []string{"id", "name", "age"},
		Columns: map[string]string{
			"id": "id", "name": "name", "age": "age",
		},

		SoftDeleteField:   "deleted_at",
		UnSoftDeleteValue: "0",
		Target:            "mysql:test.test.tests",
		SyncType:          SyncTypeCopy,
		TargetType:        "test",
		TargetDatabase:    "test",
		TargetTable:       "tests",
	}

	data := map[string]string{
		"id": "1", "name": "hhhhh", "age": "18",
	}

	binLog := &BinlogParams{
		Database: "test", Table: "tests", EventAt: 1709026288000,
		EventType: "INSERT", IsDdl: false,
		Data: []map[string]string{data},
		Old:  nil,
	}

	params := NewSyncParams(wg, &rule, data, nil, binLog)
	columns := []string{"id", "name", "age"}
	values := params.GetUpdateValues(columns)
	mapping, ok := values.(map[string]string)
	if !ok {
		t.Fatalf("get update value type error, expect: map[string]string, actual: %T", values)
	}

	if len(mapping) != len(columns) {
		t.Fatalf("clumns count error, expect: %d, actual: %d",
			len(columns), len(mapping))
	}

	params.Rule.SyncType = SyncTypeJoin
	params.Rule.JoinFieldName = "test_join_name"
	values = params.GetUpdateValues(columns)
	interMapping, ok := values.(map[string]interface{})
	if !ok {
		t.Fatalf("get update value type error, expect: map[string]interface{}, actual: %T", values)
	}

	record, ok := interMapping[params.Rule.JoinFieldName]
	if !ok {
		t.Fatalf("field %s not exists", params.Rule.JoinFieldName)
	}

	mapping, ok = record.(map[string]string)
	if !ok {
		t.Fatalf("get update value type error, expect: map[string]string, actual: %T", values)
	}

	if len(mapping) != len(columns) {
		t.Fatalf("clumns count error, expect: %d, actual: %d",
			len(columns), len(mapping))
	}

	params.Rule.SyncType = SyncTypeInner
	params.Rule.JoinFieldName = "name"
	values = params.GetUpdateValues(nil)
	valueStr, ok := values.(string)
	if !ok {
		t.Fatalf("get update value type error, expect: string, actual: %T", values)
	}

	if valueStr != params.Data["name"] {
		t.Fatalf("inner value failed, expect: %s, actual: %s",
			params.Data["name"], valueStr)
	}
}
