package core

import (
	types "github.com/Junjiayy/hamal/pkg/types"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"os"
	"testing"
)

type (
	testWriterAndFilter struct {
		records map[string]map[string]string
	}
)

func (t *testWriterAndFilter) Type() string {
	return "test"
}

func (t *testWriterAndFilter) Insert(params *types.SyncParams, values interface{}) error {
	switch params.Rule.SyncType {
	case types.SyncTypeCopy:
		t.records[params.Data[params.Rule.PrimaryKey]] = values.(map[string]string)
	case types.SyncTypeJoin:
		t.records[params.Rule.JoinFieldName] = values.(map[string]string)
	case types.SyncTypeInner:
		t.records[params.Rule.JoinFieldName] = map[string]string{"value": values.(string)}
	}

	return nil
}

func (t *testWriterAndFilter) Update(params *types.SyncParams, values interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (t *testWriterAndFilter) Delete(params *types.SyncParams) error {
	_, ok := t.records[params.Data[params.Rule.PrimaryKey]]
	if ok {
		delete(t.records, params.Data[params.Rule.PrimaryKey])
	}

	return nil
}

func (t *testWriterAndFilter) InsertEventRecord(params *types.SyncParams, updatedColumns []string) error {
	return nil
}

func (t *testWriterAndFilter) FilterColumns(params *types.SyncParams, columns []string) ([]string, error, bool) {
	switch params.Rule.SyncType {
	case types.SyncTypeCopy:
		return []string{"id", "age", "name"}, nil, true
	case types.SyncTypeJoin:
		return []string{"id", "name"}, nil, true
	case types.SyncTypeInner:
		return []string{"id", "age"}, nil, true
	}

	return nil, nil, false
}

var h *handler

func TestMain(m *testing.M) {
	wsf := &testWriterAndFilter{records: make(map[string]map[string]string)}
	redisCli := redis.NewClient(&redis.Options{
		Addr:     "10.211.55.4:6379",
		Password: "123456",
		DB:       0,
	})

	h = &handler{
		filter: wsf, ws: map[string]types.Writer{"test": wsf},
		rs: redsync.New(goredis.NewPool(redisCli)),
	}

	pool, _ := ants.NewPoolWithFunc(5, h.sync,
		ants.WithNonblocking(true))
	h.pool = pool

	code := m.Run()
	os.Exit(code)
}

func getSyncParams() *types.SyncParams {
	wg := types.NewSyncWaitGroup()

	rule := types.SyncRule{
		Database: "test", Table: "tests", PrimaryKey: "id",
		LockColumns: []string{"id", "name", "age"},
		Columns: map[string]string{
			"id": "id", "name": "name", "age": "age",
		},

		SoftDeleteField:   "deleted_at",
		UnSoftDeleteValue: "0",
		Target:            "mysql:test.test.tests",
		SyncType:          types.SyncTypeCopy,
		TargetType:        "test",
		TargetDatabase:    "test",
		TargetTable:       "tests",
	}

	data := map[string]string{
		"id": "1", "name": "hhhhh", "age": "18",
	}

	binLog := &types.BinlogParams{
		Database: "test", Table: "tests", EventAt: 1709026288000,
		EventType: "INSERT", IsDdl: false,
		Data: []map[string]string{data},
		Old:  nil,
	}

	return types.NewSyncParams(wg, &rule, data, nil, binLog)
}

func Test_handler_lockRecordByParams(t *testing.T) {
	params := getSyncParams()

	j := 0
	for i := 0; i < 1000; i++ {
		params.GetWg().Add(1)
		go func() {
			defer params.GetWg().Done()
			mutex, lockKey := h.lockRecordByParams(params)
			defer h.unlockRecordByMutex(mutex, lockKey)
			j++
		}()
	}

	params.GetWg().Wait()
	if j != 1000 {
		t.Fatalf("分布式锁测试失败，期待值: 1000, 实际值: %d", j)
	}
}

func Test_handler_insert(t *testing.T) {
	params := getSyncParams()
	params.Data[params.Rule.SoftDeleteField] = "1"
	columns, err := h.insert(params)
	if !errors.Is(err, emptyErr) {
		t.Fatalf("sync insert event failed: %s", err)
	}

	params.Data[params.Rule.SoftDeleteField] = params.Rule.UnSoftDeleteValue
	columns, err = h.insert(params)
	if err != nil {
		t.Fatalf("sync insert event failed: %s", err)
	}

	if len(columns) != 3 {
		t.Fatalf("sync insert event column count error, expect: 3, actual: %d", len(columns))
	}

	twf := h.filter.(*testWriterAndFilter)
	if len(twf.records) != 1 {
		t.Fatalf("writer records empty")
	}

	record, ok := twf.records[params.Data["id"]]
	if !ok {
		t.Fatalf("sync failed, id %s not exists", params.Data["id"])
	}

	if len(record) != 3 {
		t.Fatalf("sync insert event column count error, expect: 3, actual: %d", len(columns))
	}
}

func Test_handler_delete(t *testing.T) {
	params := getSyncParams()
	_, err := h.insert(params)
	if err != nil {
		t.Fatalf("sync insert event failed: %s", err)
	}

	wtf := h.filter.(*testWriterAndFilter)
	if len(wtf.records) != 1 {
		t.Fatalf("writer records empty")
	}

	err = h.delete(params)
	if err != nil {
		t.Fatalf("sync delete event failed: %s", err)
	}

	if len(wtf.records) != 0 {
		t.Fatal("sync delete event failed")
	}
}

func Test_handler_update(t *testing.T) {
	params := getSyncParams()
	// 常规更新
	params.Old = map[string]string{"name": "name2", "age": "25"}

}
