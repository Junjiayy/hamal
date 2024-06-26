package handlers

import (
	"fmt"
	"github.com/Junjiayy/hamal/pkg/core/writers"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"strings"
	"time"
)

type Handler struct {
	filter types.Filter
	wp     *writers.WriterPool
	pool   *ants.PoolWithFunc
	rs     *redsync.Redsync
}

var (
	emptyErr = errors.New("empty")
)

const syncLockKeyTpl = "lock:%s:%s::keys" // 格式 lock:database:table:column1_column2..

func NewHandler(redisCli *redis.Client, poolSize int) (h *Handler, err error) {
	h = &Handler{
		wp:     writers.NewWriterPool(),
		rs:     redsync.New(goredis.NewPool(redisCli)),
		filter: new(emptyFilter),
	}

	h.pool, err = ants.NewPoolWithFunc(poolSize, h.sync,
		ants.WithNonblocking(true))

	return
}

func (h *Handler) GetWriterPool() *writers.WriterPool {
	return h.wp
}

// Invoke 分配任务到 携程池
func (h *Handler) Invoke(params *types.SyncParams) (err error) {
	defer func() {
		if err != nil {
			// 如果任务放到携程池失败，直接释放本次执行参数
			params.Recycle()
		}
	}()

	params.GetWg().Add(1)
	if err := h.pool.Invoke(params); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// run 携程池执行方法，主要同步逻辑
func (h *Handler) sync(paramsInter interface{}) {
	params := paramsInter.(*types.SyncParams)
	defer params.Recycle()
	defer func() {
		if err := recover(); err != nil {
			h.writeLog(params, errors.Errorf("recover: %v", err))
		}
	}()

	mutex, lockKey := h.lockRecordByParams(params)
	if mutex == nil {
		return
	}
	defer h.unlockRecordByMutex(mutex, lockKey)

	var (
		columns []string
		err     error
	)

	switch params.RealEventType {
	case types.EventTypeInsert:
		columns, err = h.insert(params)
	case types.EventTypeUpdate:
		columns, err = h.update(params)
	case types.EventTypeDelete:
		err = h.delete(params)
	}

	if err != nil && !errors.Is(err, emptyErr) {
		h.writeLog(params, err)
	} else if err == nil {
		if err := h.filter.InsertEventRecord(params, columns); err != nil {
			h.writeLog(params, err)
		}
	}
}

// lockRecordByParams 通过同步参数给记录添加分布式锁
// 防止并发修改时数据错误
func (h *Handler) lockRecordByParams(params *types.SyncParams) (*redsync.Mutex, string) {
	lockArgs := []interface{}{params.Rule.Database, params.Rule.Table}
	for _, key := range params.Rule.LockColumns {
		lockArgs = append(lockArgs, params.Data[key])
	}

	tpl := strings.TrimRight(strings.Repeat("%s_", len(params.Rule.LockColumns)), "_")
	// 组装分布式锁的KEY
	lockKey := fmt.Sprintf(strings.ReplaceAll(syncLockKeyTpl, ":keys", tpl), lockArgs...)
	mutex := h.rs.NewMutex(lockKey, redsync.WithExpiry(3*time.Second), redsync.WithRetryDelay(100*time.Millisecond))
	if err := mutex.Lock(); err != nil {
		h.writeLog(params, err)
		return nil, lockKey
	}

	return mutex, lockKey
}

// unlockRecordByMutex 解锁分布式锁
func (h *Handler) unlockRecordByMutex(mutex *redsync.Mutex, lockKey string) {
	if _, err := mutex.Unlock(); err != nil {
		zap.L().Error("redis 解锁失败", zap.Error(err), zap.String("key", lockKey))
	}
}

// insert insert 事件同步方法
func (h *Handler) insert(params *types.SyncParams) ([]string, error) {
	// 判断同步过滤条件是否通过
	if !params.Rule.EvaluateFilterConditions(params.Data) {
		return nil, nil
	}
	// 插入的数据，如果软删除字段已存在标识，不执行同步操作直接返回
	if deletedColumnValue, ok := params.Data[params.Rule.SoftDeleteField]; ok &&
		deletedColumnValue != params.Rule.UnSoftDeleteValue {
		return nil, emptyErr
	}

	writer, err := h.wp.GetWriter(params.Rule.TargetType)
	if err != nil {
		return nil, err
	}

	var updatedColumns []string
	for column := range params.Rule.Columns {
		updatedColumns = append(updatedColumns, column)
	}

	// 过滤需要插入的字段
	// 一条记录的每次修改都记录了修改的字段和修改时间
	// 如果本次修改时间小于已记录的修改时间 (数据落后)，则只修改未修改的字段
	columns, err, isNotEmpty := h.filter.FilterColumns(params, updatedColumns)
	if err != nil {
		return nil, err
	} else if !isNotEmpty {
		return nil, emptyErr
	}
	values := params.GetUpdateValues(columns)

	return columns, writer.Insert(params, values)
}

// update update 同步事件
func (h *Handler) update(params *types.SyncParams) ([]string, error) {
	// 主键更新, 执行老记录删除，和新记录新增
	if params.IsPrimaryKeyUpdated() {
		if err := h.Invoke(params.Clone(types.EventTypeInsert)); err != nil {
			return nil, err
		}
		deleteParams := params.Clone(types.EventTypeDelete)
		deleteParams.Data, deleteParams.Old = deleteParams.MergeOldToData(), nil
		if err := h.Invoke(deleteParams); err != nil {
			return nil, err
		}

		return nil, emptyErr
	}

	oldDeletedValue, oldOk := params.Old[params.Rule.SoftDeleteField]
	dataDeletedValue := params.Data[params.Rule.SoftDeleteField]
	if oldOk && oldDeletedValue == params.Rule.UnSoftDeleteValue {
		// 被更新的 软删除字段 被更新之前为0，代表本条数据被删除
		params.RealEventType = types.EventTypeDelete
		return nil, h.delete(params)
	} else if oldOk && oldDeletedValue != params.Rule.UnSoftDeleteValue &&
		dataDeletedValue == params.Rule.UnSoftDeleteValue {
		// 被更新的 软删除字段 被更新钱不为 0，且 更新后的值为 0， 代表数据被恢复
		params.RealEventType = types.EventTypeInsert
		return h.insert(params)
	}

	oldFilterOk := params.Rule.EvaluateFilterConditions(params.MergeOldToData())
	dataFilterOk := params.Rule.EvaluateFilterConditions(params.Data)
	// 判断同步过滤条件是否通过
	if !dataFilterOk && oldFilterOk { // 新数据判断不通过，老数据通过，删除老数据
		params.RealEventType = types.EventTypeDelete
		return nil, h.delete(params)
	} else if dataFilterOk && !oldFilterOk { // 新数据判断通过，老数据不通过，新增数据
		params.RealEventType = types.EventTypeInsert
		return h.insert(params)
	} else if !dataFilterOk && !oldFilterOk {
		return nil, nil
	}

	return h.realUpdate(params)
}

// realUpdate 真实 update 方法
func (h *Handler) realUpdate(params *types.SyncParams) ([]string, error) {
	writer, err := h.wp.GetWriter(params.Rule.TargetType)
	if err != nil {
		return nil, err
	}

	var updatedColumns []string
	for column := range params.Old {
		updatedColumns = append(updatedColumns, column)
	}

	columns, err, isNotEmpty := h.filter.FilterColumns(params, updatedColumns)
	if err != nil {
		return nil, err
	} else if !isNotEmpty {
		return nil, emptyErr
	}
	values := params.GetUpdateValues(columns)

	return columns, writer.Update(params, values)
}

// delete delete 事件同步方法
func (h *Handler) delete(params *types.SyncParams) error {
	_, err, isNotEmpty := h.filter.FilterColumns(params, nil)
	if !isNotEmpty {
		return err
	}
	writer, err := h.wp.GetWriter(params.Rule.TargetType)
	if err != nil {
		return err
	}

	return writer.Delete(params)
}

// writeLog 写入日志，并追加错误到本次执行参数中
func (h *Handler) writeLog(params *types.SyncParams, err error) {
	zap.L().Error("同步失败", zap.Reflect("params", params), zap.Error(err))
	params.GetWg().AddErr(err)
}

// Release 释放处理器所有资源
func (h *Handler) Release() {
	h.pool.Release()
	for _, writer := range h.wp.GetWriters() {
		_ = writer.GetDataSource().Close()
	}
}
