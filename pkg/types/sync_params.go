package types

import (
	"strings"
	"sync"
)

// SyncParams 同步参数
// 一条 sql 可能修改多条数据，每条数据都会拆分成不同都子任务, 每个子任务都有自己都 SyncParams
// 所以 SyncParams 不涉及并发
type SyncParams struct {
	wg            *syncWaitGroup
	Rule          SyncRule          `json:"rule"` // 只读，不用指针传递
	Data          map[string]string `json:"data"`
	Old           map[string]string `json:"old"`
	binLogParams  *BinlogParams
	RealEventType string `json:"real_event_type"` // 和 BinlogParams 的 EventType 重复，用于记录真实执行同步的事件类型
	joinColumn    string
}

func (s *SyncParams) GetWg() *syncWaitGroup {
	return s.wg
}

func NewSyncParams(wg *syncWaitGroup, rule *SyncRule, data, old map[string]string, binLog *BinlogParams) *SyncParams {
	params := _syncParamsPool.Get().(*SyncParams)
	params.wg, params.Rule, params.Data, params.Old, params.binLogParams = wg, *rule, data, old, binLog
	params.joinColumn, params.RealEventType = "", binLog.EventType

	return params
}

func (s *SyncParams) Recycle() {
	s.wg.Done()
	_syncParamsPool.Put(s)
}

func (s *SyncParams) GetBingLogParams() *BinlogParams {
	return s.binLogParams
}

func (s *SyncParams) SetBinLogParams(params *BinlogParams) {
	s.binLogParams = params
}

// GetJoinColumn 当 SyncType 等于 SyncTypeInner 时只同步一个字段, 暂时缓存起来
// 同一个 SyncParams 只会被一个协程使用， 所以不存在并发问题
func (s *SyncParams) GetJoinColumn() string {
	if s.Rule.SyncType == SyncTypeInner {
		if s.joinColumn == "" {
			for local, target := range s.Rule.Columns {
				if target == s.Rule.JoinFieldName {
					s.joinColumn = local
				}
			}
		}

		return s.joinColumn
	}

	return ""
}

// MergeOldToData 获取这条记录 更新之前的所有数据
func (s *SyncParams) MergeOldToData() map[string]string {
	newData := s.Data

	if s.Old != nil {
		newData = make(map[string]string, len(s.Data))
		for key, value := range s.Data {
			if oldValue, ok := s.Old[key]; ok {
				newData[key] = oldValue
			} else {
				newData[key] = value
			}
		}
	}

	return newData
}

// GetUpdateValues 获取当次更新的数据 格式: {"column": "value"}
func (s *SyncParams) GetUpdateValues(updatedColumns []string) interface{} {

	switch s.Rule.SyncType {
	case SyncTypeCopy, SyncTypeJoin: // 拷贝记录，作为目标表的一条新记录
		record := make(map[string]string, len(updatedColumns)+len(s.Rule.TargetExtraParams))
		for _, column := range updatedColumns {
			record[s.Rule.Columns[column]] = s.Data[column]
		}

		if s.Rule.TargetExtraParams != nil {
			for extraColumn, extraValue := range s.Rule.TargetExtraParams {
				record[extraColumn] = extraValue
			}
		}

		if s.Rule.SyncType == SyncTypeJoin {
			return map[string]interface{}{
				s.Rule.Columns[s.Rule.PrimaryKey]: s.Data[s.Rule.PrimaryKey],
				s.Rule.JoinFieldName:              record,
			}
		}

		return record
	case SyncTypeInner:
		// 只取一个字段, 作为目标表的某个字段的 一个元素, 例如es数组 add
		return s.Data[s.GetJoinColumn()]
	}

	return nil
}

// IsPrimaryKeyUpdated 判断主键 或关联字段是否更新
func (s *SyncParams) IsPrimaryKeyUpdated() bool {
	_, primaryKeyUpdated := s.Old[s.Rule.PrimaryKey]

	if !primaryKeyUpdated && s.Rule.SyncType == SyncTypeInner {
		_, primaryKeyUpdated = s.Old[s.GetJoinColumn()]
	}

	return primaryKeyUpdated
}

// GetIdentifyId 获取标识id
func (s *SyncParams) GetIdentifyId() string {
	identifyIdColumns := make([]string, len(s.Rule.LockColumns))
	for _, column := range s.Rule.LockColumns {
		identifyIdColumns = append(identifyIdColumns, column)
	}

	return strings.Join(identifyIdColumns,
		identifyIdColumnSeparator)
}

// Clone 克隆一个新的同步数据，只有事件类型不同
// 主要用户软删除字段更新时，更新事件并更为 删除/插入事件
func (s *SyncParams) Clone(eventType string) *SyncParams {
	params := _syncParamsPool.Get().(*SyncParams)
	params.wg, params.Rule, params.Data = s.wg, s.Rule, s.Data
	params.Old, params.binLogParams = s.Old, s.binLogParams
	params.RealEventType = eventType

	return params
}

type syncWaitGroup struct {
	sync.WaitGroup
	mux    sync.Mutex
	errors []error
}

func NewSyncWaitGroup() *syncWaitGroup {
	wg := _syncWaitGroupPool.Get().(*syncWaitGroup)
	wg.errors = nil

	return wg
}

func (wg *syncWaitGroup) AddErr(errArr ...error) {
	wg.mux.Lock()
	defer wg.mux.Unlock()

	wg.errors = append(wg.errors, errArr...)
}

func (wg *syncWaitGroup) Recycle() {
	_syncWaitGroupPool.Put(wg)
}

func (wg *syncWaitGroup) Errors() []error {
	return wg.errors
}

var (
	_syncParamsPool = sync.Pool{
		New: func() interface{} {
			return &SyncParams{}
		},
	}

	_syncWaitGroupPool = sync.Pool{
		New: func() interface{} {
			return &syncWaitGroup{}
		},
	}
)
