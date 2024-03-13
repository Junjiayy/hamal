package types

import (
	"encoding/json"
	"strings"
)

type (
	BinlogParams struct {
		EventId   string              `json:"event_id" binding:"required"` // 事件ID 唯一
		Database  string              `json:"database" binding:"required"` // 库名
		Table     string              `json:"table" binding:"required"`    // 表名
		EventAt   int64               `json:"ts" binding:"required"`       // 事件时间
		EventType string              `json:"type" binding:"required"`     // 事件类型
		IsDdl     bool                `json:"isDdl" binding:"omitempty"`   // 是否 ddl修改(ddl 修改不处理)
		Data      []map[string]string `json:"data" binding:"required"`     // 更新后数据 (全量数据，根据 canal: canal.instance.filter.regex 的字段规则，没有字段规则就是全量)
		Old       []map[string]string `json:"old" binding:"omitempty"`     // 更新前数据 (只存在被更新的字段)
		Source    interface{}         `json:"-" binding:"omitempty"`       // 原始数据
	}

	innerBinlogParams BinlogParams
)

// UnmarshalJSON 重写 json 解析方法，如果是更新事件，记录本次更新的字段
func (c *BinlogParams) UnmarshalJSON(bytes []byte) error {
	innerParams := (*innerBinlogParams)(c)
	if err := json.Unmarshal(bytes, innerParams); err != nil {
		return err
	}
	innerParams.EventType = strings.ToLower(innerParams.EventType)

	return nil
}

// SyncRule 同步规则
type SyncRule struct {
	Database          string            `json:"database" yaml:"database"`                                       // 需要同步的库
	Table             string            `json:"table" yaml:"table"`                                             // 需要同步的表
	PrimaryKey        string            `json:"primary_key" yaml:"primary_key"`                                 // 来源表中主键名称
	LockColumns       []string          `json:"lock_columns" yaml:"lock_columns"`                               // 加锁时 依赖的字段
	Columns           map[string]string `json:"columns" yaml:"columns"`                                         // 字段映射表 local:target 格式
	SystemColumns     []string          `json:"-" yaml:"-"`                                                     // 系统字段 (特殊逻辑)
	SoftDeleteField   string            `json:"soft_delete_field,omitempty" yaml:"soft_delete_field,omitempty"` // 软删除字段名称 为空代表不支持软删除
	UnSoftDeleteValue string            `json:"un_soft_delete_value" yaml:"un_soft_delete_value"`               // 未软删除值 SoftDeleteField 不为空且作为key获取到的值不相等及被软删除
	//DataConditions    map[string][]DataCondition `json:"data_conditions"`      // 同步条件 key为 and或or  比对结果false 不同步
	TargetType     string `json:"-" yaml:"target_type"` // 目标类型 mysql|es
	Target         string `json:"target" yaml:"target"` // 目标 mysql:connect.database.table es:connect.index
	TargetDatabase string `json:"-" yaml:"target_database"`
	TargetTable    string `json:"-" yaml:"target_table"`
	//Type              string                     `json:"type"`                 // 同步类型 stats:统计 sync:同步
	SyncType      string `json:"sync_type" yaml:"sync_type"`                                 // 具体同步或统计类型
	JoinFieldName string `json:"join_field_name,omitempty" yaml:"join_field_name,omitempty"` // 加入字段名 sync_type:join|inner 时存在
	//SyncConditions    []SyncCondition            `json:"sync_conditions"`      // 同步条件 只允许and条件
}
