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
