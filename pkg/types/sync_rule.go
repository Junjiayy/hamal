package types

import (
	"encoding/json"
	"github.com/Junjiayy/hamal/pkg/tools"
	"github.com/pkg/errors"
	"strings"
)

type (
	// SyncRule 同步规则
	SyncRule struct {
		Database          string                           `json:"database" yaml:"database"`                                       // 需要同步的库
		Table             string                           `json:"table" yaml:"table"`                                             // 需要同步的表
		PrimaryKey        string                           `json:"primary_key" yaml:"primary_key"`                                 // 来源表中主键名称
		LockColumns       []string                         `json:"lock_columns" yaml:"lock_columns"`                               // 加锁时 依赖的字段
		Columns           map[string]string                `json:"columns" yaml:"columns"`                                         // 字段映射表 local:target 格式
		SystemColumns     []string                         `json:"-" yaml:"-"`                                                     // 系统字段 (特殊逻辑)
		SoftDeleteField   string                           `json:"soft_delete_field,omitempty" yaml:"soft_delete_field,omitempty"` // 软删除字段名称 为空代表不支持软删除
		UnSoftDeleteValue string                           `json:"un_soft_delete_value" yaml:"un_soft_delete_value"`               // 未软删除值 SoftDeleteField 不为空且作为key获取到的值不相等及被软删除
		DataConditions    map[string][]DataFilterCondition `json:"data_conditions,omitempty" yaml:"data_conditions,omitempty"`     // 同步条件 key为 and或or  比对结果false 不同步
		TargetType        string                           `json:"-" yaml:"target_type"`                                           // 目标类型 mysql|es
		Target            string                           `json:"target" yaml:"target"`                                           // 目标 mysql:connect.database.table es:connect.index
		TargetDatabase    string                           `json:"-" yaml:"target_database"`
		TargetTable       string                           `json:"-" yaml:"target_table"`
		//Type              string                     `json:"type"`                 // 同步类型 stats:统计 sync:同步
		SyncType      string `json:"sync_type" yaml:"sync_type"`                                 // 具体同步或统计类型
		JoinFieldName string `json:"join_field_name,omitempty" yaml:"join_field_name,omitempty"` // 加入字段名 sync_type:join|inner 时存在
		//SyncConditions    []SyncCondition            `json:"sync_conditions"`      // 同步条件 只允许and条件
		TargetExtraParams map[string]string `json:"target_extra_params,omitempty" yaml:"target_extra_params,omitempty"` // 目标额外参数，常量同步时一起写入目标表
	}

	innerSyncRule SyncRule

	// DataFilterCondition  数据规则条件
	DataFilterCondition struct {
		Column      string                           `json:"column,omitempty"`       // 字段名
		Operator    string                           `json:"operator,omitempty"`     // 运算符
		Value       string                           `json:"value,omitempty"`        // 值
		ValueColumn string                           `json:"value_column,omitempty"` // 值字段  value 和 value_column 同时只存在其中一个
		Children    map[string][]DataFilterCondition `json:"children,omitempty"`     // 子条件
	}
)

func (sr *SyncRule) UnmarshalJSON(bytes []byte) error {
	innerSr := (*innerSyncRule)(sr)
	if err := json.Unmarshal(bytes, innerSr); err != nil {
		return err
	}
	targets := strings.Split(innerSr.Target, ":")
	if len(targets) != 2 {
		return errors.Errorf("target formt error, type:connect(.db).table, current: %s", innerSr.Target)
	}

	innerSr.TargetType = targets[0]
	targetParams := strings.Split(targets[1], ".")
	if length := len(targetParams); length < 2 {
		return errors.Errorf("target format error connect(.db).table, current: %s", targets[1])
	} else if length >= 3 {
		innerSr.Target, innerSr.TargetDatabase, innerSr.TargetTable = targetParams[0], targetParams[1], targetParams[2]
	} else {
		innerSr.Target, innerSr.TargetTable = targetParams[0], targetParams[1]
	}

	return nil
}

// EvaluateFilterConditions 判断是否符合同步条件
func (sr *SyncRule) EvaluateFilterConditions(data map[string]string) bool {
	if sr.DataConditions != nil {
		return sr.evaluateFilterConditions(sr.DataConditions, data)
	}

	return true
}

// evaluateFilterConditions 判断是否符合同步条件
func (sr *SyncRule) evaluateFilterConditions(conditions map[string][]DataFilterCondition, data map[string]string) bool {
	var ok bool

	for operator, ruleConditions := range conditions {
		ok = ok || operator == ConditionTypeAnd
		switch operator {
		case ConditionTypeAnd:
			for _, condition := range ruleConditions {
				if value, columnExists := data[condition.Column]; columnExists {
					if ok = ok && tools.JudgmentEval(value, condition.Value,
						condition.Operator); !ok {
						return false
					}
				} else if condition.Column != "" {
					return false
				}

				if len(condition.Children) > 0 {
					if ok = ok && sr.evaluateFilterConditions(condition.Children,
						data); !ok {
						return false
					}
				}
			}
		case ConditionTypeOr:
			for _, condition := range ruleConditions {
				if value, columnExists := data[condition.Column]; columnExists {
					if ok = ok || tools.JudgmentEval(value, condition.Value,
						condition.Operator); ok {
						return true
					}
				}

				if len(condition.Children) > 0 {
					if ok = ok || sr.evaluateFilterConditions(condition.Children,
						data); !ok {
						return true
					}
				}
			}
		}
	}

	return ok
}
