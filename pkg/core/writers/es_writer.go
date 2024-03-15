package writers

import (
	"context"
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type ElasticSearchWriter struct {
	*writer
}

func NewElasticSearchWriter(source datasources.DataSource) Writer {
	return &ElasticSearchWriter{&writer{dataSources: source}}
}

const (
	updateInnerJoinScriptTpl = "if(ctx._source.:key == null) { ctx._source.:key = [params.value] } else if(!ctx._source.:key.contains(params.value)) { ctx._source.:key.add(params.value) }"
	deleteInnerJoinScriptTpl = "if(ctx._source.:key != null && ctx._source.:key.contains(params.value)) { ctx._source.:key.remove(ctx._source.:key.indexOf(params.value)) }"
	removeFieldScriptTpl     = "if(ctx._source.:key != null) {ctx._source.remove(':key')}"
)

func (e *ElasticSearchWriter) Insert(params *types.SyncParams, values interface{}) error {
	cliInter, err := e.dataSources.GetDataSource(params.Rule.Target)
	if err != nil {
		return err
	}

	cli, primaryKeyValue := cliInter.(*elastic.Client), params.Data[params.Rule.PrimaryKey]
	updateService := elastic.NewUpdateService(cli).Index(params.Rule.TargetTable).Id(primaryKeyValue)
	timeout, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFunc()

	if params.Rule.SyncType == types.SyncTypeInner {
		scriptStr := strings.ReplaceAll(updateInnerJoinScriptTpl, ":key", params.Rule.JoinFieldName)
		updateService.Script(elastic.NewScriptInline(scriptStr).Param("value", values))
		values = map[string]interface{}{
			params.Rule.JoinFieldName:                   []string{values.(string)},
			params.Rule.Columns[params.Rule.PrimaryKey]: primaryKeyValue,
		}
	} else {
		updateService.Doc(values)
	}

	if _, err = updateService.Upsert(values).Do(timeout); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (e *ElasticSearchWriter) Update(params *types.SyncParams, values interface{}) error {
	// types.SyncTypeJoin 不存在 update 事件
	return e.Insert(params, values)
}

func (e *ElasticSearchWriter) Delete(params *types.SyncParams) error {
	cliInter, err := e.dataSources.GetDataSource(params.Rule.Target)
	if err != nil {
		return err
	}
	cli, primaryKeyValue := cliInter.(*elastic.Client), params.Data[params.Rule.PrimaryKey]
	timeout, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFunc()

	if params.Rule.SyncType == types.SyncTypeCopy {
		_, err := elastic.NewDeleteService(cli).Index(params.Rule.TargetTable).
			Id(primaryKeyValue).Do(timeout)
		return errors.WithStack(err)
	}

	updateService := elastic.NewUpdateService(cli).Index(params.Rule.TargetTable).Id(primaryKeyValue)

	var script *elastic.Script
	switch params.Rule.SyncType {
	case types.SyncTypeInner:
		updateValue := params.Data[params.GetJoinColumn()]
		scriptStr := strings.ReplaceAll(deleteInnerJoinScriptTpl, ":key", params.Rule.JoinFieldName)
		script = elastic.NewScriptInline(scriptStr).Param("value", updateValue)
	case types.SyncTypeJoin:
		scriptStr := strings.ReplaceAll(removeFieldScriptTpl, ":key", params.Rule.JoinFieldName)
		script = elastic.NewScriptInline(scriptStr)
	}

	if _, err := updateService.Script(script).Do(timeout); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
