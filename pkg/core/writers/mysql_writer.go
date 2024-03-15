package writers

import (
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type MysqlWriter struct {
	*writer
}

var syncTypeErr = errors.New("mysql writer only support copy")

func NewMysqlWriter(source datasources.DataSource) Writer {
	return &MysqlWriter{&writer{dataSources: source}}
}

func (w *MysqlWriter) Insert(params *types.SyncParams, values interface{}) error {
	strMapValues, ok := values.(map[string]string)
	if !ok {
		return errors.New("mysql writer only support copy, so values type must be map[string]interface{}")
	}

	cliInter, err := w.dataSources.GetDataSource(params.Rule.Target)
	if err != nil {
		return err
	}
	if params.Rule.SyncType != types.SyncTypeCopy {
		// mysql 同步仅支持数据拷贝，不支持 join 和 inner
		return syncTypeErr
	}

	cli := cliInter.(*gorm.DB)
	tx := cli.Table(params.Rule.TargetTable).Create(strMpaToInterMap(strMapValues))

	return tx.Error
}

func (w *MysqlWriter) Update(params *types.SyncParams, values interface{}) error {
	strMapValues, ok := values.(map[string]string)
	if !ok {
		return errors.New("mysql writer only support copy, so values type must be map[string]interface{}")
	}
	if params.Rule.SyncType != types.SyncTypeCopy {
		// mysql 同步仅支持数据拷贝，不支持 join 和 inner
		return syncTypeErr
	}

	cliInter, err := w.dataSources.GetDataSource(params.Rule.Target)
	if err != nil {
		return err
	}

	primaryKeyValue := params.Data[params.Rule.PrimaryKey]
	primaryColumn := params.Rule.Columns[params.Rule.PrimaryKey]
	cli := cliInter.(*gorm.DB)
	tx := cli.Table(params.Rule.TargetTable).Where(primaryColumn, primaryKeyValue).
		Updates(strMpaToInterMap(strMapValues))

	return tx.Error
}

func (w *MysqlWriter) Delete(params *types.SyncParams) error {
	cliInter, err := w.dataSources.GetDataSource(params.Rule.Target)
	if err != nil {
		return err
	}
	if params.Rule.SyncType != types.SyncTypeCopy {
		// mysql 同步仅支持数据拷贝，不支持 join 和 inner
		return syncTypeErr
	}

	primaryKeyValue := params.Data[params.Rule.PrimaryKey]
	primaryColumn := params.Rule.Columns[params.Rule.PrimaryKey]
	cli := cliInter.(*gorm.DB)
	tx := cli.Table(params.Rule.TargetTable).Where(primaryColumn, primaryKeyValue).
		Delete(nil)

	return tx.Error
}

func strMpaToInterMap(sources map[string]string) map[string]interface{} {
	res := make(map[string]interface{}, len(sources))
	for key, value := range sources {
		res[key] = value
	}

	return res
}
