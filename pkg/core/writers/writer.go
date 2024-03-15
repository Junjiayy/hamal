package writers

import (
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
)

type (
	writer struct {
		dataSources datasources.DataSource
	}

	Writer interface {
		Insert(params *types.SyncParams, values interface{}) error
		Update(params *types.SyncParams, values interface{}) error
		Delete(params *types.SyncParams) error
	}

	WriterConstructor func(datasources.DataSource) Writer
)

var _writerConstructors = make(map[string]WriterConstructor) // 写入构造函数 映射表

func init() {
	SetWriterConstructor(types.DataSourceMysql, NewMysqlWriter)
	SetWriterConstructor(types.DataSourceElasticSearch, NewElasticSearchWriter)
}

func SetWriterConstructor(name string, fn WriterConstructor) {
	_writerConstructors[name] = fn
}

// GetWriterConstructor 获取写入构造函数
func GetWriterConstructor(name string) WriterConstructor {
	return _writerConstructors[name]
}
