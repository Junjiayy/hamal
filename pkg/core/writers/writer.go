package writers

import (
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
)

type (
	Writer interface {
		Insert(params *types.SyncParams, values interface{}) error
		Update(params *types.SyncParams, values interface{}) error
		Delete(params *types.SyncParams) error
		GetDataSource() datasources.DataSource
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

// writer 写入器基础结构
type writer struct {
	dataSources datasources.DataSource
}

// GetDataSource 获取当前写入器数据源
func (w *writer) GetDataSource() datasources.DataSource {
	return w.dataSources
}

// WriterPool 写入器池
type WriterPool struct {
	ws    map[string]Writer
	rwMux *sync.RWMutex
}

func NewWriterPool() *WriterPool {
	return &WriterPool{
		ws:    make(map[string]Writer),
		rwMux: new(sync.RWMutex),
	}
}

// SetConfigs 给所有写入器更新配置，添加新增写入器，删除移除写入器
func (wp *WriterPool) SetConfigs(configs map[string]map[string]datasources.DataSourceConfig) error {
	wp.rwMux.Lock()
	defer wp.rwMux.Unlock()

	var lastErr error
	for wType, config := range configs {
		w, ok := wp.ws[wType]
		if !ok {
			// 如果需要更新配置的写入器不存在，直接创建
			// notice: 不用考虑数据源连接浪费问题，数据源只有用到时才初始化
			writerConstructor := GetWriterConstructor(wType)
			dataSourceConstructor := datasources.GetDataSourceConstructor(wType)
			if writerConstructor == nil || dataSourceConstructor == nil {
				zap.L().Error("writer constructor not exists", zap.String("type", wType))
				lastErr = errors.Errorf("%s writer constructor not exists", wType)
				continue
			} else {
				w = writerConstructor(dataSourceConstructor())
				wp.ws[wType] = w
			}
		}

		if err := w.GetDataSource().SetConfigs(config); err != nil {
			lastErr = err
		}
	}

	for wType, w := range wp.ws {
		if _, ok := configs[wType]; !ok {
			// 回收已删除的写入器
			if err := w.GetDataSource().Close(); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}

// GetWriter 获取写入器
func (wp *WriterPool) GetWriter(wType string) (Writer, error) {
	wp.rwMux.RLock()
	defer wp.rwMux.RUnlock()

	w, ok := wp.ws[wType]
	if !ok {
		return nil, errors.Errorf("%s writer not exists", w)
	}

	return w, nil
}

// GetWriters 获取所有写入器
func (wp *WriterPool) GetWriters() map[string]Writer {
	return wp.ws
}
