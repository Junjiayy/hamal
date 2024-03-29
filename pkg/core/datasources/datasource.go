package datasources

import (
	"github.com/Junjiayy/hamal/pkg/tools/logs"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"reflect"
	"sync"
)

type (
	DataSourceConfig struct {
		Name     string `json:"name" yaml:"name"`                             // 连接名称 唯一
		Type     string `json:"type" yaml:"type"`                             // 配置类型 mysql es
		Host     string `json:"host" yaml:"host"`                             // ip
		Port     int    `json:"port" yaml:"port"`                             // 端口
		Username string `json:"username,omitempty" yaml:"username,omitempty"` // 账户 可为空
		Password string `json:"password,omitempty" yaml:"password,omitempty"` // 密码 可为空
		Target   string `json:"target,omitempty" yaml:"target,omitempty"`     // type 为mysql时未目标库 es时为空
		Debug    bool   `json:"debug,omitempty" yaml:"debug,omitempty"`
	}

	DataSource interface {
		GetDataSource(name string) (interface{}, error)
		SetConfigs(configs map[string]DataSourceConfig) error
		Close() error
	}
)

var _datasourceConstructors = make(map[string]func() DataSource) // 数据源构造函数 映射表

func init() {
	SetDataSourceConstructor(types.DataSourceMysql, NewMysqlDataSource)
	SetDataSourceConstructor(types.DataSourceElasticSearch, NewElasticSearchDataSource)
}

// SetDataSourceConstructor 设置数据源构造函数
func SetDataSourceConstructor(name string, fn func() DataSource) {
	_datasourceConstructors[name] = fn
}

// GetDataSourceConstructor 获取数据源构造函数
func GetDataSourceConstructor(name string) func() DataSource {
	return _datasourceConstructors[name]
}

// DataSourceBase 数据源基础类型
type DataSourceBase struct {
	clients       map[string]interface{}
	configs       map[string]DataSourceConfig
	configRwMux   *sync.RWMutex
	clientMux     *sync.Mutex
	newConnFunc   func(config DataSourceConfig) (interface{}, error)
	closeConnFunc func(conn interface{}) error
}

func NewDataSourceBase(newConnFunc func(config DataSourceConfig) (interface{}, error),
	closeConnFunc func(conn interface{}) error) *DataSourceBase {
	return &DataSourceBase{
		clients:     make(map[string]interface{}),
		configRwMux: new(sync.RWMutex),
		clientMux:   new(sync.Mutex),
		newConnFunc: newConnFunc, closeConnFunc: closeConnFunc,
	}
}

// SetConfigs 设置配置，如果配置变更或已删除，则关闭对应数据源
func (d *DataSourceBase) SetConfigs(configs map[string]DataSourceConfig) error {
	d.configRwMux.Lock()
	defer d.configRwMux.Unlock()

	var lastErr error
	if d.configs != nil {
		for name, config := range configs {
			if existsConfig, ok := d.configs[name]; ok {
				// 如果配置已存在，且与本次更新的配置完全相等，直接跳过本次循环
				if reflect.DeepEqual(config, existsConfig) {
					continue
				}
			}

			if cli, ok := d.clients[name]; ok {
				if err := d.closeConnFunc(cli); err != nil {
					logs.Error("close datasource cli failed", err, zap.String("name", name))
					lastErr = errors.WithStack(err)
				}

				delete(d.clients, name)
			}
		}
	}

	d.configs = configs

	return lastErr
}

// GetDataSource 获取数据源，如果数据源不存在根据 config 创建
func (d *DataSourceBase) GetDataSource(name string) (interface{}, error) {
	d.configRwMux.RLock()
	defer d.configRwMux.RUnlock()

	if cli, ok := d.clients[name]; ok {

		return cli, nil
	}

	d.clientMux.Lock()
	defer d.clientMux.Unlock()
	// double check
	if cli, ok := d.clients[name]; ok {

		return cli, nil
	}
	conf, ok := d.configs[name]
	if !ok {
		return nil, errors.Errorf("get not exists connect %s", name)
	}
	conn, err := d.newConnFunc(conf)
	if err != nil {
		return nil, err
	}

	d.clients[conf.Name] = conn

	return conn, nil
}

// Close 关闭当前数据源的所有连接
func (d *DataSourceBase) Close() error {
	d.configRwMux.Lock()
	defer d.configRwMux.Unlock()

	var lastErr error
	for name, cli := range d.clients {
		if err := d.closeConnFunc(cli); err != nil {
			logs.Error("close datasource cli failed", err, zap.String("name", name))
			lastErr = err
		}
	}

	d.configs, d.clients = nil, nil

	return lastErr
}
