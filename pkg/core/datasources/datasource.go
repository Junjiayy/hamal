package datasources

import "github.com/Junjiayy/hamal/pkg/types"

type (
	DataSourceConfig struct {
		Name     string `json:"name" yaml:"name"`                             // 连接名称 唯一
		Type     string `json:"type" yaml:"type"`                             // 配置类型 mysql es
		Host     string `json:"host" yaml:"host"`                             // ip
		Port     int    `json:"port" yaml:"port"`                             // 端口
		Username string `json:"username,omitempty" yaml:"username,omitempty"` // 账户 可为空
		Password string `json:"password,omitempty" yaml:"password,omitempty"` // 密码 可为空
		Target   string `json:"target,omitempty" yaml:"target,omitempty"`     // type 为mysql时未目标库 es时为空
	}

	DataSource interface {
		SetDataSource(conf *DataSourceConfig) error
		GetDataSource(name string) (interface{}, error)
	}
)

var _datasourceConstructors = make(map[string]func() DataSource) // 数据源构造函数 映射表

func init() {
	SetDataSourceConstructor(types.DataSourceMysql, NewMysqlDataSource)
}

// SetDataSourceConstructor 设置数据源构造函数
func SetDataSourceConstructor(name string, fn func() DataSource) {
	_datasourceConstructors[name] = fn
}

// GetDataSourceConstructor 获取数据源构造函数
func GetDataSourceConstructor(name string) func() DataSource {
	return _datasourceConstructors[name]
}
