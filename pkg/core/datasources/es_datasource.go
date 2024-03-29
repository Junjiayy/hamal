package datasources

import (
	"fmt"
	"github.com/olivere/elastic/v7"
)

type ElasticSearchDataSource struct {
	*DataSourceBase
}

func NewElasticSearchDataSource() DataSource {
	return &ElasticSearchDataSource{
		NewDataSourceBase(newEsConnectFunc, closeEsConnectFunc),
	}
}

// newEsConnectFunc 通过配置创建新的 es 连接函数
func newEsConnectFunc(config DataSourceConfig) (interface{}, error) {
	url := fmt.Sprintf("http://%s:%d", config.Host, config.Port)
	options := []elastic.ClientOptionFunc{elastic.SetURL(url), elastic.SetSniff(false)}
	if config.Username != "" && config.Password != "" {
		options = append(options, elastic.SetBasicAuth(config.Username, config.Password))
	}

	return elastic.NewClient(options...)
}

// closeEsConnectFunc 关闭 es 连接函数
func closeEsConnectFunc(cli interface{}) error {
	// 实际上 es 客户端是通过 http 请求发送命令的
	// 所以严格意义上来说可以不用关闭 客户端
	return nil
}
