package datasources

import (
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

type ElasticSearchDataSource struct {
	clients map[string]*elastic.Client
}

func NewElasticSearchDataSource() DataSource {
	return &ElasticSearchDataSource{clients: make(map[string]*elastic.Client)}
}

// SetDataSource 设置数据源
func (e *ElasticSearchDataSource) SetDataSource(conf *DataSourceConfig) error {
	url := fmt.Sprintf("http://%s:%d", conf.Host, conf.Port)
	options := []elastic.ClientOptionFunc{elastic.SetURL(url), elastic.SetSniff(false)}
	if conf.Username != "" && conf.Password != "" {
		options = append(options, elastic.SetBasicAuth(conf.Username, conf.Password))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return errors.WithStack(err)
	}
	e.clients[conf.Name] = client

	return nil
}

func (e *ElasticSearchDataSource) GetDataSource(name string) (interface{}, error) {
	cli, ok := e.clients[name]
	if !ok {
		return nil, errors.Errorf("get not exists connect %s", name)
	}

	return cli, nil
}
