package datasources

import (
	"fmt"
	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MysqlDataSource struct {
	clients map[string]*gorm.DB
}

func NewMysqlDataSource() DataSource {
	return &MysqlDataSource{make(map[string]*gorm.DB)}
}

func (m *MysqlDataSource) SetDataSource(conf *DataSourceConfig) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		conf.Username, conf.Password, conf.Host, conf.Port, conf.Target)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Info)})
	if err != nil {
		return err
	}
	m.clients[conf.Name] = db

	return nil
}

func (m *MysqlDataSource) GetDataSource(name string) (interface{}, error) {
	cli, ok := m.clients[name]
	if !ok {
		return nil, errors.Errorf("get not exists connect %s", name)
	}

	return cli, nil
}
