package datasources

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MysqlDataSource struct {
	*DataSourceBase
}

func NewMysqlDataSource() DataSource {
	return &MysqlDataSource{
		NewDataSourceBase(newMysqlConnectFunc, closeMysqlConnectFunc),
	}
}

// newMysqlConnectFunc 通过配置创建 mysql 连接函数
func newMysqlConnectFunc(conf DataSourceConfig) (interface{}, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		conf.Username, conf.Password, conf.Host, conf.Port, conf.Target)
	connectConfig := &gorm.Config{Logger: logger.Default.LogMode(logger.Info)}
	if conf.Debug {
		connectConfig.Logger = logger.Default.LogMode(logger.Warn)
	}

	return gorm.Open(mysql.Open(dsn), connectConfig)
}

// closeMysqlConnectFunc 关闭 mysql 连接函数
func closeMysqlConnectFunc(cli interface{}) error {
	// todo: 考虑优雅关闭的问题，如果当前连接正在被使用怎么处理
	sqlDb, err := cli.(*gorm.DB).DB()
	if err != nil {
		return err
	}

	return sqlDb.Close()
}
