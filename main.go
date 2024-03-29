package main

import (
	"flag"
	"github.com/Junjiayy/hamal/internal/core"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/core/writers"
	"github.com/Junjiayy/hamal/pkg/tools"
	"github.com/Junjiayy/hamal/pkg/tools/logs"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/ioutil"
	"log"
	"os"
)

var filePath = flag.String("f", "config.yaml", "Specify the config file")

func main() {
	flag.Parse()

	content, err := ioutil.ReadFile(*filePath)
	if err != nil {
		log.Fatalln(err)
	}

	var conf configs.SyncConfig
	if err := tools.UnmarshalYamlAndBuildDefault(content, &conf); err != nil {
		log.Fatalln(err)
	}

	dataSource := datasources.GetDataSourceConstructor(conf.DataSource.Type)()
	dataSource.SetConfigs(map[string]datasources.DataSourceConfig{
		conf.DataSource.Name: conf.DataSource,
	})

	redisCli := redis.NewClient(&redis.Options{
		Addr: conf.RedisConfig.Addr, DB: conf.RedisConfig.DB,
		Password: conf.RedisConfig.Password,
	})

	config := zap.NewProductionEncoderConfig()
	zapCore := zapcore.NewCore(logs.NewZapJsonEncoder(config), zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	logger := zap.New(zapCore)
	zap.ReplaceGlobals(logger)

	engine, err := core.NewCore(&conf, redisCli)
	if err != nil {
		log.Fatalln(err)
	}
	writerConstructor := writers.GetWriterConstructor(conf.DataSource.Type)
	engine.SetWriter(conf.DataSource.Name, writerConstructor(dataSource))

	if err := engine.Run(); err != nil {
		log.Fatalln(err)
	}
}
