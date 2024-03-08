package core

import (
	icore "github.com/Junjiayy/hamal/internal/core"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/go-redis/redis/v8"
)

type Core struct {
	*icore.Core
}

func NewCore(conf *configs.SyncConfig, redisCli *redis.Client) *Core {
	return &Core{
		Core: icore.NewCore(conf, redisCli),
	}
}
