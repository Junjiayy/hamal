package core

import (
	"context"
	"github.com/Junjiayy/hamal/internal/pkg/configs"
	"github.com/go-redis/redis/v8"
	"sync"
)

type Core struct {
	conf       *configs.SyncConfig
	h          *handler
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	redisCli   *redis.Client
}
