package core

import (
	"context"
	"github.com/Junjiayy/hamal/internal/core/nodes"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
	"time"
)

type Core struct {
	conf       *configs.SyncConfig
	ctx        context.Context
	cancelFunc context.CancelFunc
	f          *nodes.Follower
}

func NewCore(conf *configs.SyncConfig) (*Core, error) {
	zkConn, _, err := zk.Connect(conf.ZookeeperConfig.Hosts, time.Second*5)
	if err != nil {
		return nil, err
	}
	redisCli := redis.NewClient(&redis.Options{
		Addr: conf.RedisConfig.Addr, DB: conf.RedisConfig.DB,
		Password: conf.RedisConfig.Password,
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	f, err := nodes.NewFollowerNode(ctx, redisCli, zkConn, conf.PoolSize)
	if err != nil {
		defer cancelFunc()
		return nil, err
	}

	c := &Core{
		conf: conf, ctx: ctx, cancelFunc: cancelFunc, f: f,
	}

	return c, nil
}

func (c *Core) Run() error {
	// Follower.Run 会被阻塞
	return c.f.Run()
}

func (c *Core) Stop() {
	c.cancelFunc()
	c.f.Stop()
}
