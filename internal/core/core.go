package core

import (
	"context"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/Junjiayy/hamal/pkg/tools/logs"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"io"
	"log"
	"sync"
)

type Core struct {
	conf       *configs.SyncConfig
	h          *handler
	wg         *sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	redisCli   *redis.Client
}

func NewCore(conf *configs.SyncConfig, redisCli *redis.Client) *Core {
	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Core{
		conf: conf, redisCli: redisCli, ctx: ctx,
		wg: new(sync.WaitGroup), cancelFunc: cancelFunc,
		h: &handler{
			ws: make(map[string]types.Writer),
			rs: redsync.New(goredis.NewPool(redisCli)),
		},
	}

	pool, err := ants.NewPoolWithFunc(conf.PoolSize, c.h.sync, ants.WithNonblocking(true))
	if err != nil {
		log.Fatalf("create sync task pool failure: %v", err)
	}

	c.h.pool = pool

	return c
}

func (c *Core) SetFilter(filter types.Filter) *Core {
	c.h.filter = filter
	return c
}

func (c *Core) SetWriter(name string, writer types.Writer) *Core {
	c.h.ws[name] = writer
	return c
}

func (c *Core) Run() (err error) {
	defer func() {
		if err != nil {
			if err := c.Stop(); err != nil {
				zap.L().Error("关闭同步失败", zap.Error(err))
			}
		}
	}()

	for _, config := range c.conf.Readers {
		constructor := readers.GetReaderConstructor(config.Name)
		reader, err := constructor(config.Params, c.wg, c.ctx)
		if err != nil {
			return err
		}

		go c.listenByReader(reader)
	}

	c.wg.Wait()

	return nil
}

func (c *Core) listenByReader(reader types.Reader) {
	defer reader.Close()

	for {
		select {
		case <-c.ctx.Done():
			if err := reader.Close(); err != nil {
				zap.L().Error("close reader failed", zap.Error(err))
			}
			return
		default:
			binLogParams, err := reader.Read()
			if err == io.EOF || err == io.ErrClosedPipe {
				zap.L().Info("reader closed")
				return
			}

			if !binLogParams.IsDdl {
				swg, ruleKey := types.NewSyncWaitGroup(), binLogParams.Database+"-"+binLogParams.Table
				rules, ok := c.conf.Rules[ruleKey]
				if !ok {
					zap.L().Info("rule not exists", zap.String("key", ruleKey))
				}

				for _, rule := range rules {
					for i, datum := range binLogParams.Data {
						var old map[string]string
						if len(binLogParams.Old) > i {
							old = binLogParams.Old[i]
						}

						params := types.NewSyncParams(swg, rule, datum, old, binLogParams)
						if err := c.h.invoke(params); err != nil {
							zap.L().Error("sync failed", logs.ParseErr(err)...)
						}
					}
				}

				swg.Wait()
				if errArr := swg.Errors(); len(errArr) == 0 {
					if err := reader.Complete(binLogParams); err != nil {
						zap.L().Error("commit message failed", zap.Error(err))
					}
				}
			}
		}
	}
}

func (c *Core) Stop() error {
	c.cancelFunc()
	c.h.pool.Release()
	return c.redisCli.Close()
}
