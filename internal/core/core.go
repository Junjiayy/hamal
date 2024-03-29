package core

import (
	"context"
	"github.com/Junjiayy/hamal/internal/core/nodes"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/Junjiayy/hamal/pkg/core/writers"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type Core struct {
	conf *configs.SyncConfig
	//h          *handlers.handler
	wg         *sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	redisCli   *redis.Client
	follower   *nodes.Follower
	//runner     *runners.runner
}

func NewCore(conf *configs.SyncConfig, redisCli *redis.Client) (*Core, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	conn, _, err := zk.Connect([]string{"10.150.106.133:2181"}, time.Second*5)
	if err != nil {
		defer cancelFunc()
		return nil, err
	}

	c := &Core{
		conf: conf, redisCli: redisCli, ctx: ctx,
		wg: new(sync.WaitGroup), cancelFunc: cancelFunc,
		follower: nodes.NewFollowerNode(ctx, conn),
		//h: &handlers.handler{
		//	ws:     make(map[string]writers.Writer),
		//	rs:     redsync.New(goredis.NewPool(redisCli)),
		//	filter: new(handlers.emptyFilter),
		//},
	}

	//pool, err := ants.NewPoolWithFunc(conf.PoolSize, c.h.sync, ants.WithNonblocking(true))
	//if err != nil {
	//	log.Fatalf("create sync task pool failure: %v", err)
	//}
	//
	//c.h.pool = pool

	return c, nil
}

func (c *Core) SetFilter(filter types.Filter) *Core {
	//c.h.filter = filter
	return c
}

func (c *Core) SetWriter(name string, writer writers.Writer) *Core {
	//c.h.ws[name] = writer
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

		c.wg.Add(1)
		go c.listenByReader(reader)
	}

	c.wg.Wait()

	return nil
}

func (c *Core) listenByReader(reader readers.Reader) {
	defer c.wg.Done()

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
				err = c.submitTaskExec(binLogParams)
			}

			if err == nil {
				if err := reader.Complete(binLogParams); err != nil {
					zap.L().Error("commit message failed", zap.Error(err))
				}
			}
		}
	}
}

func (c *Core) submitTaskExec(binLogParams *types.BinlogParams) error {
	//swg, ruleKey := types.NewSyncWaitGroup(), binLogParams.Database+"_"+binLogParams.Table
	//defer swg.Recycle()

	//rules, ok := c.conf.Rules[ruleKey]
	//if !ok {
	//	zap.L().Info("rule not exists", zap.String("key", ruleKey))
	//}

	//for _, rule := range rules {
	//	for i, datum := range binLogParams.Data {
	//		var old map[string]string
	//		if len(binLogParams.Old) > i {
	//			old = binLogParams.Old[i]
	//		}
	//
	//		//params := types.NewSyncParams(swg, rule, datum, old, binLogParams)
	//		//if err := c.h.invoke(params); err != nil {
	//		//	zap.L().Error("sync failed", logs.ParseErr(err)...)
	//		//}
	//	}
	//}

	//swg.Wait()
	//if errArr := swg.Errors(); len(errArr) == 0 {
	//	return errArr[0]
	//}

	return nil
}

func (c *Core) Stop() error {
	c.cancelFunc()
	//c.h.pool.Release()
	return c.redisCli.Close()
}
