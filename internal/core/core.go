package core

import (
	"context"
	"github.com/Junjiayy/hamal/internal/pkg/configs"
	"github.com/Junjiayy/hamal/internal/pkg/tools/logs"
	"github.com/Junjiayy/hamal/internal/pkg/types"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"io"
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

func (c *Core) Run() (err error) {
	defer func() {
		if err != nil {
			if err := c.Stop(); err != nil {
				zap.L().Error("关闭同步失败", zap.Error(err))
			}
		}
	}()

	readerConstructor := types.GetReaderConstructor(c.conf.Reader)

	for i := 0; i < c.conf.ReaderConcurrencyNum; i++ {
		reader, err := readerConstructor(i, c.conf.ReaderConfig)
		if err != nil {
			return err
		}
		c.wg.Add(1)
		go c.listenByReader(reader)
	}

	c.wg.Wait()

	return nil
}

func (c *Core) listenByReader(reader types.Reader) {
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
