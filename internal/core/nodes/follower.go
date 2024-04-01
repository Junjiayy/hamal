package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Junjiayy/hamal/internal/core/handlers"
	"github.com/Junjiayy/hamal/internal/core/runners"
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/Junjiayy/hamal/pkg/tools"
	"github.com/Junjiayy/hamal/pkg/tools/logs"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"strconv"
	"sync"
	"time"
)

type Follower struct {
	node
	rules           map[string][]*types.SyncRule
	ruleRwMux       *sync.RWMutex
	rs              map[string]readers.Reader
	rsMux           *sync.Mutex
	runner          *runners.Runner
	runnerCloseChan chan struct{}
	h               *handlers.Handler
	wg              *sync.WaitGroup
	l               *leader
}

const rulesPath = "/porter/rules"            // 任务监听目录
const leaderPath = "/porter/leader"          // 主节点监听目录
const followerRootPath = "/porter/followers" // 任务节点根目录
const writerConfigPath = "/porter/writers"   // 写入器配置监听目录
const eventLockPath = "/porter/event-lock"   // 事件锁目录，主要防止 follower 和 leader 节点初始化时数据不正确

func NewFollowerNode(parent context.Context, redisCli *redis.Client, zkConn *zk.Conn, pSize int) (*Follower, error) {
	h, err := handlers.NewHandler(redisCli, pSize)
	if err != nil {
		return nil, err
	}
	ctx, cancelFunc := context.WithCancel(parent)
	runnerCloseChan := make(chan struct{}, 1)

	return &Follower{
		rules:           make(map[string][]*types.SyncRule),
		ruleRwMux:       new(sync.RWMutex),
		rs:              make(map[string]readers.Reader),
		rsMux:           new(sync.Mutex),
		runner:          runners.NewRunner(parent, runnerCloseChan),
		runnerCloseChan: runnerCloseChan,
		h:               h,
		wg:              new(sync.WaitGroup),
		node: node{
			ctx: ctx, zkConn: zkConn, cancelFunc: cancelFunc,
		},
	}, nil
}

func (f *Follower) Run() error {
	// 注册 follower 节点时先加锁，防止 leader 节点获取 followers 不全
	lock := zk.NewLock(f.zkConn, eventLockPath, zk.WorldACL(zk.PermAll))
	if err := lock.Lock(); err != nil {
		return errors.WithStack(err)
	}

	path := followerRootPath + "/follower-"
	tempChildPath, err := f.zkConn.Create(path, nil, zk.FlagEphemeral|zk.FlagSequence,
		zk.WorldACL(zk.PermAll))
	// 不管 followers 创建节点是否成功，都先释放锁，防止死锁
	if err := lock.Unlock(); err != nil {
		return errors.WithStack(err)
	}
	if err != nil {
		return err
	}

	// 开始监听数据源配置目录变更时间
	f.runner.RunWorker(f.watchNodeDataChange(writerConfigPath, f.dbConfigsChanged))
	// 开始监听任务目录变更事件
	f.runner.RunWorker(f.watchNodeDataChange(tempChildPath, f.readerConfigsChanged))
	// 开始监听同步规则目录数据变更事件
	f.runner.RunWorker(f.watchNodeDataChange(rulesPath, f.rulesChanged))
	// 抢占 leader 节点目录，抢占失败则开启监听主节点删除事件
	// Notice 所有节点都会运行 Follower 任务
	// 所以 leader 节点的 Follower 也会监听主节点删除事件
	f.runner.RunWorker(f.watchLeaderNodeDeleted)

	// 阻塞: 等待 runnerCloseChan 通道读取事件
	select {
	case <-f.runnerCloseChan:
		// 接受到 runner 关闭通道数据，说明 runner 已经被关闭
		return f.stopOnceFunc()
	}
}

// watchLeaderNodeDeleted 监听 leader 节点删除事件
func (f *Follower) watchLeaderNodeDeleted(ctx context.Context) {
	exists, _, events, err := f.zkConn.ExistsW(leaderPath)
	if err != nil {
		panic(fmt.Sprintf("watch manager deleted failed: %v", err))
	}
	if !exists {
		err = f.preemptLeaderNode()
		if err == nil {
			return
		} else if !errors.Is(err, zk.ErrNodeExists) {
			panic(fmt.Sprintf("preempt leader failed: %v", err))
		}
	}

	for {
		select {
		case event := <-events:
			// 监听到 leader 节点删除事件后，直接抢占 Leader 节点
			// 抢占节点成功，本节点同时开启 leader 节点任务，重新分配所有任务
			if event.Type == zk.EventNodeDeleted {
				if err := f.preemptLeaderNode(); err != nil {
					if !errors.Is(err, zk.ErrNodeExists) {
						panic(fmt.Sprintf("preempt leader failed: %v", err))
					}
				} else {
					return
				}
			}
		case <-ctx.Done():
			return
		}

		_, _, events, err = f.zkConn.ExistsW(leaderPath)
		if err != nil {
			panic(fmt.Sprintf("watch manager deleted failed: %v", err))
		}
	}
}

// preemptLeaderNode 争抢 leader 节点，
// 争抢到后执行 leader 逻辑，并阻塞当前 goroutine，等待 leader 节点被释放
func (f *Follower) preemptLeaderNode() error {
	leaderNodeVal := tools.Hash32(strconv.Itoa(time.Now().Nanosecond()))
	if _, err := f.zkConn.Create(leaderPath, []byte(leaderNodeVal), zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll)); err != nil {
		return err
	}

	f.l = newLeaderNode(f.ctx, f.zkConn, leaderNodeVal)
	if err := f.l.run(); err != nil {
		return err
	}

	return nil
}

// readerConfigsChanged reader config (任务) 数据变更事件处理方法
func (f *Follower) readerConfigsChanged(data []byte) error {
	f.rsMux.Lock()
	defer f.rsMux.Unlock()

	var configs map[string]readers.ReaderConfigByType
	if err := json.Unmarshal(data, &configs); err != nil {
		return err
	}

	for uniqueId, config := range configs {
		if reader, ok := f.rs[uniqueId]; ok {
			// 如果更新前的配置信息和更新后的配置信息不一致，则关闭老的 reader
			// notice: 一般不太会出现这个情况，reader config 的唯一id都是通过重要的敏感信息hash来的
			if !reader.GetConfig().Equal(config.Config) {
				zap.L().Info("reader replace close old reader", zap.String("id", uniqueId))
				if err := reader.Close(); err != nil {
					logs.Error("close reader failed", err)
				}
			} else {
				// 如果 reader config 没有被修改，直接提过本次循环
				continue
			}
		}

		// 开启 reader 监听
		zap.L().Info("reader start listen", zap.Reflect("config", config.Config))
		readerConstructor := readers.GetReaderConstructor(config.Type)
		reader, err := readerConstructor(config.Config, f.wg, f.ctx)
		f.rs[uniqueId] = reader
		if err != nil {
			logs.Error("reader initialize failed", err)
		}

		f.runner.RunWorker(f.listen(reader))
	}

	for uniqueId, reader := range f.rs {
		// 检查所有正在执行的 reader，如果不在本次更新中就关闭 reader
		if _, ok := configs[uniqueId]; !ok {
			if err := reader.Close(); err != nil {
				logs.Error("close reader failed", err)
			}
		}
	}

	return nil
}

// rulesChanged 同步规则 数据变更处理规则
func (f *Follower) rulesChanged(data []byte) error {
	var rules map[string][]*types.SyncRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}

	f.ruleRwMux.Lock()
	defer f.ruleRwMux.Unlock()
	f.rules = rules

	return nil
}

// dbConfigsChanged 数据源 数据变更处理方法
func (f *Follower) dbConfigsChanged(data []byte) error {
	var dbConfigsByType map[string]map[string]datasources.DataSourceConfig
	if err := json.Unmarshal(data, &dbConfigsByType); err != nil {
		return err
	}

	return f.h.GetWriterPool().SetConfigs(dbConfigsByType)
}

// listen 开始监听 reader, 此方法被 runners.Runner 调用
func (f *Follower) listen(reader readers.Reader) func(ctx context.Context) {
	return func(ctx context.Context) {
		for {
			select {
			case <-reader.GetCtx().Done():
				// 每个 reader 都有自己独立都 context 当关闭 reader 时，context 需要一起关闭
				return
			case <-ctx.Done():
				// Runner 被关闭
				return
			default:
				bingLogParams, err := reader.Read()
				if err == io.EOF || err == io.ErrClosedPipe {
					zap.L().Info("reader closed", zap.String("unique", reader.GetConfig().GetUniqueId()))
					return
				} else if err != nil {
					// todo: 考虑短时间内失败多次是否需要抛弃阅读器
					zap.L().Error("listen reader failed", zap.String("unique",
						reader.GetConfig().GetUniqueId()), zap.Error(err))
					continue
				}

				if !bingLogParams.IsDdl {
					err = f.submitToPoolExec(bingLogParams)
				}

				// 不管是否 ddl 修改，都需要提交 reader 成功
				if err == nil {
					if err := reader.Complete(bingLogParams); err != nil {
						zap.L().Error("commit message failed", zap.Error(err))
					}
				}
			}
		}
	}
}

// submitToPoolExec 提交任务到携程池执行
func (f *Follower) submitToPoolExec(binLogParams *types.BinlogParams) error {
	swg, ruleKey := types.NewSyncWaitGroup(), binLogParams.Database+"_"+binLogParams.Table
	defer swg.Recycle()

	f.ruleRwMux.RLock()
	rules, ok := f.rules[ruleKey]
	f.ruleRwMux.RUnlock()

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
			if err := f.h.Invoke(params); err != nil {
				logs.Error("sync failed", err)
			}
		}
	}

	swg.Wait()
	if errArr := swg.Errors(); len(errArr) == 0 {
		return errArr[0]
	}

	return nil
}

// stopOnceFunc 停止方法，只能调用一次，多次调用会 panic
// 当当前方法被调用时，runners.Runner.Stop 方法肯定已经被调用
// 所有 goroutine 都已经被停止
// 所有不用考虑有任务未完成的问题
func (f *Follower) stopOnceFunc() error {
	// runners.Runner 可能会主动停止
	// 如果是主动停止 Follower.Stop 方法不会被主动调用，
	// 不主动调用 Follower.cancelFunc 也不会被调用，
	// 但是 Follower.cancelFunc 调用不会 panic
	// 所以在这再调用一次
	f.cancelFunc()
	close(f.runnerCloseChan)
	// 释放处理器
	f.h.Release()
	// 关闭所有读取器
	var lastErr error
	for _, reader := range f.rs {
		if err := reader.Close(); err != nil {
			logs.Error("close reader failed", errors.WithStack(err))
			lastErr = err
		}
	}

	// 如果 leader 节点不为 nil 关闭 leader 节点
	if f.l != nil {
		f.l.stop()
	}

	return lastErr
}

func (f *Follower) Stop() {
	// 因为 runner 的所有任务协程都监听了 ctx
	// 这个 ctx 是所有 goroutine 的父级
	// 当 f.ctx 被取消时，leader 的 ctx 和 leader.Runner 的 ctx 都会被取消
	// 所以当 cancelFunc 被调用后，所有的 goroutine 都会被停止
	f.cancelFunc()
	f.runner.Stop()
}
