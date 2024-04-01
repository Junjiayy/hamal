package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Junjiayy/hamal/internal/core/runners"
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
	"math"
	"sync"
)

type (
	task struct {
		Readers map[string]readers.ReaderConfigByType `json:"readers"`
	}

	leader struct {
		node
		runner               *runners.Runner
		tasks                map[string]task
		followerPaths        map[string]struct{}
		taskSharingFollowers map[string]string
		rwMux                *sync.RWMutex
		runnerCloseChan      chan struct{}
		val                  string
	}
)

const readersPath = "/porter/readers" // 所有任务节点

func newLeaderNode(parent context.Context, zkConn *zk.Conn, val string) *leader {
	runnerCloseChan := make(chan struct{}, 1)
	ctx, cancelFunc := context.WithCancel(parent)

	return &leader{
		runner:               runners.NewRunner(ctx, runnerCloseChan),
		taskSharingFollowers: make(map[string]string),
		rwMux:                new(sync.RWMutex),
		val:                  val,
		node: node{
			ctx: ctx, zkConn: zkConn, cancelFunc: cancelFunc,
		},
	}
}

func (l *leader) run() error {
	// 先加锁，防止 leader 节点获取 followers 不全
	lock := zk.NewLock(l.zkConn, eventLockPath, zk.WorldACL(zk.PermAll))
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	followerData, _, err := l.zkConn.Get(followerRootPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(followerData, &l.tasks); err != nil {
		return err
	}
	// 开始监听 followers 目录结构变更
	l.runner.RunWorker(l.watchFollowersChanged)
	l.runner.RunWorker(l.watchNodeDataChange(readersPath, l.readersChanged))

	// 阻塞: 等待 runnerCloseChan 通道读取事件
	select {
	case <-l.runnerCloseChan:
		// 接受到 runner 关闭通道数据，说明 runner 已经被关闭
		return l.stopOnceFunc()
	}
}

// watchFollowersChanged 监听跟随者节点删除或新增
func (l *leader) watchFollowersChanged(ctx context.Context) {
	children, _, events, err := l.zkConn.ChildrenW(followerRootPath)
	if err != nil {
		panic(fmt.Sprintf("watch followers failed: %v", err))
	}
	l.updateFollowerPaths(children)

	for {
		select {
		case event := <-events:
			if event.Type == zk.EventNodeChildrenChanged {
				updatedFollowerPaths, _, err := l.zkConn.Children(followerRootPath)
				if err != nil {
					panic(fmt.Sprintf("get updated follower paths failed: %v", err))
				}
				err = l.updateFollowerPaths(updatedFollowerPaths).broadcast()
				if err != nil {
					panic(fmt.Sprintf("leader broadcast failed: %v", err))
				}
			}
		case <-ctx.Done():
			return
		}

		_, _, events, err = l.zkConn.ChildrenW(followerRootPath)
		if err != nil {
			panic(fmt.Sprintf("watch followers failed: %v", err))
		}
	}
}

// updateFollowerPaths follower 更新事件处理方法
func (l *leader) updateFollowerPaths(followerPaths []string) *leader {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()

	followerPathMap := make(map[string]struct{}, len(followerPaths))
	for _, childPath := range followerPaths {
		followerPathMap[childPath] = struct{}{}
		if _, ok := l.tasks[childPath]; !ok {
			l.tasks[childPath] = task{
				Readers: make(map[string]readers.ReaderConfigByType),
			}
		}
	}

	if l.followerPaths != nil {
		for existsFollowerPath := range l.followerPaths {
			// 如果有已删除的 follower 节点
			// 需要移除对应的任务记录
			if _, ok := followerPathMap[existsFollowerPath]; !ok {
				if t, ok := l.tasks[existsFollowerPath]; ok {
					delete(l.tasks, existsFollowerPath)

					for uniqueId := range t.Readers {
						delete(l.taskSharingFollowers, uniqueId)
					}
				}
			}
		}
	}

	l.followerPaths = followerPathMap

	return l
}

// readersChanged 读取器配置更新处理方法
func (l *leader) readersChanged(data []byte) error {
	var configs []readers.ReaderConfigByType
	if err := json.Unmarshal(data, &configs); err != nil {
		return err
	}

	configByUniqueId := make(map[string]readers.ReaderConfigByType, len(configs))
	for _, config := range configs {
		configByUniqueId[config.Config.GetUniqueId()] = config
	}

	return l.updateReaderConfigs(configByUniqueId).
		broadcast()
}

// updateReaderConfigs 更新读取器配置
func (l *leader) updateReaderConfigs(configs map[string]readers.ReaderConfigByType) *leader {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()

	// 先删除已经移除的 reader，方便比较平均的分配任务
	for uniqueId, followerPath := range l.taskSharingFollowers {
		if _, ok := configs[uniqueId]; !ok {
			delete(l.tasks[followerPath].Readers, uniqueId)
			delete(l.taskSharingFollowers, uniqueId)
		}
	}

	for uniqueId, config := range configs {
		// 先判断当前配置是否已经分配过了
		// 如果分配过了，并且上次分配的 follower 节点还存在
		// 直接把当前配置赋值给 原 follower 节点
		if sharedFollowerPath, ok := l.taskSharingFollowers[uniqueId]; ok {
			if _, ok = l.followerPaths[sharedFollowerPath]; ok {
				l.tasks[sharedFollowerPath].Readers[uniqueId] = config
				continue
			}
		}

		minTaskPath := l.getMinTaskNumFollowerPath()
		l.tasks[minTaskPath].Readers[uniqueId] = config
	}

	return l
}

// broadcast 向所有 follower 节点发送广播
func (l *leader) broadcast() error {
	l.rwMux.RLock()
	defer l.rwMux.RUnlock()

	followerData, err := json.Marshal(&l.tasks)
	if err != nil {
		return err
	}
	if _, err = l.zkConn.Set(followerRootPath, followerData, -1); err != nil {
		return err
	}
	for followerPath, followerReaderConfigs := range l.tasks {
		followerReaderConfigData, err := json.Marshal(followerReaderConfigs)
		// 任何一个 follower 节点更新失败，直接放弃后续节点更新
		// 因为更新失败只可能是某个节点被删除，只需要等待 目录更新事件 二次更新就可以
		if err != nil {
			return err
		}
		_, err = l.zkConn.Set(followerPath, followerReaderConfigData, -1)
		if err != nil {
			return err
		}
	}

	return nil
}

// getMinTaskNumFollowerPath 获取任务数量最少的 follower 节点路径
func (l *leader) getMinTaskNumFollowerPath() string {
	minFollowerPath, minTaskNum := "", math.MaxInt

	for followerPath, t := range l.tasks {
		if length := len(t.Readers); length < minTaskNum {
			minFollowerPath, minTaskNum = followerPath, length
		}
	}

	return minFollowerPath
}

// stopOnceFunc 停止方法，只能调用一次，多次调用会 panic
func (l *leader) stopOnceFunc() error {
	// runners.Runner 可能会主动停止
	// 如果是主动停止 leader.Stop 方法不会被主动调用，
	// 不主动调用 leader.cancelFunc 也不会被调用，
	// 但是 leader.cancelFunc 调用不会 panic
	// 所以在这再调用一次
	l.cancelFunc()
	close(l.runnerCloseChan)
	data, _, err := l.zkConn.Get(leaderPath)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return nil
		} else {
			return err
		}
	}

	if string(data) == l.val {
		// 如果主节点和 l.val 相等，说明当前 leader 节点是由当前进程创建，删除 leader 节点
		if err := l.zkConn.Delete(leaderPath, -1); err != nil {
			return err
		}
	}

	return nil
}

// stop 关闭 leader 任务逻辑, 只关闭 ctx 和 runner
// 其他任务的关闭操作详见 run 方法
// 当调用 runners.Runner 的 Stop 方法时，会触发 closeRunnerChan 通道写入事件
// run 方法监听到 closeRunnerChan 事件后会关闭其他关闭项
func (l *leader) stop() {
	// 因为 runner 的所有任务协程都监听了 ctx
	// 所以当 cancelFunc 被调用后，所有协程都会被停止，runner 等待被回收
	l.cancelFunc()
	l.runner.Stop()
}
