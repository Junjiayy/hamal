package nodes

import (
	"context"
	"fmt"
	"github.com/Junjiayy/hamal/pkg/tools/logs"
	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
	"reflect"
	"runtime"
)

type node struct {
	ctx    context.Context
	zkConn *zk.Conn
}

// getNodeDataOrCreate 获取 zookeeper 节点数据，如果节点不存在则创建节点
func (n *node) getNodeDataOrCreate(path string, flags int32) ([]byte, error) {
	data, _, err := n.zkConn.Get(path)
	if dataNotExists := errors.Is(err, zk.ErrNoNode); dataNotExists || err == nil {
		if dataNotExists {
			if _, err = n.zkConn.Create(path, nil, flags, zk.WorldACL(zk.PermAll)); err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					data, _, err = n.zkConn.Get(path)
				}
			}
		}
	}

	return data, err
}

// watchNodeDataChange 监听 zookeeper 节点数据比变更
func (n *node) watchNodeDataChange(path string, fn func([]byte) error) func(ctx context.Context) {
	return func(ctx context.Context) {
		data, _, events, err := n.zkConn.GetW(path)
		if err != nil {
			// 出现错误，不主动结束整个进程，等待再次被压入队列
			panic(fmt.Sprintf("watch %s data failed: %v", path, err))
		}
		// events 是缓冲通道，所以不用立马开始监听 events，可以先初始化数据
		if data != nil {
			if err = fn(data); err != nil {
				funcName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
				panic(fmt.Sprintf("init %s func falied: %v", funcName, err))
			}
		}

		for {
			select {
			case event := <-events:
				// 事件是 channel 方式通知的，所以事件一定是顺序的，不用考虑事件更新的并发
				data, _, err = n.zkConn.Get(event.Path)
				if err != nil {
					panic(fmt.Sprintf("get %s data failed: %v", path, err))
				}
				// notice: events 是长度为1的缓冲通道，当监听到数据后就会关闭
				// 所以监听到数据后需要立即重置 events
				// 数据变更拉起新到协程执行，节点变更不平凡，不需要进行池化
				if data != nil {
					go n.exec(data, fn)
				}
			case <-ctx.Done():
				return
			}

			_, _, events, err = n.zkConn.GetW(path)
			if err != nil {
				panic(fmt.Sprintf("watch %s data failed: %v", path, err))
			}
		}
	}
}

func (n *node) exec(data []byte, fn func([]byte) error) {
	if err := fn(data); err != nil {
		logs.Error("run data changed func failed", errors.WithStack(err))
	}
}
