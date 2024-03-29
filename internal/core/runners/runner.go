package runners

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Runner struct {
	wg                  *sync.WaitGroup
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	timePeriodFailedNum int32
	downChan            chan<- struct{}
	panicChan           chan struct{}
}

const failedNumCheckIntervalSeconds = 300
const maxFailedNum = 10

func NewRunner(parent context.Context, downChan chan<- struct{}) *Runner {
	ctx, cancelFunc := context.WithCancel(parent)

	r := &Runner{
		wg: new(sync.WaitGroup), ctx: ctx,
		cancelFunc: cancelFunc, downChan: downChan,
		panicChan: make(chan struct{}, maxFailedNum),
	}

	r.wg.Add(1)
	go r.listenPeriodFailedNum()

	return r
}

// RunWorker 单独拉起携程执行某个函数，并拦截 panic 并在一秒之后重新启动
// 这个执行函数，一般都是阻塞的
func (r *Runner) RunWorker(fns ...func(ctx context.Context)) {
	for _, fn := range fns {
		r.wg.Add(1)
		go r.runWorkerFunc(fn)
	}
}

func (r *Runner) runWorkerFunc(fn func(ctx context.Context)) {
	defer r.runWorkerRecover(fn)
	fn(r.ctx)
}

// runWorkerRecover runWorker运行时 panic 处理方法
func (r *Runner) runWorkerRecover(fn func(ctx context.Context)) {
	defer r.wg.Done()

	if err := recover(); err != nil {
		funcName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		zap.L().Error("core run worker recover:", zap.String("func", funcName),
			zap.Error(errors.WithStack(errors.Errorf("%v", err))))

		// 拦截 panic 后重新压入到 携程中
		select {
		case r.panicChan <- struct{}{}:
			// Stop 方法被调用时，channel 可能会被阻塞
			// 不过这个时候已经在执行关闭流程，所以任务可以直接放弃
			r.RunWorker(fn)
		default:
			return
		}
	}
}

func (r *Runner) listenPeriodFailedNum() {
	defer r.wg.Done()
	ticker := time.NewTicker(failedNumCheckIntervalSeconds * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定时检查时间段内 runner panic 是否超过最大限制
			if atomic.LoadInt32(&r.timePeriodFailedNum) >= maxFailedNum {
				// 超过最大限制， 关闭 runner
				r.Stop()
				return
			} else {
				// 时间段内未超过最大限制，重置错误次数
				atomic.StoreInt32(&r.timePeriodFailedNum, 0)
			}
		case <-r.panicChan:
			// 防止未到达检查时间，短时间频繁出现错误
			updated := atomic.AddInt32(&r.timePeriodFailedNum, 1)
			if updated >= maxFailedNum {
				r.Stop()
				return
			}

		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) Stop() {
	r.cancelFunc()
	r.wg.Wait()
	close(r.panicChan)
	r.downChan <- struct{}{}
}
