package readers

import (
	"context"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type (
	HttpReader struct {
		conf   *HttpReaderConfig
		srv    *http.Server
		wg     *sync.WaitGroup
		ctx    context.Context
		params chan *types.BinlogParams
	}

	HttpReaderConfig struct {
		Listen       string        `json:"listen" yaml:"listen"`                                                  // 监听端口
		PushPath     string        `json:"push_path" yaml:"push_path"`                                            // 请求接收路径
		PreParamsLen int32         `json:"pre_params_len,omitempty" yaml:"pre_params_len,omitempty" default:"10"` // params 管道缓冲长度
		PushTimeout  time.Duration `json:"push_timeout,omitempty" yaml:"push_timeout,omitempty" default:"1s"`     // 超时时间
	}
)

var configAssertErr = errors.New("reader config assert failed")

func NewHttpReaderFunc(conf interface{}, wg *sync.WaitGroup, ctx context.Context) (Reader, error) {
	config, ok := conf.(*HttpReaderConfig)
	if !ok {
		return nil, configAssertErr
	}
	reader := &HttpReader{
		params: make(chan *types.BinlogParams, config.PreParamsLen),
		wg:     wg, ctx: ctx, conf: config,
	}

	engine := gin.Default()
	engine.POST(reader.conf.PushPath, reader.httpAcceptHandle)
	reader.srv = &http.Server{Addr: reader.conf.Listen, Handler: engine}
	wg.Add(1)
	go reader.listen()

	return reader, nil
}

func NewHttpReaderConfigFunc() interface{} {
	return &HttpReaderConfig{}
}

func (h *HttpReader) httpAcceptHandle(ctx *gin.Context) {
	binLogParams := new(types.BinlogParams)
	if err := ctx.ShouldBindJSON(binLogParams); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code": http.StatusBadRequest, "message": err.Error(),
		})
		return
	}

	timeout, cancelFunc := context.WithTimeout(ctx.Request.Context(), h.conf.PushTimeout)
	defer cancelFunc()

	select {
	case <-h.ctx.Done():
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code": http.StatusInternalServerError, "message": "service closed",
		})
	case h.params <- binLogParams:
		ctx.JSON(http.StatusAccepted, gin.H{
			"code": http.StatusAccepted,
		})
	case <-timeout.Done():
		ctx.JSON(http.StatusRequestTimeout, gin.H{
			"code": http.StatusAccepted, "message": "timeout",
		})
	}
}

func (h *HttpReader) listen() {
	defer h.wg.Done()
	if err := h.srv.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		log.Fatalln(err)
	}
}

func (h *HttpReader) Read() (*types.BinlogParams, error) {
	binLogParams, ok := <-h.params
	if !ok {
		return nil, io.ErrClosedPipe
	}

	return binLogParams, nil
}

func (h *HttpReader) Complete(params *types.BinlogParams) error {
	return nil
}

func (h *HttpReader) Close() error {
	if h.srv != nil {
		timeout, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelFunc()

		return h.srv.Shutdown(timeout)
	}

	close(h.params)

	return nil
}
