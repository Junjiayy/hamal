package readers

import (
	"context"
	"encoding/json"
	"github.com/Junjiayy/hamal/pkg/tools"
	"github.com/Junjiayy/hamal/pkg/types"
	"sync"
	"sync/atomic"
)

type (
	Reader interface {
		Read() (*types.BinlogParams, error)
		Complete(params *types.BinlogParams) error
		GetConfig() ReaderConfig
		GetCtx() context.Context
		Close() error
	}

	ReaderConfig interface {
		GetUniqueId() string
		Equal(ReaderConfig) bool
	}

	// ReaderConfigByType 读取器配置携带type参数
	// json 反序列化时通过type获取具体的类型，再进行实例化
	ReaderConfigByType struct {
		Type   string       `json:"type"`
		Config ReaderConfig `json:"-"`
	}

	innerReaderConfigByType struct {
		Type   string      `json:"type"`
		Config interface{} `json:"config"`
	}

	ReaderConstructor func(ReaderConfig, *sync.WaitGroup, context.Context) (Reader, error)
)

var (
	_readerConstructors       = make(map[string]ReaderConstructor)  // 读取器构造函数 映射表
	_readerConfigConstructors = make(map[string]func() interface{}) // 读取器配置类型构造函数 映射表
)

func init() {
	SetReaderConstructor(types.ReaderTypeWeb, NewHttpReaderFunc)
	SetReaderConfigConstructor(types.ReaderTypeWeb, NewHttpReaderConfigFunc)
	SetReaderConstructor(types.ReaderTypeKafka, NewKafkaReaderFunc)
	SetReaderConfigConstructor(types.ReaderTypeKafka, NewKafkaReaderConfigFunc)
}

// UnmarshalJSON 根据type 获取到具体到 config 结构体，并重新序列化赋值
func (r *ReaderConfigByType) UnmarshalJSON(bytes []byte) error {
	var innerConfig innerReaderConfigByType
	if err := json.Unmarshal(bytes, &innerConfig); err != nil {
		return err
	}
	configConstructor := GetReaderConfigConstructor(innerConfig.Type)
	config := configConstructor()
	configBytes, err := json.Marshal(innerConfig.Config)
	if err != nil {
		return err
	}
	if err := tools.UnmarshalJsonAndBuildDefault(configBytes, config); err != nil {
		return err
	}
	r.Type, r.Config = innerConfig.Type, config.(ReaderConfig)

	return nil
}

// SetReaderConstructor 注册读取器构造函数
func SetReaderConstructor(name string, fn ReaderConstructor) {
	_readerConstructors[name] = fn
}

// GetReaderConstructor 获取读取器构造函数
func GetReaderConstructor(name string) ReaderConstructor {
	return _readerConstructors[name]
}

// SetReaderConfigConstructor 注册读取器配置构造函数
func SetReaderConfigConstructor(name string, fn func() interface{}) {
	_readerConfigConstructors[name] = fn
}

// GetReaderConfigConstructor 获取读取器配置构造函数
func GetReaderConfigConstructor(name string) func() interface{} {
	return _readerConfigConstructors[name]
}

type ReaderBase struct {
	conf       ReaderConfig
	ctx        context.Context
	cancelFunc context.CancelFunc
	done       int32
}

func NewReaderBase(conf ReaderConfig, parent context.Context) ReaderBase {
	ctx, cancelFunc := context.WithCancel(parent)

	return ReaderBase{
		conf: conf, ctx: ctx, cancelFunc: cancelFunc,
	}
}

func (r *ReaderBase) GetConfig() ReaderConfig {
	return r.conf
}

func (r *ReaderBase) GetCtx() context.Context {
	return r.ctx
}

func (r *ReaderBase) FirstClose() bool {
	if updated := atomic.AddInt32(&r.done, 1); updated == 1 {
		r.cancelFunc()
		return true
	}

	return false
}