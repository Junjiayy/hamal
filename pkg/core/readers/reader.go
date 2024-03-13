package readers

import (
	"context"
	"github.com/Junjiayy/hamal/pkg/types"
	"sync"
)

type (
	Reader interface {
		Read() (*types.BinlogParams, error)
		Complete(params *types.BinlogParams) error
		Close() error
	}

	ReaderConstructor func(interface{}, *sync.WaitGroup, context.Context) (Reader, error)
)

var (
	_readerConstructors       = make(map[string]ReaderConstructor)  // 读取器构造函数 映射表
	_readerConfigConstructors = make(map[string]func() interface{}) // 读取器配置类型构造函数 映射表
)

func init() {
	SetReaderConstructor(types.ReaderTypeWeb, NewHttpReaderFunc)
	SetReaderConfigConstructor(types.ReaderTypeWeb, NewHttpReaderConfigFunc)
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
