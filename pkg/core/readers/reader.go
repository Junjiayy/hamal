package readers

import (
	"github.com/Junjiayy/hamal/pkg/types"
	"sync"
)

type ReaderConstructor func(interface{}, *sync.WaitGroup) (types.Reader, error)

// 读取器构造函数集合
var (
	_readers       = make(map[string]ReaderConstructor)
	_readerConfigs = make(map[string]func() interface{})
)

func init() {
	RegisterReaderConstructor("web", NewHttpReaderFunc)
	RegisterReaderConfigConstructor("web", NewHttpReaderConfigFunc)
}

// RegisterReaderConstructor 注册读取器构造函数
func RegisterReaderConstructor(name string, fn ReaderConstructor) {
	_readers[name] = fn
}

// GetReaderConstructor 获取读取器构造函数
func GetReaderConstructor(name string) ReaderConstructor {
	return _readers[name]
}

// RegisterReaderConfigConstructor 注册读取器配置构造函数
func RegisterReaderConfigConstructor(name string, fn func() interface{}) {
	_readerConfigs[name] = fn
}

// GetReaderConfigConstructor 获取读取器配置构造函数
func GetReaderConfigConstructor(name string) func() interface{} {
	return _readerConfigs[name]
}
