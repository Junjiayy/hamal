package configs

import "github.com/Junjiayy/hamal/internal/pkg/types"

type (
	SyncConfig struct {
		PoolSize             int                          `json:"pool_size" default:"50"`             // 协程池大小
		ReaderConcurrencyNum int                          `json:"reader_concurrency_num" default:"1"` // 读取器并发数量
		Reader               string                       `json:"reader" default:"web"`               // 读取器
		ReaderConfig         interface{}                  `json:"-"`                                  // 读取器初始化配置
		Rules                map[string][]*types.SyncRule `json:"rules"`                              // 同步规则
	}
)
