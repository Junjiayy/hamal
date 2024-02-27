package configs

type (
	SyncConfig struct {
		PoolSize             int `json:"pool_size" default:"50"`             // 协程池大小
		ReaderConcurrencyNum int `json:"reader_concurrency_num" default:"1"` // 读取器并发数量
	}
)
