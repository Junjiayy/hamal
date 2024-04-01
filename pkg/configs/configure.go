package configs

type (
	SyncConfig struct {
		PoolSize    int `json:"pool_size,omitempty" yaml:"pool_size,omitempty" default:"50"` // 协程池大小
		RedisConfig struct {
			Addr     string `json:"addr" yaml:"addr"`
			Password string `json:"password,omitempty" yaml:"password,omitempty"`
			DB       int    `json:"db,omitempty" yaml:"db,omitempty" default:"0"`
		} `json:"redis" yaml:"redis"`

		ZookeeperConfig struct {
			Hosts    []string `json:"hosts" yaml:"hosts"`
			Username string   `json:"username" yaml:"username"`
			Password string   `json:"password" yaml:"password"`
		}
	}
)
