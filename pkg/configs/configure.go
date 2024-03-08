package configs

import (
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/Junjiayy/hamal/pkg/types"
	"gopkg.in/yaml.v2"
)

type (
	SyncConfig struct {
		PoolSize int                          `json:"pool_size" default:"50"` // 协程池大小
		Readers  []*ReaderConfig              `json:"readers"`                // 读取器配置
		Rules    map[string][]*types.SyncRule `json:"rules"`                  // 同步规则
	}

	ReaderConfig struct {
		Name   string      `json:"name" yaml:"name" default:"web"`
		Params interface{} `json:"params" yaml:"params"`
	}

	innerReaderConfig ReaderConfig
)

func (c *ReaderConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var innerConfig innerReaderConfig
	if err := unmarshal(&innerConfig); err != nil {
		return err
	}
	configConstructor := readers.GetReaderConfigConstructor(innerConfig.Name)
	params := configConstructor()
	paramsData, err := yaml.Marshal(innerConfig.Params)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(paramsData, params); err != nil {
		return err
	}
	c.Name, c.Params = innerConfig.Name, params

	return nil
}
