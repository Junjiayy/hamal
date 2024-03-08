package configs

import (
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"gopkg.in/yaml.v2"
	"testing"
)

func TestReaderConfig_UnmarshalYAML(t *testing.T) {
	readers.RegisterReaderConfigConstructor("web", readers.NewHttpReaderConfigFunc)
	yamlData := `
name: "web"
params:
  listen: ":8081"
  push_path: "/push/tests"
`
	var conf ReaderConfig
	if err := yaml.Unmarshal([]byte(yamlData), &conf); err != nil {
		t.Fatal(err)
	}

	if conf.Params == nil {
		t.Fatal("ReaderConfig.Params is nil")
	}

	if _, ok := conf.Params.(*readers.HttpReaderConfig); !ok {
		t.Fatal("ReaderConfig.Params Not a readers.HttpReaderConfig instance")
	}
}
