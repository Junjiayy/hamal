package main

import (
	"flag"
	"github.com/Junjiayy/hamal/internal/core"
	"github.com/Junjiayy/hamal/pkg/configs"
	"github.com/Junjiayy/hamal/pkg/tools"
	"io/ioutil"
)

var filePath = flag.String("f", "config.yaml", "Specify the config file")

func main() {
	flag.Parse()

	content, err := ioutil.ReadFile(*filePath)
	if err != nil {
		panic(err)
	}

	var conf configs.SyncConfig
	if err := tools.UnmarshalYamlAndBuildDefault(content, &conf); err != nil {
		panic(err)
	}

	c, err := core.NewCore(&conf)
	if err != nil {
		panic(err)
	}
	if err := c.Run(); err != nil {
		panic(err)
	}
}
