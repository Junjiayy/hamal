package nodes

import (
	"encoding/json"
	"fmt"
	"github.com/Junjiayy/hamal/pkg/core/readers"
	"github.com/go-zookeeper/zk"
	"testing"
	"time"
)

func TestFollower_Run(t *testing.T) {
	servers := []string{"10.150.106.201:2181"}
	conn, _, err := zk.Connect(servers, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	n := &node{zkConn: conn}
	data, err := n.getNodeDataOrCreate("/test", zk.FlagEphemeral|zk.FlagSequence)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%v, %d, %v", data, len(data), data == nil)
	var configs []readers.ReaderConfigByType
	err = json.Unmarshal([]byte{}, &configs)
	if err != nil {
		t.Fatal(err)
	}
}
