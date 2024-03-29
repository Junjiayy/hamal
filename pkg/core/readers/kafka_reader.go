package readers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Junjiayy/hamal/pkg/tools"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"reflect"
	"strings"
	"sync"
	"time"
)

type KafkaReader struct {
	ReaderBase
	kr *kafka.Reader
}

func NewKafkaReaderFunc(conf ReaderConfig, wg *sync.WaitGroup, parent context.Context) (Reader, error) {
	config, ok := conf.(*KafkaReaderConfig)
	if !ok {
		return nil, configAssertErr
	}
	var dialer *kafka.Dialer
	if config.Username != "" && config.Password != "" {
		dialer = &kafka.Dialer{SASLMechanism: plain.Mechanism{Username: config.Username, Password: config.Password}}
	}

	readerConfig := kafka.ReaderConfig{
		Brokers: config.Brokers, GroupID: config.Group, Topic: config.Topic,
		MinBytes: config.MinBytes, MaxBytes: config.MaxBytes, StartOffset: config.StartOffset,
		MaxWait: config.MaxWait, CommitInterval: config.CommitInterval,
		QueueCapacity: config.QueueCapacity, Partition: config.Partition, Dialer: dialer,
	}

	return &KafkaReader{
		kr:         kafka.NewReader(readerConfig),
		ReaderBase: NewReaderBase(config, parent),
	}, nil
}

func (k *KafkaReader) Read() (*types.BinlogParams, error) {
	message, err := k.kr.FetchMessage(k.ctx)
	if err != nil {
		return nil, err
	}

	binLogParams := new(types.BinlogParams)
	if err := json.Unmarshal(message.Value, binLogParams); err != nil {
		return nil, err
	}
	binLogParams.Source = message

	return binLogParams, nil
}

func (k *KafkaReader) Complete(params *types.BinlogParams) error {
	return k.kr.CommitMessages(k.ctx, params.Source.(kafka.Message))
}

func (k *KafkaReader) Close() error {
	if k.FirstClose() {
		return k.kr.Close()
	}

	return nil
}

type KafkaReaderConfig struct {
	Brokers        []string      `json:"brokers" yaml:"brokers" default:"localhost:9092"`
	Username       string        `json:"username,omitempty" yaml:"username,omitempty"`
	Password       string        `json:"password,omitempty" yaml:"password,omitempty"`
	Group          string        `json:"group" yaml:"group" default:"test"`
	Topic          string        `json:"topic" yaml:"topic"`
	Partition      int           `json:"partition" yaml:"partition"`
	MinBytes       int           `json:"min_bytes,omitempty" yaml:"min_bytes,omitempty" default:"10240"`
	MaxBytes       int           `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty" default:"10485760"`
	StartOffset    int64         `json:"start_offset,omitempty" yaml:"start_offset,omitempty" default:"-1"`
	MaxWait        time.Duration `json:"max_wait,omitempty" yaml:"max_wait,omitempty" default:"1s"`
	CommitInterval time.Duration `json:"commit_interval,omitempty" yaml:"commit_interval,omitempty" default:"1s"`
	QueueCapacity  int           `json:"queue_capacity,omitempty" yaml:"queue_capacity,omitempty" default:"1000"`
}

func NewKafkaReaderConfigFunc() interface{} {
	return &KafkaReaderConfig{}
}

func (k *KafkaReaderConfig) GetUniqueId() string {
	return tools.Hash32(fmt.Sprintf("%s-%s-%s-%s-%s-%d", strings.Join(k.Brokers, "-"),
		k.Username, k.Password, k.Group, k.Topic, k.Partition))
}

func (k *KafkaReaderConfig) Equal(config ReaderConfig) bool {
	newConfig, ok := config.(*KafkaReaderConfig)
	if ok {
		ok = reflect.DeepEqual(k, newConfig)
	}

	return ok
}
