package writers

import (
	"context"
	"encoding/json"
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
	"github.com/olivere/elastic/v7"
	"sync"
	"testing"
)

var (
	ew         ElasticSearchWriter
	esInitOnce sync.Once
)

func initEsWriter(t *testing.T) {
	esInitOnce.Do(func() {
		dataSource := datasources.NewElasticSearchDataSource()
		err := dataSource.SetDataSource(&datasources.DataSourceConfig{
			Name: "test", Host: "10.211.55.4", Port: 9200,
		})

		if err != nil {
			t.Fatal(err)
		}
		cli, _ := dataSource.GetDataSource("test")
		cli.(*elastic.Client).DeleteByQuery("test_user_trans_records").
			Query(elastic.NewMatchAllQuery()).Do(context.Background())

		ew = ElasticSearchWriter{&writer{dataSources: dataSource}}
	})
}

func TestElasticSearchWriter_Insert(t *testing.T) {
	initEsWriter(t)
	params := &types.SyncParams{
		Data: map[string]string{
			"id":           "1",
			"order_sn":     "xlz2024030816051904940892",
			"user_id":      "1",
			"product_type": "activity",
			"product_id":   "1",
			"price":        "5000",
		},
		Rule: types.SyncRule{
			Target: "test", PrimaryKey: "id", TargetTable: "test_user_trans_records",
			SyncType: types.SyncTypeCopy,
		},
	}

	err := ew.Insert(params, map[string]string{
		"original_order_sn": "xlz2024030816051904940892",
		"user_id":           "1",
		"trans_price":       "5000",
		"id":                "1",
	})

	if err != nil {
		t.Fatal(err)
	}

	cli, _ := ew.dataSources.GetDataSource("test")
	esCli := cli.(*elastic.Client)
	result, err := esCli.Search("test_user_trans_records").
		Query(elastic.NewTermQuery("id", 1)).
		Size(1).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if length := len(result.Hits.Hits); length != 1 {
		t.Fatalf("es records count should be:1, get: %d", length)
	}

	firstHit := result.Hits.Hits[0]
	var insertRecord map[string]interface{}
	if err := json.Unmarshal(firstHit.Source, &insertRecord); err != nil {
		t.Fatal(err)
	}

	if firstHit.Id != insertRecord["id"].(string) {
		t.Fatalf("_id != id, _id: %s, id: %v", firstHit.Id, insertRecord["id"])
	}
}
