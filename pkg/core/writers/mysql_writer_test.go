package writers

import (
	"github.com/Junjiayy/hamal/pkg/core/datasources"
	"github.com/Junjiayy/hamal/pkg/types"
	"gorm.io/gorm"
	"sync"
	"testing"
)

var (
	mw            MysqlWriter
	mysqlInitOnce = sync.Once{}
)

func initMysqlWriter(t *testing.T) {
	mysqlInitOnce.Do(func() {
		dataSource := datasources.NewMysqlDataSource()
		err := dataSource.SetDataSource(&datasources.DataSourceConfig{
			Name: "test", Host: "10.211.55.4", Port: 3306,
			Username: "root", Password: "123456", Target: "sync_tests",
			Debug: true,
		})

		if err != nil {
			t.Fatal(err)
		}

		source, _ := dataSource.GetDataSource("test")
		source.(*gorm.DB).Exec("TRUNCATE test_user_trans_records")
		mw = MysqlWriter{&writer{dataSources: dataSource}}
	})
}

func TestMysqlWriter_Insert(t *testing.T) {
	initMysqlWriter(t)
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
			Target: "test", PrimaryKey: "order_sn", TargetTable: "test_user_trans_records",
			SyncType: types.SyncTypeCopy,
		},
	}

	err := mw.Insert(params, map[string]string{
		"original_order_sn": "xlz2024030816051904940892",
		"user_id":           "1",
		"trans_price":       "5000",
	})

	if err != nil {
		t.Fatal(err)
	}

	source, _ := mw.dataSources.GetDataSource("test")
	cli := source.(*gorm.DB)
	var recordCount int64
	cli.Table("test_user_trans_records").Where("original_order_sn = ?",
		"xlz2024030816051904940892").Count(&recordCount)
	if recordCount != 1 {
		t.Fatalf("sync failed, db record counts should be: 1, get %d\n",
			recordCount)
	}
}

func TestMysqlWriter_Update(t *testing.T) {
	initMysqlWriter(t)
	source, _ := mw.dataSources.GetDataSource("test")
	cli := source.(*gorm.DB)
	cli.Exec("TRUNCATE test_user_trans_records")
	TestMysqlWriter_Insert(t)

	params := &types.SyncParams{
		Data: map[string]string{
			"id":           "1",
			"order_sn":     "xlz2024030816051904940892",
			"user_id":      "2",
			"product_type": "activity",
			"product_id":   "1",
			"price":        "4500",
		},
		Old: map[string]string{"user_id": "1", "price": "4500"},
		Rule: types.SyncRule{
			Target: "test", PrimaryKey: "order_sn", TargetTable: "test_user_trans_records",
			SyncType: types.SyncTypeCopy,
			Columns: map[string]string{
				"order_sn": "original_order_sn",
				"user_id":  "user_id",
				"price":    "trans_price",
			},
		},
	}

	var updateBeforeRecord, updateAfterRecord map[string]interface{}
	cli.Table("test_user_trans_records").Where("original_order_sn = ?",
		"xlz2024030816051904940892").Find(&updateBeforeRecord)
	if updateBeforeRecord == nil {
		t.Fatalf("update data Not ready")
	}

	err := mw.Update(params, map[string]string{
		"user_id":     "2",
		"trans_price": "4500",
	})

	if err != nil {
		t.Fatal(err)
	}

	cli.Table("test_user_trans_records").Where("original_order_sn = ?",
		"xlz2024030816051904940892").Find(&updateAfterRecord)

	beforeUserId := updateBeforeRecord["user_id"].(uint32)
	afterUserId := updateAfterRecord["user_id"].(uint32)
	if beforeUserId == afterUserId || afterUserId != 2 {
		t.Fatalf("update user_id failed")
	}

	beforeTransPrice := updateBeforeRecord["trans_price"].(uint32)
	afterTransPrice := updateAfterRecord["trans_price"].(uint32)
	if beforeTransPrice == afterTransPrice || afterTransPrice != 4500 {
		t.Fatalf("update trans_price failed")
	}
}

func TestMysqlWriter_Delete(t *testing.T) {
	initMysqlWriter(t)
	source, _ := mw.dataSources.GetDataSource("test")
	cli := source.(*gorm.DB)
	cli.Exec("TRUNCATE test_user_trans_records")
	TestMysqlWriter_Insert(t)

	params := &types.SyncParams{
		Data: map[string]string{
			"order_sn": "xlz2024030816051904940892",
		},
		Rule: types.SyncRule{
			Target: "test", PrimaryKey: "order_sn", TargetTable: "test_user_trans_records",
			SyncType: types.SyncTypeCopy,
			Columns: map[string]string{
				"order_sn": "original_order_sn",
			},
		},
	}

	err := mw.Delete(params)

	if err != nil {
		t.Fatal(err)
	}

	var recordCount int64
	cli.Table("test_user_trans_records").Where("original_order_sn = ?",
		"xlz2024030816051904940892").Count(&recordCount)
	if recordCount != 0 {
		t.Fatalf("sync failed, db record counts should be: 0, get %d\n",
			recordCount)
	}
}
