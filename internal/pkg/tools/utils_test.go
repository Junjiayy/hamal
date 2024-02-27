package tools

import (
	"reflect"
	"testing"
	"time"
)

func Test_setFiledDefaultValue(t *testing.T) {
	type testArgs2 struct {
		Name   string  `default:"riley2"`
		Age    int32   `default:"6"`
		Money  float64 `default:"50.53"`
		Height uint32  `default:"176"`
	}

	type testArgs struct {
		Name     string        `default:"riley"`
		Age      int32         `default:"5"`
		Money    float64       `default:"50.5"`
		Height   uint32        `default:"175"`
		Values   []int32       `default:"1,2,3,4"`
		Values2  *[]int32      `default:"5,6,7"`
		Time     time.Duration `default:"5s"`
		TestArgs *testArgs2
	}

	args := &testArgs{}
	typeOf, valueOf := reflect.TypeOf(args), reflect.ValueOf(args)
	err := setFiledDefaultValue(valueOf, typeOf, "")
	if err != nil {
		t.Fatalf("Conversion failure: %v", err)
	}

	if args.Name != "riley" || args.Age != 5 || args.Height != 175 {
		t.Fatal("get default value failed")
	}

	if args.TestArgs.Name != "riley2" || args.TestArgs.Age != 6 || args.TestArgs.Height != 176 {
		t.Fatal("get pointer default value failed")
	}

	if len(args.Values) != 4 || len(*args.Values2) != 3 {
		t.Fatal("get pointer default value failed")
	}
}
