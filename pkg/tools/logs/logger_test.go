package logs

import (
	sysErr "errors"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
)

func TestFormat(t *testing.T) {
	//err := sysErr.New("this is test err")
	//err = errors.Wrap(err, "with message")
	//err = errors.WithStack(err)
	//err = errors.Wrap(err, "two with message")
	err := test2(test1(test()))
	//fmt.Printf("%+v\n", err)

	config := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(NewZapJsonEncoder(config), zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	logger := zap.New(core)
	logger.Error("test", ParseErr(err))
	//ParseErr(err)
}

func test() error {
	return sysErr.New("this is test err")
}

func test1(err error) error {
	return errors.Wrap(err, "with message")
}

func test2(err error) error {
	return errors.Wrap(err, "two message")
}

type (
	testLogParams struct {
		Field1 testParamsFiled1 `json:"field_1"`
	}
	testParamsFiled1 struct {
		Str     string             `json:"str"`
		Int     int                `json:"int"`
		Uint    uint               `json:"uint"`
		Float   float64            `json:"float"`
		Bool    bool               `json:"bool"`
		PStruct *testParamsPStruct `json:"p_struct"`
	}
	testParamsPStruct struct {
		Str       string                 `json:"str"`
		Int       int                    `json:"int"`
		Uint      uint                   `json:"uint"`
		Float     float64                `json:"float"`
		Bool      bool                   `json:"bool"`
		PStr      *string                `json:"p_str"`
		PInt      *int                   `json:"p_int"`
		PUint     *uint                  `json:"p_uint"`
		PFloat    *float64               `json:"p_float"`
		PBool     *bool                  `json:"p_bool"`
		Map       map[string]interface{} `json:"map"`
		StrMap    map[string]*string     `json:"str_map"`
		StructMap map[int]*pMapStruct    `json:"struct_map"`
	}

	pMapStruct struct {
		Slice []string                `json:"slice"`
		PMap  *map[string]*pArrStruct `json:"p_map"`
	}

	pArrStruct struct {
		Arr [2]*string `json:"arr"`
	}
)

func TestReflectiveLogObject_MarshalLogObject(t *testing.T) {
	pStr := "point test str"
	pInt := 3
	var pUint uint = 4
	pFloat := 4.4
	pBool := true

	params := &testLogParams{
		Field1: testParamsFiled1{
			Str:   "test str",
			Int:   1,
			Uint:  1,
			Float: 1.1,
			Bool:  true,
			PStruct: &testParamsPStruct{
				Str:    "inner test str",
				Int:    2,
				Uint:   2,
				Float:  2.2,
				Bool:   true,
				PStr:   &pStr,
				PInt:   &pInt,
				PUint:  &pUint,
				PFloat: &pFloat,
				PBool:  &pBool,
				Map: map[string]interface{}{
					"map1": "5",
					"map2": 5,
					"map3": 5.5,
				},
				StrMap: map[string]*string{
					pStr: &pStr,
				},
				StructMap: map[int]*pMapStruct{
					1: {
						Slice: []string{"1", "2", "3"},
						PMap: &map[string]*pArrStruct{
							"p_map_1": {
								Arr: [2]*string{&pStr, &pStr},
							},
						},
					},
				},
			},
		},
	}

	config := zap.NewProductionEncoderConfig()
	zapCore := zapcore.NewCore(NewZapJsonEncoder(config), zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	logger := zap.New(zapCore)

	logger.Info("test", zap.Reflect("test_params", params))
}
