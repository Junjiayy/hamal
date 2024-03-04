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
	logger.Error("test", ParseErr(err)...)
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
