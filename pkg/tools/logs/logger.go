package logs

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"regexp"
	"strconv"
	"strings"
)

type (
	// StackError 判断是否实现 errors.withStack 和 errors.withMessage
	stackError interface {
		Error() string
		Format(f fmt.State, verb rune)
		Cause() error
	}

	stackFormat struct {
		function, file, line string
	}

	stackFormats []stackFormat

	stackErrContext struct {
		err     string
		formats stackFormats
	}

	stackErrContexts []stackErrContext
)

var fileAndLinePattern = regexp.MustCompile(`\s+([^:]+):(\d+)`)

func ParseErr(err error) []zap.Field {
	fields := []zap.Field{zap.Error(err)}

	if _, ok := err.(stackError); ok {
		lines := strings.Split(fmt.Sprintf("%+v", err), "\n")
		if length := len(lines); length > 1 {
			var (
				contexts []stackErrContext
				ctx      stackErrContext
			)

		LineLoop:
			for i := 0; i < length || (ctx.err != "" || ctx.formats != nil); {
				for j := i; j <= i+1 && j < length; j++ {
					matches := fileAndLinePattern.FindStringSubmatch(lines[j])
					if len(matches) > 0 {
						ctx.formats = append(ctx.formats, stackFormat{
							function: lines[j-1], file: matches[1], line: matches[2],
						})

						i += j - i + 1
						continue LineLoop
					}
				}

				if ctx.err != "" {
					contexts = append(contexts, ctx)
					ctx = stackErrContext{}
				}

				if i < length {
					ctx.err = lines[i]
				}

				i++
			}

			if len(contexts) > 0 {
				fields = append(fields, zap.Array("context",
					stackErrContexts(contexts)))
			}
		}
	}

	return fields
}

func (formats stackFormats) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, format := range formats {
		if err := encoder.AppendObject(format); err != nil {
			return err
		}
	}

	return nil
}

func (s stackFormat) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	line, err := strconv.ParseInt(s.line, 10, 64)
	if err != nil {
		return err
	}

	encoder.AddString("func", s.function)
	encoder.AddString("file", s.file)
	encoder.AddInt64("line", line)

	return nil
}

func (s stackErrContext) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("error", s.err)
	if s.formats != nil {
		return encoder.AddArray("stack", s.formats)
	}

	return nil
}

func (contexts stackErrContexts) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, context := range contexts {
		if err := encoder.AppendObject(context); err != nil {
			return err
		}
	}

	return nil
}
