package tools

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// UnmarshalYamlAndBuildDefault 解析yaml文件并赋值默认值
func UnmarshalYamlAndBuildDefault(in []byte, dest interface{}) error {
	if err := yaml.Unmarshal(in, dest); err != nil {
		return err
	}
	typeOf, valueOf := reflect.TypeOf(dest), reflect.ValueOf(dest)

	return setFiledDefaultValue(valueOf, typeOf, "")
}

// UnmarshalJsonAndBuildDefault 解析json文件并赋值默认值
func UnmarshalJsonAndBuildDefault(in []byte, dest interface{}) error {
	if err := json.Unmarshal(in, dest); err != nil {
		return err
	}
	typeOf, valueOf := reflect.TypeOf(dest), reflect.ValueOf(dest)

	return setFiledDefaultValue(valueOf, typeOf, "")
}

// setFiledDefaultValue 根据字段default tag 赋值默认值
func setFiledDefaultValue(field reflect.Value, valType reflect.Type, tag string) error {
	if field.Kind() == reflect.Ptr {
		field, valType = field.Elem(), field.Elem().Type()
	}

	switch valType.Kind() {
	case reflect.Struct:
		for i := 0; i < valType.NumField(); i++ {
			innerField, innerTypeFiled := field.Field(i), valType.Field(i)
			if innerField.Kind() == reflect.Ptr && innerField.IsZero() {
				elem := reflect.New(innerTypeFiled.Type.Elem()).Elem()
				innerField.Set(elem.Addr())
			}

			innerTag := innerTypeFiled.Tag.Get("default")
			if err := setFiledDefaultValue(innerField, innerTypeFiled.Type,
				innerTag); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if field.IsZero() && tag != "" {
			// 切片根据逗号分割，例如: `default:"1,2,3,4"` 赋值为 [1, 2, 3, 4]
			values, indexValType := strings.Split(tag, ","), valType.Elem()
			field.Set(reflect.MakeSlice(reflect.SliceOf(indexValType), len(values), len(values)))
			for i := 0; i < len(values); i++ {
				if err := getFieldDefaultValueByReflect(values[i], field.Index(i), indexValType); err != nil {
					return err
				}
			}
		}
	default:
		if field.IsZero() && tag != "" {
			if err := getFieldDefaultValueByReflect(tag, field, valType); err != nil {
				return err
			}
		}
	}

	return nil
}

// getFieldDefaultValueByReflect 通过反射给字段赋值
func getFieldDefaultValueByReflect(defaultValue string, field reflect.Value, valType reflect.Type) error {
	switch field.Interface().(type) {
	case time.Duration:
		val, err := time.ParseDuration(defaultValue)
		if err != nil {
			return err
		}
		field.Set(reflect.ValueOf(val))
	default:
		switch valType.Kind() {
		case reflect.Bool:
			val, err := strconv.ParseBool(defaultValue)
			if err != nil {
				return err
			}
			field.SetBool(val)
		case reflect.String:
			field.SetString(defaultValue)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64, reflect.Int32:
			val, err := strconv.ParseInt(defaultValue, 10, 64)
			if err != nil {
				return err
			}
			field.SetInt(val)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := strconv.ParseUint(defaultValue, 10, 64)
			if err != nil {
				return err
			}
			field.SetUint(val)
		case reflect.Float32, reflect.Float64:
			val, err := strconv.ParseFloat(defaultValue, 64)
			if err != nil {
				return err
			}
			field.SetFloat(val)
		}
	}

	return nil
}

// JudgmentEval 判断 动态执行
func JudgmentEval(first, two string, operator string) bool {
	switch operator {
	case ">":
		return first > two
	case "<":
		return first < two
	case "=":
		return first == two
	case "!=":
		return first != two
	case ">=":
		return first >= two
	case "<=":
		return first <= two
	}

	return false
}

// CamelToSnake 驼峰式字符串转下划线式
func CamelToSnake(dest string) string {
	var result strings.Builder
	for i, r := range dest {
		if unicode.IsUpper(r) {
			if i > 0 {
				result.WriteByte('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// Hash32 快速获取 32 位hash
func Hash32(source string) string {
	h := fnv.New32a() // 使用32位FNV-1a算法
	_, err := h.Write([]byte(source))
	if err != nil {
		panic(err) // 处理写入错误
	}

	return fmt.Sprintf("%x",
		h.Sum32())
}
