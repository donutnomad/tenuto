// Package jsonvalue 解码 JSON 时保留数字的精确值：整数是 int64（超过 int64 的是 uint64），带小数的是 float64。
// 超过 2^53 的整数因此原样保留，两个后端与 HTTP 两侧得到同一种数值表示。
package jsonvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Unmarshal 把 data 解码进 v，v 里 any 类型的值（map[string]any、[]any 与数字）按 Normalize 归一化。
func Unmarshal(data []byte, v any) error {
	return Decode(bytes.NewReader(data), v)
}

// Decode 从 r 读取一个 JSON 值解码进 v，v 里 any 类型的值按 Normalize 归一化。
func Decode(r io.Reader, v any) error {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	return normalizeValue(reflect.ValueOf(v))
}

// normalizeValue 把经 UseNumber 解码的 any 值（map[string]any、[]any、json.Number）原地换成归一化后的值。
func normalizeValue(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			return normalizeValue(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				if err := normalizeValue(v.Field(i)); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < v.Len(); i++ {
			if err := normalizeValue(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map, reflect.Interface:
		if v.IsNil() || !v.CanSet() {
			return nil
		}
		if _, ok := v.Interface().(map[string]any); v.Kind() == reflect.Map && !ok {
			return nil
		}
		normalized, err := Normalize(v.Interface())
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(normalized))
	}
	return nil
}

func DecodeMap(data []byte) (map[string]any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := requireEOF(decoder); err != nil {
		return nil, err
	}
	normalized, err := Normalize(value)
	if err != nil {
		return nil, err
	}
	return normalized.(map[string]any), nil
}

func Normalize(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		return normalizeNumber(value)
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, item := range value {
			normalized, err := Normalize(item)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
			result[key] = normalized
		}
		return result, nil
	case []any:
		result := make([]any, len(value))
		for index, item := range value {
			normalized, err := Normalize(item)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", index, err)
			}
			result[index] = normalized
		}
		return result, nil
	default:
		return value, nil
	}
}

func NormalizeMap(value map[string]any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	normalized, err := Normalize(value)
	if err != nil {
		return nil, err
	}
	return normalized.(map[string]any), nil
}

func normalizeNumber(value json.Number) (any, error) {
	text := value.String()
	if strings.ContainsAny(text, ".eE") {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return nil, fmt.Errorf("invalid JSON number %q", text)
		}
		return parsed, nil
	}
	if strings.HasPrefix(text, "-") {
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("integer %q exceeds int64", text)
		}
		return parsed, nil
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("integer %q exceeds uint64", text)
	}
	if parsed <= math.MaxInt64 {
		return int64(parsed), nil
	}
	return parsed, nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
