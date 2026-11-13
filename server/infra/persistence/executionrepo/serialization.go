package executionrepo

import (
	"encoding/json"
	"fmt"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
)

func marshalMap(value map[string]any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal json map: %w", err)
	}
	return data, nil
}

// unmarshalMap 用 jsonvalue 的规则还原数字，与事件载荷的解码一致：整数是 int64，大于 2^53 时保持精确。
func unmarshalMap(data []byte) (map[string]any, error) {
	value, err := jsonvalue.DecodeMap(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal json map: %w", err)
	}
	return value, nil
}

func marshalStringMap(value map[string]string) ([]byte, error) {
	if len(value) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal json string map: %w", err)
	}
	return data, nil
}

// traceContextColumn 编码写进 trace_context_json 的值。这一列是 MySQL JSON 类型，值以字符串绑定：客户端插值
// （interpolateParams=true）把 []byte 参数写成 _binary'...'，MySQL 拒绝用 binary 字符集的字符串构造 JSON 值。
// 空 map 写 NULL。
func traceContextColumn(value map[string]string) (*string, error) {
	data, err := marshalStringMap(value)
	if err != nil || data == nil {
		return nil, err
	}
	text := string(data)
	return &text, nil
}

func traceContextFromColumn(value *string) (map[string]string, error) {
	if value == nil {
		return nil, nil
	}
	return unmarshalStringMap([]byte(*value))
}

func unmarshalStringMap(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var value map[string]string
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("unmarshal json string map: %w", err)
	}
	return value, nil
}
