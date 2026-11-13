package client

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
)

// 编码规则与 Worker、Server 相同：Workflow 的输入与结果、Signal 与 Query 的参数、Query 结果是 JSON 对象；
// Update 的参数、结果与应用错误的 details 是 json/plain-v1 payload。数字保留精确值（整数解码为
// int64/uint64，小数为 float64）。

// EncodedValue 是一个尚未解码的结果，例如 Query 的返回值。
type EncodedValue interface {
	// HasValue 报告是否带有值。
	HasValue() bool
	// Get 把值解码进 valuePtr（非 nil 指针）；没有值时 valuePtr 不变并返回 nil。
	Get(valuePtr any) error
}

type objectValue struct {
	value map[string]any
}

func (v objectValue) HasValue() bool { return v.value != nil }

func (v objectValue) Get(valuePtr any) error {
	if v.value == nil {
		return checkDecodeTarget(valuePtr)
	}
	return decodeObject(v.value, valuePtr)
}

// encodeObject 把至多一个参数编码成 JSON 对象；没有参数或参数为 nil 时返回 nil。
func encodeObject(what string, args []any) (map[string]any, error) {
	switch len(args) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, fmt.Errorf("client: %s accepts at most one argument, got %d", what, len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	data, err := json.Marshal(args[0])
	if err != nil {
		return nil, fmt.Errorf("client: encode %s: %w", what, err)
	}
	var object map[string]any
	if err := jsonvalue.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("client: %s must encode as a JSON object, got %T: %w", what, args[0], err)
	}
	if object == nil {
		return nil, fmt.Errorf("client: %s must encode as a JSON object, got %T", what, args[0])
	}
	return object, nil
}

// decodeObject 把 Server 返回的 JSON 对象解码进 valuePtr。
func decodeObject(object map[string]any, valuePtr any) error {
	if err := checkDecodeTarget(valuePtr); err != nil || valuePtr == nil {
		return err
	}
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("client: encode result: %w", err)
	}
	if err := jsonvalue.Unmarshal(data, valuePtr); err != nil {
		return fmt.Errorf("client: decode result into %T: %w", valuePtr, err)
	}
	return nil
}

func encodePayloads(args []any) (protocol.Payloads, error) {
	payloads := make(protocol.Payloads, len(args))
	for i, arg := range args {
		payload, err := protocol.NewJSONPayload(arg)
		if err != nil {
			return nil, fmt.Errorf("client: encode argument %d: %w", i, err)
		}
		payloads[i] = payload
	}
	return payloads, nil
}

// decodePayload 是应用错误 details 与 Update 结果的解码器。
func decodePayload(payload protocol.Payload, valuePtr any) error {
	if valuePtr == nil {
		return fmt.Errorf("client: decode target must be a non-nil pointer, got nil")
	}
	if err := checkDecodeTarget(valuePtr); err != nil {
		return err
	}
	return payload.Decode(valuePtr)
}

func checkDecodeTarget(valuePtr any) error {
	if valuePtr == nil {
		return nil
	}
	value := reflect.ValueOf(valuePtr)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("client: decode target must be a non-nil pointer, got %T", valuePtr)
	}
	return nil
}
