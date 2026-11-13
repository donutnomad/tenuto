package history

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
)

var anyMapType = reflect.TypeOf(map[string]any(nil))

// DecodeEventJSON 解出 CanonicalEventJSON 写出的事件，是存储层读事件的唯一解码入口。
// map[string]any 载荷里的数字按 jsonvalue 的规则还原：整数是 int64（超过 int64 的是 uint64），
// 带小数或指数的是 float64，大于 2^53 的整数保持精确。
func DecodeEventJSON(data []byte) (Event, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var event Event
	if err := decoder.Decode(&event); err != nil {
		return Event{}, fmt.Errorf("decode event json: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Event{}, fmt.Errorf("decode event json: trailing data")
	}
	// 子事件的载荷字段都是 map[string]any，按类型找出来统一还原数字，新增的载荷字段不用登记。
	union := reflect.ValueOf(&event).Elem()
	for i := 0; i < union.NumField(); i++ {
		sub := union.Field(i)
		if sub.Kind() != reflect.Pointer || sub.IsNil() {
			continue
		}
		fields := sub.Elem()
		for j := 0; j < fields.NumField(); j++ {
			field := fields.Field(j)
			if field.Type() != anyMapType || field.IsNil() {
				continue
			}
			normalized, err := jsonvalue.NormalizeMap(field.Interface().(map[string]any))
			if err != nil {
				return Event{}, fmt.Errorf("decode event json: %s.%s: %w", union.Type().Field(i).Name, fields.Type().Field(j).Name, err)
			}
			field.Set(reflect.ValueOf(normalized))
		}
	}
	return event, nil
}
