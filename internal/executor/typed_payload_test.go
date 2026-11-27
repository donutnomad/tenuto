package executor

import (
	"encoding/json"
	"reflect"
	"testing"
)

// GoJSONNumbers 把 map[string]any、[]any 里各层的数字换成 float64，与 Go 的 encoding/json 解码进 map[string]any
// 的结果相同；超过 2^53 的整数与 Go 一样丢失精度。
func TestGoJSONNumbersMatchesEncodingJSON(t *testing.T) {
	payload := map[string]any{
		"int": int64(1), "uint": uint64(18446744073709551615), "big": int64(9007199254740993),
		"float": 1.5, "number": json.Number("7"), "text": "1", "null": nil,
		"list":   []any{int64(2), "x", []any{int(3)}},
		"nested": map[string]any{"n": int64(3), "deep": map[string]any{"m": int32(4)}},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if got := GoJSONNumbers(payload); !reflect.DeepEqual(got, want) {
		t.Fatalf("GoJSONNumbers = %#v, encoding/json = %#v", got, want)
	}
	if payload["int"] != int64(1) {
		t.Fatalf("GoJSONNumbers modified its input: %#v", payload)
	}
}
