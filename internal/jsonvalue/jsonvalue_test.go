package jsonvalue

import (
	"math"
	"reflect"
	"testing"
)

func TestDecodeMapNormalizesJSONNumbers(t *testing.T) {
	got, err := DecodeMap([]byte(`{"small":42,"negative":-7,"maximum":18446744073709551615,"ratio":1.5,"nested":[9007199254740993]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"small": int64(42), "negative": int64(-7), "maximum": uint64(math.MaxUint64),
		"ratio": float64(1.5), "nested": []any{int64(9007199254740993)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded = %#v", got)
	}
}

func TestDecodeMapRejectsIntegerOutsideUint64(t *testing.T) {
	_, err := DecodeMap([]byte(`{"value":18446744073709551616}`))
	if err == nil {
		t.Fatal("out-of-range integer accepted")
	}
}
