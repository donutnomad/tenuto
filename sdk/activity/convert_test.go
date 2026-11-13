package activity

import (
	"math"
	"testing"
)

func TestStructToMapPreservesTypedIntegerWidths(t *testing.T) {
	value, err := structToMap(struct {
		Maximum uint64  `json:"maximum"`
		Count   int64   `json:"count"`
		Ratio   float64 `json:"ratio"`
	}{Maximum: math.MaxUint64, Count: 9007199254740993, Ratio: 1.25})
	if err != nil {
		t.Fatal(err)
	}
	if value["maximum"] != uint64(math.MaxUint64) {
		t.Fatalf("maximum = %#v", value["maximum"])
	}
	if value["count"] != int64(9007199254740993) {
		t.Fatalf("count = %#v", value["count"])
	}
	if value["ratio"] != float64(1.25) {
		t.Fatalf("ratio = %#v", value["ratio"])
	}
}
