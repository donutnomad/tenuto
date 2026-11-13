package executionrepo

import "testing"

func TestHistoryMetricsAcceptsMySQLDecimalDriverValues(t *testing.T) {
	for _, input := range []any{[]byte("2207.000000000000000000000000000000"), "2207.000000000000000000000000000000", int64(2207)} {
		var metrics historyMetrics
		if err := metrics.Size.Scan(input); err != nil {
			t.Fatalf("scan %T: %v", input, err)
		}
		if got := metrics.Size.IntPart(); got != 2207 {
			t.Fatalf("size=%d", got)
		}
	}
	var metrics historyMetrics
	if err := metrics.Size.Scan([]byte("9007199254740993.000000000000000000000000000000")); err != nil {
		t.Fatal(err)
	}
	if metrics.Size.IntPart() != 9007199254740993 {
		t.Fatal("history byte count lost integer precision")
	}
}
