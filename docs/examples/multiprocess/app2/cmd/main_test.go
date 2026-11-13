package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitCSV(t *testing.T) {
	// Happy Path / Edge Case: 保留队列顺序，去除各项空白并过滤空项。
	for _, tt := range []struct {
		name  string
		value string
		want  []string
	}{
		{name: "queues", value: "inventory,ledger", want: []string{"inventory", "ledger"}},
		{name: "trim whitespace", value: " inventory ,\tledger\n", want: []string{"inventory", "ledger"}},
		{name: "filter empty items", value: ",inventory,, ,ledger,", want: []string{"inventory", "ledger"}},
		{name: "empty input", value: ""},
		{name: "only whitespace and separators", value: " \t, ,\n,"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, splitCSV(tt.value))
		})
	}
}
