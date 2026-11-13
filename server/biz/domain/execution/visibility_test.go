package execution_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Run List 的页大小、偏移与 RunIDs 个数越界时返回 400 类错误，不读存储；上限本身照常受理。
func TestListRunsBoundsPageSize(t *testing.T) {
	store := memory.NewExecutionManager()
	tooManyIDs := make([]string, execution.MaxRunListPageSize+1)
	for i := range tooManyIDs {
		tooManyIDs[i] = fmt.Sprintf("run-%d", i)
	}
	for name, filter := range map[string]execution.RunListFilter{
		"limit above maximum": {Limit: execution.MaxRunListPageSize + 1},
		"negative limit":      {Limit: -1},
		"negative offset":     {Offset: -1},
		"too many run ids":    {RunIDs: tooManyIDs},
	} {
		if _, err := execution.ListRuns(store, filter); !errors.Is(err, protocol.ErrInvalidRequest) {
			t.Errorf("%s: err = %v, want ErrInvalidRequest", name, err)
		}
	}
	if _, err := execution.ListRuns(store, execution.RunListFilter{Limit: execution.MaxRunListPageSize}); err != nil {
		t.Fatalf("limit at maximum: %v", err)
	}
}
