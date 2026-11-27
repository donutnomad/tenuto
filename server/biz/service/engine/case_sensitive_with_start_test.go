package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// caseFoldingCurrentRuns 模拟不区分大小写的存储：按 Workflow ID 查 current Run 时忽略大小写。
type caseFoldingCurrentRuns struct{ execution.ExecutionManager }

func (m caseFoldingCurrentRuns) GetCurrentRunID(namespace, workflowID string) (string, error) {
	return m.ExecutionManager.GetCurrentRunID(strings.ToLower(namespace), strings.ToLower(workflowID))
}

// 存储把 order-AB12 解析到 order-ab12 的 current Run 时，signal-with-start 与 update-with-start 拒绝写入（409），
// 不把 Signal 或 Update 写进另一个 Workflow。
func TestWithStartRejectsCurrentRunOfAnotherWorkflowID(t *testing.T) {
	ctx := context.Background()
	server := assembleServer(caseFoldingCurrentRuns{memory.NewExecutionManager()}, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-case")
	run, err := server.StartWorkflow(ctx, "", "wf-case", "order-ab12", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := server.SignalWithStart(ctx, "", "wf-case", "order-AB12", nil, "pay", "", nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("signal-with-start = %v, want ErrCurrentRunConflict", err)
	}
	if _, _, err := server.UpdateWithStart(ctx, "", "wf-case", "order-AB12", nil, "approve", "u-1", nil, protocol.UpdateWaitStageEnums.Accepted); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("update-with-start = %v, want ErrCurrentRunConflict", err)
	}
	if got := countSignals(t, server, run.ID); got != 0 {
		t.Fatalf("WorkflowSignaled events on order-ab12 = %d, want 0", got)
	}
	events, err := server.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowUpdateAccepted != nil {
			t.Fatalf("update-with-start for order-AB12 was accepted on order-ab12")
		}
	}
}
