package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Error Case: History 已有终态事件、Run 行仍是 Running（状态投影落后于 History）时，Signal、Update 与取消按请求错误
// 应答，History 停在终态事件。按 Workflow ID 寻址时由存储在持有 execution state 锁的事务里拒绝（ErrRunClosed）；
// 按 Run ID 寻址时 lease 外按 History 判定 Run 已结束。
func TestInteractionsWithClosedHistoryReturnRunClosed(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	run := &execution.Run{ID: "run-closed-history", Namespace: protocol.DefaultNamespace, WorkflowID: "wf-closed-history", WorkflowType: "wf-closed-history", Status: historypkg.StatusRunning, IsCurrent: true, CreatedAt: time.Now()}
	registerRunWorkflowForTest(t, srv, run)
	createStartedRunForTest(t, mgr, run)
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	for _, runID := range []string{"", run.ID} {
		if _, err := srv.SignalWorkflow(ctx, run.Namespace, run.WorkflowID, runID, "approve", "", nil); !closedRunError(runID, err) {
			t.Errorf("signal (run id %q): err = %v, want an invalid request", runID, err)
		}
		if _, err := srv.SignalWorkflow(ctx, run.Namespace, run.WorkflowID, runID, "approve", "req-1", nil); !closedRunError(runID, err) {
			t.Errorf("signal with request id (run id %q): err = %v, want an invalid request", runID, err)
		}
		if err := srv.CancelWorkflow(ctx, run.Namespace, run.WorkflowID, runID, "stop"); !closedRunError(runID, err) {
			t.Errorf("cancel (run id %q): err = %v, want an invalid request", runID, err)
		}
		if _, err := srv.UpdateWorkflow(ctx, run.Namespace, run.WorkflowID, runID, "setLimit", "upd-1", protocol.UpdateWaitStage("Accepted"), nil); !closedRunError(runID, err) {
			t.Errorf("update (run id %q): err = %v, want an invalid request", runID, err)
		}
	}
	assertNoEventAfterTerminal(t, mgr, run.ID)
}

func closedRunError(runID string, err error) bool {
	if runID == "" {
		return errors.Is(err, execution.ErrRunClosed)
	}
	return errors.Is(err, protocol.ErrInvalidRequest)
}
