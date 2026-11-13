package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// Workflow Task 完成时发现 started 之后有新事实
// ============================================
// 测试策略:
// - Error Case: Worker 读完历史后才落库的事实不随本次完成进入同一批；本次 WFT 记为失败并立即重做。
// - Happy Path: started 之后没有新事实时正常完成。
// ============================================

func startWorkflowTaskForTest(t *testing.T, mgr execution.ExecutionManager, runID string, wftID int64) {
	t.Helper()
	if _, err := mgr.Append(runID, "", "", history.NewWorkflowTaskStartedEvent(wftID, "worker-1", "", 0)); err != nil {
		t.Fatal(err)
	}
}

// Error Case: signal 落在「WFT 已 started、尚未完成」的窗口内，完成不提交，Run 得到一个新的 open WFT。
func TestWorkflowTaskCompletion_FactAfterStartedSupersedesTask(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{ID: "wf-superseded-task", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	startWorkflowTaskForTest(t, mgr, runID, wftID)
	if _, err := mgr.Append(runID, "", "", history.NewWorkflowSignaledEvent("late", nil)); err != nil {
		t.Fatal(err)
	}

	commands := []history.Command{{Type: history.CmdCompleteWorkflow}}
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, "worker-1"); err != nil {
		t.Fatal(err)
	}

	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var failedCause string
	for _, event := range events {
		if event.WorkflowTaskCompleted != nil || event.WorkflowCompleted != nil {
			t.Fatalf("superseded workflow task committed %s", event.EventType())
		}
		if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.ScheduledEventID == wftID {
			failedCause = event.WorkflowTaskFailed.Cause
		}
	}
	if failedCause != execution.WorkflowTaskFailedCauseUnhandledEvents {
		t.Fatalf("workflow task failed cause = %q, want %q", failedCause, execution.WorkflowTaskFailedCauseUnhandledEvents)
	}
	next := openWorkflowTaskForTest(t, mgr, runID)
	if next == wftID {
		t.Fatalf("open workflow task = %d, want a new one", next)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Blocked.Blocked || state.Blocked.ConsecutiveFailures != 0 {
		t.Fatalf("superseded workflow task counted as a failure: %+v", state.Blocked)
	}

	// 重做的 WFT 看得到 signal，正常完成。
	startWorkflowTaskForTest(t, mgr, runID, next)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, next), runID, next, commands, "worker-1"); err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != history.StatusCompleted {
		t.Fatalf("run status = %s, want completed", run.Status)
	}
}

// Happy Path: started 之后没有新事实，完成照常提交。
func TestWorkflowTaskCompletion_NoFactAfterStartedCommits(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{ID: "wf-not-superseded-task", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	startWorkflowTaskForTest(t, mgr, runID, wftID)

	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []history.Command{{Type: history.CmdCompleteWorkflow}}, "worker-1"); err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != history.StatusCompleted {
		t.Fatalf("run status = %s, want completed", run.Status)
	}
}

// Error Case: Update 在「WFT 已 started、尚未完成」的窗口内被接收。Worker 这次决策没看到它，
// 完成不提交；重做的 WFT 看得到 UpdateAccepted，Handler 才有机会运行。
func TestWorkflowTaskCompletion_UpdateAcceptedAfterStartedSupersedesTask(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{ID: "wf-superseded-by-update", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	startWorkflowTaskForTest(t, mgr, runID, wftID)
	if _, err := srv.UpdateWorkflow(ctx, "", "", runID, "place", "update-late", protocol.UpdateWaitStageEnums.Accepted, nil); err != nil {
		t.Fatal(err)
	}

	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []history.Command{{Type: history.CmdCompleteWorkflow}}, "worker-1"); err != nil {
		t.Fatal(err)
	}

	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var failedCause string
	for _, event := range events {
		if event.WorkflowTaskCompleted != nil || event.WorkflowCompleted != nil {
			t.Fatalf("workflow task that never saw the update committed %s", event.EventType())
		}
		if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.ScheduledEventID == wftID {
			failedCause = event.WorkflowTaskFailed.Cause
		}
	}
	if failedCause != execution.WorkflowTaskFailedCauseUnhandledEvents {
		t.Fatalf("workflow task failed cause = %q, want %q", failedCause, execution.WorkflowTaskFailedCauseUnhandledEvents)
	}
	if next := openWorkflowTaskForTest(t, mgr, runID); next == wftID {
		t.Fatalf("open workflow task = %d, want a new one", next)
	}
}
