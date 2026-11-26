package engine

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// Workflow Task 进行期间到达的新事实
// ============================================
// 测试策略:
// - Error Case: 含终态命令的完成遇到 Worker 没看到的事实：本次 WFT 记为失败（unhandled_events）并立即重做。
// - Happy Path: 非终态命令照常提交，新事实归入下一批并调度下一个 WFT；没有新事实时正常完成。
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
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-superseded-task", Program: programtest.Noop()})
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
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-not-superseded-task", Program: programtest.Noop()})
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
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-superseded-by-update", Program: programtest.Noop()})
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

// Happy Path: 非终态命令照常提交。WFT 进行期间到达的 Signal 排在 WorkflowTaskCompleted 之前、ID 大于
// SeenThroughEventID，重放归入下一批；提交之后 Run 立即得到下一个 WFT 来处理它。
func TestWorkflowTaskCompletion_FactAfterStartedLeavesNonTerminalCommit(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-unseen-fact", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	startWorkflowTaskForTest(t, mgr, runID, wftID)
	signalID, err := srv.SignalWorkflow(ctx, "", "", runID, "late", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	commands := []history.Command{{Type: history.CmdScheduleTimer, TimerID: "t", Delay: time.Hour}}
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, "worker-1"); err != nil {
		t.Fatal(err)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var completed *history.Event
	for i, event := range events {
		if event.WorkflowTaskFailed != nil {
			t.Fatalf("non-terminal completion was superseded: %+v", event.WorkflowTaskFailed)
		}
		if event.WorkflowTaskCompleted != nil && event.WorkflowTaskCompleted.ScheduledEventID == wftID {
			completed = &events[i]
		}
	}
	if completed == nil {
		t.Fatal("workflow task completion was not committed")
	}
	if seen := completed.WorkflowTaskCompleted.SeenThroughEventID; seen != signalID-1 {
		t.Fatalf("SeenThroughEventID = %d, want the WorkflowTaskStarted %d", seen, signalID-1)
	}
	batches := executor.SplitBatches(events)
	if len(batches) != 2 || !slices.ContainsFunc(batches[1], func(event history.Event) bool { return event.ID == signalID }) {
		t.Fatalf("late signal %d is not replayed in the next decision batch: %+v", signalID, batches)
	}
	next := openWorkflowTaskForTest(t, mgr, runID)
	if next == wftID {
		t.Fatalf("open workflow task = %d, want a new one for the late signal", next)
	}
}

// 审查 A3 第 4 条：Signal 的到达间隔短于 WFT 往返时，每次完成仍然提交，Run 持续前进，History 只多出每轮的
// Signal 与 WFT 事件。
func TestWorkflowTaskCompletion_SteadySignalsDoNotStarveWorkflowTasks(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-steady-signals", Program: programtest.Noop()})

	const rounds = 200
	for i := 0; i < rounds; i++ {
		wftID := openWorkflowTaskForTest(t, mgr, runID)
		startWorkflowTaskForTest(t, mgr, runID, wftID)
		// Worker 还在执行时，下一条 Signal 到达。
		if _, err := srv.SignalWorkflow(ctx, "", "", runID, "tick", "", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
		commands := []history.Command{{Type: history.CmdScheduleTimer, TimerID: fmt.Sprintf("t%d", i), Delay: time.Hour}}
		if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, "worker-1"); err != nil {
			t.Fatal(err)
		}
	}

	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	completed, failed, timers := 0, 0, 0
	for _, event := range events {
		switch {
		case event.WorkflowTaskCompleted != nil:
			completed++
		case event.WorkflowTaskFailed != nil:
			failed++
		case event.TimerScheduled != nil:
			timers++
		}
	}
	if completed != rounds || timers != rounds || failed != 0 {
		t.Fatalf("after %d rounds: %d completed, %d timers, %d failed workflow tasks; every completion should commit", rounds, completed, timers, failed)
	}
}
