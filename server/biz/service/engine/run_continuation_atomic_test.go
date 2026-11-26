package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// 接续 Run 与终态事件同事务提交（continue-as-new、失败重试）
// ============================================
// 测试策略:
// - Error Case: 接续 Run 写不进去时 WFT 完成整体回滚：旧 Run 仍在运行，同 Workflow ID 的启动报冲突，
//   Signal 记在旧 Run 上（终态之前）；重做完成后接续 Run、旧 Run 的终态与 current 一起生效。
// - Error Case: 重试 Run 写不进去时同样整体回滚；重做后重试 Run 带 backoff timer，退避期内不调度首个 WFT。
// - Happy Path: 提交返回时旧 Run 行已是终态，current 指向接续 Run，没有中间窗口。
// ============================================

// continueAsNewForTest 用 CmdContinueAsNew 完成 runID 的 open WFT，返回接续 Run 的 ID。完成因新事实被 superseded 时
// 在重新调度的 WFT 上再完成一次。
func continueAsNewForTest(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string, input map[string]any) string {
	t.Helper()
	ctx := context.Background()
	for range 3 {
		wftID := openWorkflowTaskForTest(t, mgr, runID)
		if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID,
			[]historypkg.Command{{Type: historypkg.CmdContinueAsNew, Input: input}}, ""); err != nil {
			t.Fatal(err)
		}
		if next := continuedRunIDForTest(t, mgr, runID); next != "" {
			return next
		}
	}
	t.Fatalf("run %s did not continue as new", runID)
	return ""
}

func continuedRunIDForTest(t *testing.T, mgr execution.ExecutionManager, runID string) string {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowContinuedAsNew != nil {
			return event.WorkflowContinuedAsNew.NextRunID
		}
	}
	return ""
}

// assertNoEventAfterTerminal 断言 runID 的 History 在终态事件之后没有别的事件。
func assertNoEventAfterTerminal(t *testing.T, mgr execution.ExecutionManager, runID string) {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events {
		if historypkg.IsWorkflowTerminalEventType(event.EventType()) && i != len(events)-1 {
			t.Fatalf("run %s has %s after its terminal %s", runID, events[i+1].EventType(), event.EventType())
		}
	}
}

func eventTypes(events []historypkg.Event) []historypkg.EventType {
	types := make([]historypkg.EventType, len(events))
	for i, event := range events {
		types[i] = event.EventType()
	}
	return types
}

func assertRunRowStatus(t *testing.T, mgr execution.ExecutionManager, runID string, want historypkg.Status) {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != want {
		t.Fatalf("run %s row status = %s, want %s", runID, run.Status, want)
	}
}

func assertCurrentRun(t *testing.T, mgr execution.ExecutionManager, workflowID, want string) {
	t.Helper()
	run, err := mgr.Get(want)
	if err != nil {
		t.Fatal(err)
	}
	current, err := mgr.GetCurrentRunID(run.Namespace, workflowID)
	if err != nil || current != want {
		t.Fatalf("current run of %s = %q, %v; want %s", workflowID, current, err, want)
	}
}

// Error Case: 接续 Run 的写入失败时，WorkflowContinuedAsNew 不会单独落库。旧做法先提交终态事件、再在第二个事务里建
// 接续 Run，这个窗口里同 Workflow ID 的 StartWorkflow 抢走 current，Signal 被确认后记在终态事件之后丢失。
func TestContinueAsNewCommitsContinuedRunWithTerminalEvent(t *testing.T) {
	ctx := context.Background()
	const wfType = "wf-can-atomic"
	srv, mgr := newFailingServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: wfType, Program: programtest.Noop()})
	old, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	wftID := openWorkflowTaskForTest(t, mgr, runID)

	mgr.failOn(historypkg.EventWorkflowStarted)
	err = srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID,
		[]historypkg.Command{{Type: historypkg.CmdContinueAsNew, Input: map[string]any{"page": int64(2)}}}, "")
	mgr.failOn("")
	if err == nil {
		t.Fatal("expected the continued run write failure to be returned")
	}
	if next := continuedRunIDForTest(t, mgr, runID); next != "" {
		t.Fatalf("WorkflowContinuedAsNew committed without its continued run %s", next)
	}
	assertRunRowStatus(t, mgr, runID, historypkg.StatusRunning)
	if status, _ := srv.GetRunStatus(runID); status != historypkg.StatusRunning {
		t.Fatalf("run status after the rolled back completion = %s, want running", status)
	}

	// 旧 Run 仍在运行：启动报冲突，Signal 记在旧 Run 上并由下一次 WFT 看到。
	if stolen, err := srv.StartWorkflow(ctx, old.Namespace, wfType, old.WorkflowID, "", nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("StartWorkflow while the run is still running = %v, %v; want ErrCurrentRunConflict", stolen, err)
	}
	if _, err := srv.SignalWorkflow(ctx, old.Namespace, old.WorkflowID, "", "approve", "", map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	nextRunID := continueAsNewForTest(t, srv, mgr, runID, map[string]any{"page": int64(2)})
	assertRunRowStatus(t, mgr, runID, historypkg.StatusContinuedAsNew)
	assertRunRowStatus(t, mgr, nextRunID, historypkg.StatusRunning)
	assertCurrentRun(t, mgr, old.WorkflowID, nextRunID)
	assertNoEventAfterTerminal(t, mgr, runID)
	oldEvents, _ := mgr.GetEvents(runID)
	if countEngineEvents(oldEvents, historypkg.EventWorkflowSignaled) != 1 {
		t.Fatalf("expected the signal recorded on the old run before its terminal event, got %v", eventTypes(oldEvents))
	}
	nextEvents, _ := mgr.GetEvents(nextRunID)
	if countEngineEvents(nextEvents, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected the continued run's first workflow task scheduled, got %v", eventTypes(nextEvents))
	}

	// 接续之后的启动看到的是运行中的接续 Run。
	if _, err := srv.StartWorkflow(ctx, old.Namespace, wfType, old.WorkflowID, "", nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("StartWorkflow after continue-as-new: err = %v, want ErrCurrentRunConflict", err)
	}
}

// Error Case: 失败重试的重试 Run 写入失败时，WorkflowFailed 不会单独落库；重做后重试 Run、旧 Run 的 Failed 状态、
// current 与 backoff timer 同事务生效，退避期内不调度重试 Run 的首个 WFT。
func TestFailedWorkflowRetryCommitsRetryRunWithTerminalEvent(t *testing.T) {
	ctx := context.Background()
	const wfType = "wf-retry-atomic"
	srv, mgr := newFailingServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{
		Type: wfType, Program: programtest.Noop(),
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "1h"},
	})
	old, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	commands := []historypkg.Command{{Type: historypkg.CmdFailWorkflow, Error: "boom"}}
	wftID := openWorkflowTaskForTest(t, mgr, runID)

	mgr.failOn(historypkg.EventWorkflowStarted)
	err = srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, "")
	mgr.failOn("")
	if err == nil {
		t.Fatal("expected the retry run write failure to be returned")
	}
	events, _ := mgr.GetEvents(runID)
	if countEngineEvents(events, historypkg.EventWorkflowFailed) != 0 || countEngineEvents(events, historypkg.EventWorkflowTaskCompleted) != 0 {
		t.Fatalf("WorkflowFailed committed without its retry run: %v", eventTypes(events))
	}
	assertRunRowStatus(t, mgr, runID, historypkg.StatusRunning)
	if _, err := srv.StartWorkflow(ctx, old.Namespace, wfType, old.WorkflowID, "", nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("StartWorkflow while the run is still running: err = %v, want ErrCurrentRunConflict", err)
	}

	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, ""); err != nil {
		t.Fatal(err)
	}
	retryRunID := retryRunIDForTest(t, mgr, runID)
	assertRunRowStatus(t, mgr, runID, historypkg.StatusFailed)
	assertRunRowStatus(t, mgr, retryRunID, historypkg.StatusRunning)
	assertCurrentRun(t, mgr, old.WorkflowID, retryRunID)
	retryRun, _ := mgr.Get(retryRunID)
	if retryRun.Attempt != 2 || retryRun.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("unexpected retry run: attempt=%d initiator=%s", retryRun.Attempt, retryRun.Initiator)
	}
	timers, err := mgr.ListTimerStates(retryRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(timers) != 1 || timers[0].Kind != execution.TimerKindEnums.RetryBackoff {
		t.Fatalf("expected one retry backoff timer, got %+v", timers)
	}

	// 退避期内的周期补偿不提前调度重试 Run 的首个 WFT。
	if _, err := srv.DispatchPendingTasks(ctx); err != nil {
		t.Fatal(err)
	}
	retryHistory, _ := mgr.GetEvents(retryRunID)
	if countEngineEvents(retryHistory, historypkg.EventWorkflowTaskScheduled) != 0 {
		t.Fatalf("retry run must wait for its backoff timer, got history %v", eventTypes(retryHistory))
	}
	assertNoEventAfterTerminal(t, mgr, runID)
}
