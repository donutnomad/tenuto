package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 提交后动作失败的可恢复性与周期补偿（reconcileRun）
// ============================================
// 测试策略:
// - Error Case: WorkflowFailed 写入失败 → WFT 提交整体回滚，同一次完成可重试。
// - Error Case: 子 run 启动失败不中断其余命令；周期补偿补建子 run。
// - Error Case: 子终态向父传播失败 → 周期补偿经父的 open child state 补传播。
// - Edge Case: 事件已提交、WFT 未调度 → 周期补偿补调度；重试退避期内不提前调度。
// - Edge Case: 单个 run 补偿失败不影响其余 run；ctx 取消时停止扫描。
// - Edge Case: 超时处理在终态提交失败时保留定时器；Stop 取消待触发的 WFT 退避重调度。
// ============================================

func startReconcileTestRun(t *testing.T, srv *Server, wf *program.WorkflowConfig) string {
	t.Helper()
	ref, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(srv.StartWorkflow(context.Background(), "", ref.WorkflowType, ref.WorkflowType, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	return runID
}

func openWorkflowTaskForTest(t *testing.T, mgr execution.ExecutionManager, runID string) int64 {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	id := openWorkflowTaskScheduledIDInEvents(events)
	if id == 0 {
		t.Fatalf("run %s has no open workflow task", runID)
	}
	return id
}

func retryRunIDForTest(t *testing.T, mgr execution.ExecutionManager, runID string) string {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowFailed != nil {
			return event.WorkflowFailed.NewExecutionRunID
		}
	}
	t.Fatalf("run %s has no WorkflowFailed event", runID)
	return ""
}

// Error Case: WorkflowFailed 写不进去时 WFT 完成一起回滚，同一次完成重试后成功。
func TestFailWorkflowCommand_TerminalAppendFailureRollsBackWorkflowTask(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newFailingServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-fail-terminal-append-error", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	commands := []historypkg.Command{{Type: historypkg.CmdFailWorkflow, Error: "boom"}}

	mgr.failOn(historypkg.EventWorkflowFailed)
	err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, "")
	mgr.failOn("")
	if err == nil {
		t.Fatal("expected the terminal append failure to be returned")
	}
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventWorkflowTaskCompleted) != 0 {
		t.Fatalf("workflow task must stay open when its terminal event failed, got %+v", history)
	}

	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, ""); err != nil {
		t.Fatal(err)
	}
	if stored, _ := mgr.Get(runID); stored.Status != historypkg.StatusFailed {
		t.Fatalf("expected run failed after retrying the completion, got %s", stored.Status)
	}
}

// Error Case: 子 run 启动失败不中断同批其余命令；周期补偿按 ChildWorkflowState 补建子 run。
func TestWorkflowTaskPostCommit_ChildStartFailureKeepsRemainingCommands(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newFailingServerForTest(t)
	childRef, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "child-start-error", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "parent-child-start-error", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)

	mgr.failOn(historypkg.EventWorkflowStarted)
	err = srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{
		{Type: historypkg.CmdStartChildWorkflow, ChildID: "child_1", WorkflowType: childRef.WorkflowType},
		{Type: historypkg.CmdScheduleTimer, TimerID: "sleep-1", Delay: time.Hour},
	}, "")
	mgr.failOn("")
	if err != nil {
		t.Fatalf("committed workflow task must not be reported as failed: %v", err)
	}
	armedTimers := timerCount(srv, timerDurable)
	if armedTimers != 1 {
		t.Fatalf("expected the timer command after the failed child start to be processed, got %d timers", armedTimers)
	}
	states, _ := mgr.ListChildWorkflowStates(runID)
	if len(states) != 1 || srv.runExists(states[0].ChildRunID) {
		t.Fatalf("expected one child state without a child run, got %+v", states)
	}

	if _, err := srv.DispatchPendingTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if !srv.runExists(states[0].ChildRunID) {
		t.Fatal("expected reconcile to start the child run")
	}
}

// Error Case: 子终态向父传播失败 → 周期补偿经父的 open ChildWorkflowState 补传播并调度父 WFT。
func TestReconcilePropagatesClosedChildToParent(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newFailingServerForTest(t)
	parentRunID, scheduledEventID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-reconcile-propagate")

	mgr.failOn(historypkg.EventChildWorkflowCompleted)
	err := srv.completeWorkflow(childRunID, map[string]any{"ok": true})
	mgr.failOn("")
	if err == nil {
		t.Fatal("expected the propagation failure to be returned")
	}

	if _, err := srv.DispatchPendingTasks(ctx); err != nil {
		t.Fatal(err)
	}
	state, _, _ := mgr.GetChildWorkflowState(parentRunID, scheduledEventID)
	if state.Status != execution.ChildWorkflowStatusEnums.Completed {
		t.Fatalf("expected parent child state completed, got %q", state.Status)
	}
	parentHistory, _ := mgr.GetEvents(parentRunID)
	if countEngineEvents(parentHistory, historypkg.EventChildWorkflowCompleted) != 1 {
		t.Fatalf("expected one ChildWorkflowCompleted on the parent, got %+v", parentHistory)
	}
	if openWorkflowTaskScheduledIDInEvents(parentHistory) == 0 {
		t.Fatal("expected a parent workflow task after the recovered child completion")
	}
}

// Edge Case: 事件已提交但 WFT 没调度出去（提交后调度失败）→ 周期补偿补调度并投递。
func TestReconcileSchedulesWorkflowTaskForUnactivatedEvents(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	srv := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	t.Cleanup(srv.Stop)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-reconcile-activation", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, runID, runID, historypkg.NewWorkflowSignaledEvent("go", nil)); err != nil {
		t.Fatal(err)
	}
	queued := len(workflowTasks.snapshot())

	result, err := srv.DispatchPendingTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	history, _ := mgr.GetEvents(runID)
	if openWorkflowTaskScheduledIDInEvents(history) == 0 {
		t.Fatalf("expected a workflow task for the unactivated signal, got %+v", history)
	}
	if len(workflowTasks.snapshot()) != queued+1 {
		t.Fatalf("expected one workflow task enqueued, got %d new", len(workflowTasks.snapshot())-queued)
	}

	// 幂等：再跑一轮不产生第二个 WFT。
	if _, err := srv.DispatchPendingTasks(ctx); err != nil {
		t.Fatal(err)
	}
	history, _ = mgr.GetEvents(runID)
	if got := countEngineEvents(history, historypkg.EventWorkflowTaskScheduled); got != 2 {
		t.Fatalf("expected 2 workflow tasks in total, got %d (first round result %+v)", got, result)
	}
}

// Edge Case: 一个 run 补偿失败只计入返回的 error，其余 run 照常补偿；已终态的 run 不参与扫描。
func TestReconcileRunningRuns_OneFailingRunDoesNotStopOthers(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	srv := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	t.Cleanup(srv.Stop)

	now := time.Now()
	// 先创建的坏 run：open child state 指向未注册的 workflow，补建子 run 必然失败。
	bad := &execution.Run{ID: "run-reconcile-bad", WorkflowID: "wf-bad", WorkflowType: "wf-bad", Status: historypkg.StatusRunning, CreatedAt: now}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(bad)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(bad.ID, bad.ID, bad.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	wftID := appendOpenWorkflowTaskForTest(t, mgr, bad.ID)
	if _, _, err := mgr.AppendWorkflowTaskClosed(bad.ID, bad.ID, bad.ID, historypkg.NewWorkflowTaskCompletedEvent(wftID, "")); err != nil {
		t.Fatal(err)
	}
	if err := mgr.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID: bad.ID, ScheduledEventID: 99, ChildID: "child_1", ChildRunID: "run-missing-child",
		WorkflowType: "wf-not-registered", Status: execution.ChildWorkflowStatusEnums.Initiated,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	good := &execution.Run{ID: "run-reconcile-good", WorkflowID: "wf-good", WorkflowType: "wf-good", Status: historypkg.StatusRunning, CreatedAt: now.Add(time.Millisecond)}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(good)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(good.ID, good.ID, good.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	goodWFT := appendOpenWorkflowTaskForTest(t, mgr, good.ID)
	closed := &execution.Run{ID: "run-reconcile-closed", WorkflowID: "wf-closed", WorkflowType: "wf-closed", Status: historypkg.StatusRunning, CreatedAt: now.Add(2 * time.Millisecond)}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(closed)) }); err != nil {
		t.Fatal(err)
	}
	appendOpenWorkflowTaskForTest(t, mgr, closed.ID)
	markRunTerminalForTest(t, mgr, closed.ID, historypkg.StatusCompleted, nil)

	result, err := srv.DispatchPendingTasks(ctx)
	if err == nil || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("expected an error naming the failing run, got %v", err)
	}
	if strings.Contains(err.Error(), good.ID) {
		t.Fatalf("healthy run must not be reported: %v", err)
	}
	if result.WorkflowTasks != 1 {
		t.Fatalf("expected only the healthy run's workflow task dispatched, got %+v", result)
	}
	tasks := workflowTasks.snapshot()
	if len(tasks) != 1 || tasks[0].Task.RunID != good.ID || tasks[0].Task.ScheduledEventID != goodWFT {
		t.Fatalf("unexpected dispatched tasks: %+v", tasks)
	}

	// 启动恢复走同一条路径：坏 run 的错误汇总返回，不中断。
	if err := srv.RecoverRunningWorkflows(ctx); err == nil || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("expected startup recovery to report the failing run, got %v", err)
	}
}

// Edge Case: ctx 已取消时扫描立即停止并返回 ctx 错误。
func TestReconcileRunningRuns_StopsOnContextCancel(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	srv := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	t.Cleanup(srv.Stop)
	run := &execution.Run{ID: "run-reconcile-cancelled", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := srv.DispatchPendingTasks(ctx); err == nil {
		t.Fatal("expected the context error")
	}
	if len(workflowTasks.snapshot()) != 0 {
		t.Fatal("no task may be dispatched after cancellation")
	}
}

// Edge Case: 超时处理的终态提交失败时保留进程内定时器，提交成功后才释放。
func TestRunTimeoutReleasesTimersOnlyAfterTerminalCommit(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newFailingServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-timeout-release-order", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{{Type: historypkg.CmdScheduleTimer, TimerID: "sleep-1", Delay: time.Hour}}, ""); err != nil {
		t.Fatal(err)
	}
	armedTimers := func() int { return timerCount(srv, timerDurable) }

	mgr.failOn(historypkg.EventWorkflowFailed)
	srv.handleRunTimeout(runID)
	mgr.failOn("")
	if armedTimers() != 1 {
		t.Fatal("timers must stay armed while the terminal event is not committed")
	}

	srv.handleRunTimeout(runID)
	if armedTimers() != 0 {
		t.Fatal("expected timers released after the terminal commit")
	}
	history, _ := mgr.GetEvents(runID)
	if countEngineEvents(history, historypkg.EventWorkflowFailed) != 1 {
		t.Fatalf("expected one WorkflowFailed, got %+v", history)
	}
}

// Edge Case: Stop 取消待触发的 WFT 退避重调度，停机后不再写 history。
func TestStopCancelsPendingWorkflowTaskReschedule(t *testing.T) {
	srv, mgr := newServerForTest(t)
	run := &execution.Run{ID: "run-stop-wft-backoff", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	srv.scheduleWorkflowTaskAfter(context.Background(), run.ID, 20*time.Millisecond)
	srv.Stop()
	time.Sleep(80 * time.Millisecond)

	history, _ := mgr.GetEvents(run.ID)
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 0 {
		t.Fatalf("expected no workflow task scheduled after Stop, got %+v", history)
	}
}

// Happy Path: 不重试的失败命令在同一次提交后落 Failed 终态并释放 durable timer。
func TestFailWorkflowCommand_WithoutRetryMarksFailed(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "wf-fail-no-retry", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	failure := protocol.Failure{Message: "boom", Type: "Fatal"}

	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{
		{Type: historypkg.CmdScheduleTimer, TimerID: "sleep-1", Delay: time.Hour},
		{Type: historypkg.CmdFailWorkflow, Error: failure.Message, WorkflowFailure: &failure},
	}, ""); err != nil {
		t.Fatal(err)
	}
	if stored, _ := mgr.Get(runID); stored.Status != historypkg.StatusFailed {
		t.Fatalf("expected run failed, got %s", stored.Status)
	}
	if retryRunIDForTest(t, mgr, runID) != "" {
		t.Fatal("no retry run expected without a retry policy")
	}
	armedTimers := timerCount(srv, timerDurable)
	if armedTimers != 0 {
		t.Fatalf("expected durable timers released on failure, got %d", armedTimers)
	}
}
