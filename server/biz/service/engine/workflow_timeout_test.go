package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// WorkflowRun Timeout + ExecutionTimeout 正确性测试
// ============================================

// Happy Path: execution timeout 跨代不重置
// continue-as-new 后新 run 的 DeadlineAt == 旧 run 的 DeadlineAt（不是 now+timeout）
func TestExecutionTimeoutNotResetOnContinueAsNew(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		Type:             "wf-exec-timeout-can",
		ExecutionTimeout: "500ms",
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowID, ref.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// DeadlineAt 随 Run 一起写入
	run1, err := mgr.Get(run1ID)
	if err != nil {
		t.Fatal(err)
	}
	run1DeadlineAt := run1.DeadlineAt
	if run1DeadlineAt == nil || !run1DeadlineAt.Equal(testClockStart.Add(500*time.Millisecond)) {
		t.Fatalf("run1.DeadlineAt = %v, want start+500ms", run1DeadlineAt)
	}

	// 时间前进后 continue-as-new：按新时刻重算会得到不同的 deadline。
	clk.Advance(200 * time.Millisecond)
	// 创建 continue-as-new run，继承旧 run 的 DeadlineAt
	run2ID := execution.NewID()
	err = server.startContinuedRunWithRef(ctx, run1ID, run2ID, nil, definition.WorkflowConfigRef{
		Namespace:  ref.Namespace,
		WorkflowID: ref.WorkflowID,
		Version:    ref.Version,
	})
	if err != nil {
		t.Fatal(err)
	}

	run2, err := mgr.Get(run2ID)
	if err != nil {
		t.Fatal(err)
	}
	if run2 == nil {
		t.Fatal("run2 should exist")
	}
	if run2.DeadlineAt == nil {
		t.Fatal("run2.DeadlineAt should be inherited from run1")
	}
	// 关键断言：跨代继承，不重算
	if !run2.DeadlineAt.Equal(*run1DeadlineAt) {
		t.Fatalf("run2.DeadlineAt should equal run1.DeadlineAt (cross-run inheritance), got run1=%v run2=%v",
			run1DeadlineAt, run2.DeadlineAt)
	}
}

// Happy Path: run timeout 每代重置
// continue-as-new 后新 run 的 RunDeadlineAt 从新 run 启动时起算（不继承旧代）。假时钟推进两代之间的时间。
func TestRunTimeoutResetsOnContinueAsNew(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:    programtest.Noop(),
		Type:       "wf-run-timeout-reset",
		RunTimeout: "1m",
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowID, ref.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	run1, err := mgr.Get(run1ID)
	if err != nil {
		t.Fatal(err)
	}
	if run1.RunDeadlineAt == nil || !run1.RunDeadlineAt.Equal(testClockStart.Add(time.Minute)) {
		t.Fatalf("run1.RunDeadlineAt = %v, want %v", run1.RunDeadlineAt, testClockStart.Add(time.Minute))
	}

	clk.Advance(20 * time.Second)
	run2ID := execution.NewID()
	err = server.startContinuedRunWithRef(ctx, run1ID, run2ID, nil, definition.WorkflowConfigRef{
		Namespace:  ref.Namespace,
		WorkflowID: ref.WorkflowID,
		Version:    ref.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	run2, err := mgr.Get(run2ID)
	if err != nil {
		t.Fatal(err)
	}
	want := testClockStart.Add(20*time.Second + time.Minute)
	if run2.RunDeadlineAt == nil || !run2.RunDeadlineAt.Equal(want) {
		t.Fatalf("run2.RunDeadlineAt = %v, want %v (reset per run)", run2.RunDeadlineAt, want)
	}
	// run1 的 deadline 过去时 run2 仍在运行，run2 自己的 deadline 到期才失败。
	clk.Advance(40 * time.Second)
	if status, _ := server.GetRunStatus(run2ID); status != history.StatusRunning {
		t.Fatalf("run2 status at run1's deadline = %s, want running", status)
	}
	clk.Advance(20 * time.Second)
	waitForRunStatusWithinTest(t, mgr, run2ID, history.StatusFailed, 3*time.Second)
}

// failedTimeoutType 返回 run 的 WorkflowFailed 事件记录的超时类型。
func failedTimeoutType(t *testing.T, mgr execution.ExecutionManager, runID string) string {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.WorkflowFailed != nil {
			return ev.WorkflowFailed.TimeoutType
		}
	}
	t.Fatal("expected WorkflowFailed event")
	return ""
}

// Happy Path: run timeout 到期触发单代失败，TimeoutType = "Run"；到期之前 run 保持运行。
func TestRunTimeoutTriggersFailure(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:    programtest.Noop(),
		Type:       "wf-run-timeout-fail",
		RunTimeout: "30s",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-run-timeout-fail", "wf-run-timeout-fail", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(30*time.Second - time.Millisecond)
	if status, _ := server.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("status before the run timeout = %s, want running", status)
	}
	clk.Advance(time.Millisecond)
	waitForEngineEventCount(t, mgr, runID, history.EventWorkflowFailed, 1, 3*time.Second)
	if got := failedTimeoutType(t, mgr, runID); got != TimeoutTypeRun {
		t.Fatalf("expected TimeoutType=%q, got %q", TimeoutTypeRun, got)
	}
}

// Happy Path: execution + run 共存，run 先超时
func TestBothTimeoutsRunTimeoutWins(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		Type:             "wf-both-timeouts",
		ExecutionTimeout: "10m",
		RunTimeout:       "1m",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-both-timeouts", "wf-both-timeouts", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	waitForEngineEventCount(t, mgr, runID, history.EventWorkflowFailed, 1, 3*time.Second)
	if got := failedTimeoutType(t, mgr, runID); got != TimeoutTypeRun {
		t.Fatalf("expected run timeout to win, got TimeoutType=%q", got)
	}
}

// Happy Path: execution timeout 到期，TimeoutType = "Execution"
func TestExecutionTimeoutTriggersFailure(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-exec-timeout-fail", ExecutionTimeout: "2m", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-exec-timeout-fail", "wf-exec-timeout-fail", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2*time.Minute - time.Millisecond)
	if status, _ := server.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("status before the execution timeout = %s, want running", status)
	}
	clk.Advance(time.Millisecond)
	waitForEngineEventCount(t, mgr, runID, history.EventWorkflowFailed, 1, 3*time.Second)
	if got := failedTimeoutType(t, mgr, runID); got != TimeoutTypeExecution {
		t.Fatalf("expected TimeoutType=%q, got %q", TimeoutTypeExecution, got)
	}
}

// Edge Case: 都没设，永不超时
func TestNoTimeoutConfigured(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		Type:    "wf-no-timeout",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-no-timeout", "wf-no-timeout", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	clk.Advance(24 * time.Hour)

	status, err := server.GetRunStatus(runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != history.StatusRunning {
		t.Fatalf("expected running (no timeout configured), got %s", status)
	}

	// 清理
	_ = server.failWorkflow(runID, protocol.Failure{Message: "cleanup"}, "")
}

// Happy Path: 正常完成后 run 与 workflow 执行超时 timer 都停掉（不泄漏）
func TestTimersCleanedUpOnCompletion(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		Type:             "wf-timer-cleanup",
		ExecutionTimeout: "10s",
		RunTimeout:       "10s",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-timer-cleanup", "wf-timer-cleanup", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 启动时 armRunTimers 已排定两个 timer
	if !timerArmed(server, timerRef{Kind: timerWorkflowExecution, RunID: runID}) {
		t.Fatal("expected the workflow execution timer armed before completion")
	}
	if !timerArmed(server, timerRef{Kind: timerWorkflowRun, RunID: runID}) {
		t.Fatal("expected the run timer armed before completion")
	}
	pendingBefore := clk.Pending()

	// 提交 WorkflowCompleted 并走完成后的动作
	if err := server.completeWorkflow(runID, nil); err != nil {
		t.Fatal(err)
	}

	// 验证 timers 已清理
	hasWfAfter := timerArmed(server, timerRef{Kind: timerWorkflowExecution, RunID: runID})
	hasRunAfter := timerArmed(server, timerRef{Kind: timerWorkflowRun, RunID: runID})

	if hasWfAfter {
		t.Fatal("workflow execution timer should be stopped after completion")
	}
	if hasRunAfter {
		t.Fatal("run timer should be stopped after completion")
	}
	// 时钟上的定时器也已停掉，不只是从 timer 表摘除。
	if got := clk.Pending(); got > pendingBefore-2 {
		t.Fatalf("clock timers pending = %d after completion, want at most %d", got, pendingBefore-2)
	}
}

// Error Case: run_timeout > execution_timeout 被 reject
func TestRunTimeoutGreaterThanExecutionTimeoutReject(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		Type:             "wf-reject-timeout",
		ExecutionTimeout: "10s",
		RunTimeout:       "20s",
	})

	if err == nil {
		t.Fatal("expected error when run_timeout > execution_timeout, got nil")
	}
}

// Happy Path: WorkflowFailedEvent 有正确的 TimeoutType（直接调 failWorkflow）
func TestTimeoutTypeInFailedEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		Type:    "wf-timeout-type",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-timeout-type", "wf-timeout-type", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.failWorkflow(runID, protocol.Failure{Message: "run timeout"}, TimeoutTypeRun); err != nil {
		t.Fatal(err)
	}

	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.WorkflowFailed != nil {
			if ev.WorkflowFailed.TimeoutType != TimeoutTypeRun {
				t.Fatalf("expected TimeoutType=%q, got %q", TimeoutTypeRun, ev.WorkflowFailed.TimeoutType)
			}
			return
		}
	}
	t.Fatal("expected WorkflowFailed event with TimeoutType")
}
