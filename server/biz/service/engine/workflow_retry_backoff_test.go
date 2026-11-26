package engine

// 重试 Run 在首个 Workflow Task 的退避期内收到的交互，语义与 Temporal 一致：Signal 照常写入、不调度 Workflow Task，
// 退避到期时一并处理（signalworkflow 的 IsWorkflowPendingOnWorkflowTaskBackoff）；SignalWithStart、取消请求与 Update
// 立即调度 Workflow Task（SignalWithStart 对应 EnableSignalWithStartWorkflowTaskBackoff 默认关闭），Run 提前开始，之后的 Signal 照常调度，退避到期不再多调度一次（HadOrHasWorkflowTask）。

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

type retryBackoffEnv struct {
	server     *Server
	clk        *clocktest.Fake
	mgr        *memory.ExecutionManager
	workflowID string
	retryRunID string
}

// newRetryBackoffEnv 启动一个 Run 并让它失败，返回处于 1 小时退避期内的重试 Run。
func newRetryBackoffEnv(t *testing.T, workflowType string) *retryBackoffEnv {
	t.Helper()
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(), Type: workflowType,
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "1h", BackoffCoefficient: 2.0},
	}); err != nil {
		t.Fatal(err)
	}
	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", workflowType, workflowType, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "downstream is down"}, ""); err != nil {
		t.Fatal(err)
	}
	env := &retryBackoffEnv{server: server, clk: clk, mgr: mgr, workflowID: workflowType}
	env.retryRunID = onlyRetryRun(t, mgr, workflowType, run1ID)
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("retry run has %d workflow tasks before the backoff elapsed", got)
	}
	return env
}

func (e *retryBackoffEnv) count(t *testing.T, typ history.EventType) int {
	t.Helper()
	return countEngineEvents(mustEvents(t, e.mgr, e.retryRunID), typ)
}

func TestRetryBackoff_SignalWaitsForBackoff(t *testing.T) {
	env := newRetryBackoffEnv(t, "wf-backoff-signal")
	ctx := context.Background()

	if _, err := env.server.SignalWorkflow(ctx, "", env.workflowID, "", "poke", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := env.server.SignalWorkflow(ctx, "", env.workflowID, "", "poke", "req-2", nil); err != nil {
		t.Fatal(err)
	}
	if got := env.count(t, history.EventWorkflowSignaled); got != 2 {
		t.Fatalf("WorkflowSignaled = %d, want 2: signals are recorded during the backoff", got)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("signals during the 1h backoff scheduled %d workflow tasks, want 0", got)
	}
	// 恢复路径同样不提前调度。
	if _, err := env.server.DispatchPendingTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("reconcile during the backoff scheduled %d workflow tasks, want 0", got)
	}

	env.clk.Advance(time.Hour)
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("WorkflowTaskScheduled after the backoff = %d, want 1", got)
	}
}

func TestRetryBackoff_SignalWithStartStartsRunImmediately(t *testing.T) {
	env := newRetryBackoffEnv(t, "wf-backoff-signal-with-start")
	ctx := context.Background()

	runID, _, err := env.server.SignalWithStart(ctx, "", env.workflowID, env.workflowID, nil, "poke", "req-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if runID != env.retryRunID {
		t.Fatalf("SignalWithStart landed on %s, want the retry run %s", runID, env.retryRunID)
	}
	if got := env.count(t, history.EventWorkflowSignaled); got != 1 {
		t.Fatalf("WorkflowSignaled = %d, want 1", got)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("SignalWithStart during the backoff scheduled %d workflow tasks, want 1", got)
	}
	completeOpenWorkflowTask(t, env.mgr, env.retryRunID)
	env.clk.Advance(time.Hour)
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("WorkflowTaskScheduled after the backoff = %d, want 1 (no extra task once the run has started)", got)
	}
}

func TestRetryBackoff_CancelStartsRunImmediately(t *testing.T) {
	env := newRetryBackoffEnv(t, "wf-backoff-cancel")
	ctx := context.Background()

	if err := env.server.CancelWorkflow(ctx, "", env.workflowID, "", "stop"); err != nil {
		t.Fatal(err)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("cancel during the backoff scheduled %d workflow tasks, want 1", got)
	}
	// Run 已经开始：之后的 Signal 照常调度，退避到期不再多调度。
	completeOpenWorkflowTask(t, env.mgr, env.retryRunID)
	if _, err := env.server.SignalWorkflow(ctx, "", env.workflowID, "", "poke", "", nil); err != nil {
		t.Fatal(err)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 2 {
		t.Fatalf("signal after the run started early scheduled %d workflow tasks in total, want 2", got)
	}
	completeOpenWorkflowTask(t, env.mgr, env.retryRunID)
	env.clk.Advance(time.Hour)
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 2 {
		t.Fatalf("WorkflowTaskScheduled after the backoff = %d, want 2 (no extra task once the run has started)", got)
	}
}

func TestRetryBackoff_UpdateStartsRunImmediately(t *testing.T) {
	env := newRetryBackoffEnv(t, "wf-backoff-update")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// 没有 Worker 处理 Update，等待按 ctx 超时结束；这里只看调度。
	_, _ = env.server.UpdateWorkflow(ctx, "", env.workflowID, "", "set", "upd-1", protocol.UpdateWaitStageEnums.Completed, nil)
	if got := env.count(t, history.EventWorkflowUpdateAccepted); got != 1 {
		t.Fatalf("WorkflowUpdateAccepted = %d, want 1", got)
	}
	if got := env.count(t, history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("update during the backoff scheduled %d workflow tasks, want 1", got)
	}
}
