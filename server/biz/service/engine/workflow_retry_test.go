package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Workflow Retry 地基测试（阶段 0+1）
// ============================================
// TDD：先写测试（红），再实施（绿）。
// 对标 Temporal SetupNewWorkflowForRetryOrCron。
// Server 用假时钟：backoff 与超时只在 clk.Advance 时到期，到期回调在 Advance 内同步执行。
// ============================================

// 1. Happy Path: CmdFailWorkflow + RetryPolicy(max=3) → 开新 run，attempt=2，Initiator=Retry
func TestWorkflowRetry_BasicRetry(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-basic",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    "100ms",
			BackoffCoefficient: 2.0,
			MaxInterval:        "1s",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowID, ref.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// run1 应该 attempt=1
	run1, err := mgr.Get(run1ID)
	if err != nil || run1 == nil {
		t.Fatalf("get run1: %v", err)
	}
	if run1.Attempt != 1 {
		t.Fatalf("expected run1.Attempt=1, got %d", run1.Attempt)
	}

	// 触发失败（相当于 CmdFailWorkflow → AppendWorkflowFailed 路径）；重试 run 与 WorkflowFailed 同步提交。
	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient error"}, ""); err != nil {
		t.Fatal(err)
	}
	run2ID := onlyRetryRun(t, mgr, ref.WorkflowID, run1ID)

	run2, err := mgr.Get(run2ID)
	if err != nil || run2 == nil {
		t.Fatalf("get run2: %v", err)
	}
	if run2.Status != history.StatusRunning {
		t.Fatalf("expected run2 running, got %s", run2.Status)
	}
	if run2.Attempt != 2 {
		t.Fatalf("expected run2.Attempt=2, got %d", run2.Attempt)
	}
	if run2.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("expected run2.Initiator=Retry, got %q", run2.Initiator)
	}
	if run2.ContinuedRunID != run1ID {
		t.Fatalf("expected run2.ContinuedRunID=%q, got %q", run1ID, run2.ContinuedRunID)
	}
	if run2.FirstRunID != run1.FirstRunID {
		t.Fatalf("expected run2.FirstRunID=%q, got %q", run1.FirstRunID, run2.FirstRunID)
	}
}

// 2. Happy Path: 重试的新 run 首个 WFT 延迟了 backoff 时长（不是立即跑）
func TestWorkflowRetry_BackoffDelay(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-backoff",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    "200ms",
			BackoffCoefficient: 1.0,
			MaxInterval:        "1s",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-backoff", "wf-retry-backoff", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient"}, ""); err != nil {
		t.Fatal(err)
	}
	run2ID := onlyRetryRun(t, mgr, "wf-retry-backoff", run1ID)

	// backoff 到期前 run2 只有 WorkflowStarted，没有 WorkflowTaskScheduled。
	clk.Advance(200*time.Millisecond - time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("expected no WFT scheduled during backoff period, got %d", got)
	}

	// backoff 到期，首个 WFT 被调度。
	clk.Advance(time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("expected WFT to be scheduled after backoff period, got %d", got)
	}
}

// 3. Happy Path: 达到 MaxAttempts → 不再重试，落终态失败
func TestWorkflowRetry_MaxAttempts(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-maxattempts",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:     2,
			InitialInterval: "50ms",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-maxattempts", "wf-retry-maxattempts", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 第 1 次失败 → 应该 retry（attempt 1→2）
	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "err"}, ""); err != nil {
		t.Fatal(err)
	}
	run2ID := onlyRetryRun(t, mgr, "wf-retry-maxattempts", run1ID)

	// backoff 结束，run2 的 WFT 出现
	clk.Advance(50 * time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("run2 WorkflowTaskScheduled = %d after backoff, want 1", got)
	}

	// 第 2 次失败（MaxAttempts=2）→ 不再重试，run2 终态失败
	if err := server.failWorkflow(run2ID, protocol.Failure{Message: "err2"}, ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, mgr, run2ID, history.StatusFailed)

	// 推进到任何 backoff 之后，仍不应有 run3
	clk.Advance(time.Hour)
	assertNoRetryRun(t, mgr, "wf-retry-maxattempts", run1ID, run2ID)
}

// 4. Error Case: NonRetryable 错误 → 不重试，直接失败
func TestWorkflowRetry_NonRetryable(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-nonretryable",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "50ms",
			NonRetryableErrors: []string{"fatal-error"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-nonretryable", "wf-retry-nonretryable", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 触发 NonRetryable 错误
	if err := server.failWorkflow(runID, protocol.Failure{Message: "fatal-error: bad input"}, ""); err != nil {
		t.Fatal(err)
	}

	// run 应该终态失败，不重试
	requireRunStatus(t, mgr, runID, history.StatusFailed)
	clk.Advance(time.Hour)
	assertNoRetryRun(t, mgr, "wf-retry-nonretryable", runID)
}

// 5. Happy Path: run timeout 触发重试（对标 Temporal）
func TestWorkflowRetry_RunTimeoutTriggerRetry(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:    programtest.Noop(),
		ID:         "wf-retry-runtimeout",
		RunTimeout: "80ms",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    "50ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-runtimeout", "wf-retry-runtimeout", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	clk.Advance(80*time.Millisecond - time.Millisecond)
	assertNoRetryRun(t, mgr, "wf-retry-runtimeout", run1ID)

	// run1 超时触发 retry
	clk.Advance(time.Millisecond)
	run2ID := onlyRetryRun(t, mgr, "wf-retry-runtimeout", run1ID)

	run2, err := mgr.Get(run2ID)
	if err != nil || run2 == nil {
		t.Fatal("get run2 failed")
	}
	if run2.Attempt != 2 {
		t.Fatalf("expected run2.Attempt=2, got %d", run2.Attempt)
	}
	if run2.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("expected run2.Initiator=Retry, got %q", run2.Initiator)
	}
}

// 6. Error Case: execution timeout 总闸 → 不重试（即使 attempt 没用完）
func TestWorkflowRetry_ExecutionTimeoutNoRetry(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		ID:               "wf-retry-execttimeout",
		ExecutionTimeout: "100ms",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        10,
			InitialInterval:    "10ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-execttimeout", "wf-retry-execttimeout", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// execution timeout 触发
	clk.Advance(100 * time.Millisecond)
	requireRunStatus(t, mgr, runID, history.StatusFailed)

	// execution timeout 不重试
	clk.Advance(time.Hour)
	for _, r := range workflowRunsForTest(t, mgr, "wf-retry-execttimeout") {
		if r.Status == history.StatusRunning {
			t.Fatalf("expected no retry on execution timeout, but found running run %s", r.ID)
		}
	}
}

// 7. Edge Case: backoff 后超过 execution deadline → 不重试，RetryState=Timeout
func TestWorkflowRetry_BackoffExceedsDeadline(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		ID:               "wf-retry-backoff-deadline",
		ExecutionTimeout: "300ms",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "500ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-backoff-deadline", "wf-retry-backoff-deadline", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if run, _ := mgr.Get(runID); run == nil || run.DeadlineAt == nil {
		t.Fatal("run.DeadlineAt should be set at start")
	}

	// 触发失败，此时 backoff(500ms) > remaining deadline → 不重试
	if err := server.failWorkflow(runID, protocol.Failure{Message: "transient"}, ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, mgr, runID, history.StatusFailed)

	// 不应有新 run
	clk.Advance(time.Hour)
	for _, r := range workflowRunsForTest(t, mgr, "wf-retry-backoff-deadline") {
		if r.ID != runID {
			t.Fatalf("expected no retry when backoff exceeds deadline, found run %s", r.ID)
		}
	}
}

// 8. Happy Path: 重试链上每代新 run 的 DeadlineAt == 第一代（绝对 deadline 透传）
func TestWorkflowRetry_DeadlineInheritedAcrossRetries(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:          programtest.Noop(),
		ID:               "wf-retry-deadline-inherit",
		ExecutionTimeout: "10s",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "50ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-deadline-inherit", "wf-retry-deadline-inherit", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	run1, _ := mgr.Get(run1ID)
	if run1 == nil || run1.DeadlineAt == nil {
		t.Fatal("run1.DeadlineAt should be set")
	}
	run1Deadline := *run1.DeadlineAt

	// 时间前进后再失败：继承的是第一代的绝对 deadline，不是按失败时刻重算。
	clk.Advance(time.Second)
	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient"}, ""); err != nil {
		t.Fatal(err)
	}
	run2ID := onlyRetryRun(t, mgr, "wf-retry-deadline-inherit", run1ID)

	run2, _ := mgr.Get(run2ID)
	if run2 == nil || run2.DeadlineAt == nil {
		t.Fatal("run2.DeadlineAt should be set")
	}
	if !run2.DeadlineAt.Equal(run1Deadline) {
		t.Fatalf("run2.DeadlineAt should equal run1.DeadlineAt (inherited), got run1=%v run2=%v",
			run1Deadline, run2.DeadlineAt)
	}
	// RunDeadlineAt 不继承（nil）
	if run2.RunDeadlineAt != nil {
		t.Fatalf("run2.RunDeadlineAt should be nil (reset per run), got %v", run2.RunDeadlineAt)
	}
}

// 9. Happy Path: 崩溃恢复 — 重试的"延迟首 WFT" timer，重启后续上
func TestWorkflowRetry_CrashRecovery(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	clk := clocktest.NewFake(testClockStart)
	server := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	defer server.Stop()

	retryPolicy := &program.RetryPolicy{
		MaxAttempts:        3,
		InitialInterval:    "300ms", // 长一点，方便模拟崩溃窗口
		BackoffCoefficient: 1.0,
	}
	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-retry-crash-recovery", RetryPolicy: retryPolicy, Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}

	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-crash-recovery", "wf-retry-crash-recovery", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 触发失败，开始 retry backoff；run2 已建，WFT 还没调度（backoff=300ms）
	if err := server.failWorkflow(run1ID, protocol.Failure{Message: "transient"}, ""); err != nil {
		t.Fatal(err)
	}
	run2ID := onlyRetryRun(t, mgr, "wf-retry-crash-recovery", run1ID)

	// 验证 run2 的 retry timer state 已持久化
	timerStates, err := mgr.ListTimerStates(run2ID)
	if err != nil {
		t.Fatal(err)
	}
	var retryTimer *execution.TimerState
	for _, st := range timerStates {
		if st.Kind == execution.TimerKindEnums.RetryBackoff {
			retryTimer = st
			break
		}
	}
	if retryTimer == nil {
		t.Fatal("expected RetryBackoff timer state for run2")
	}
	if retryTimer.IsTerminal() {
		t.Fatal("retry timer should not be terminal yet")
	}

	// 模拟崩溃：停止老 server，它的内存 timer 随之丢失；崩溃窗口内时间照常流逝。
	server.Stop()
	clk.Advance(100 * time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("WFT scheduled by a stopped server: %d", got)
	}

	// 建新 server（同一个 mgr、同一个时钟），触发崩溃恢复
	server2 := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	defer server2.Stop()
	if _, err := server2.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-retry-crash-recovery", RetryPolicy: retryPolicy, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if err := server2.RecoverRunningWorkflows(ctx); err != nil {
		t.Fatal(err)
	}

	// 恢复的 timer 沿用持久化的截止时间：剩余 200ms 到期前不调度，到期时调度。
	clk.Advance(200*time.Millisecond - time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 0 {
		t.Fatalf("recovered retry timer fired before its deadline: %d", got)
	}
	clk.Advance(time.Millisecond)
	if got := countEngineEvents(mustEvents(t, mgr, run2ID), history.EventWorkflowTaskScheduled); got != 1 {
		t.Fatalf("WorkflowTaskScheduled = %d after recovered backoff, want 1", got)
	}
}

// 10. Edge Case: 并发幂等 — 两路失败同时触发，只开一个新 run
func TestWorkflowRetry_ConcurrentIdempotent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-concurrent",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "50ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-concurrent", "wf-retry-concurrent", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 并发触发两次失败；两路都返回后重试 run 已提交
	done := make(chan error, 2)
	go func() { done <- server.failWorkflow(runID, protocol.Failure{Message: "err1"}, "") }()
	go func() { done <- server.failWorkflow(runID, protocol.Failure{Message: "err2"}, "") }()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	// 恰好一个新 run
	onlyRetryRun(t, mgr, "wf-retry-concurrent", runID)
}

// 11. Happy Path: 不配 RetryPolicy → 失败直接终态（现状不回退）
func TestWorkflowRetry_NoRetryPolicy(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-nopolicy",
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "wf-retry-nopolicy", "wf-retry-nopolicy", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.failWorkflow(runID, protocol.Failure{Message: "error"}, ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, mgr, runID, history.StatusFailed)

	clk.Advance(time.Hour)
	assertNoRetryRun(t, mgr, "wf-retry-nopolicy", runID)
}

// 12. Edge Case: cancel/terminate 不重试（即使配了 RetryPolicy）
func TestWorkflowRetry_CancelNoRetry(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-retry-cancel",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "50ms",
			BackoffCoefficient: 1.0,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "wf-retry-cancel", "wf-retry-cancel", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	// 直接提交 cancel 终态并走它的后续动作：验证 cancel 路径不走 prepareWorkflowFailure（核心断言）
	// CancelWorkflow 只发送 request 事件，需要 worker 响应才真正 cancel，测试无 worker，
	// 故直接使用包内方法触发 cancel 终态。
	run, _, err := server.commitTerminalForTest(runID, history.NewWorkflowCancelledEvent(nil), history.StatusCancelled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.workflowCancelledAfterCommit(run); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, mgr, runID, history.StatusCancelled)

	clk.Advance(time.Hour)
	assertNoRetryRun(t, mgr, "wf-retry-cancel", runID)
}

// retryRuns 返回 workflowID 下 excluded 之外的 run ID。
func retryRuns(t *testing.T, mgr execution.ExecutionManager, workflowID string, excluded ...string) []string {
	t.Helper()
	var ids []string
	for _, r := range workflowRunsForTest(t, mgr, workflowID) {
		if !slices.Contains(excluded, r.ID) {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// onlyRetryRun 断言 workflowID 下除 excluded 外恰有一个 run，并返回它。
func onlyRetryRun(t *testing.T, mgr execution.ExecutionManager, workflowID string, excluded ...string) string {
	t.Helper()
	ids := retryRuns(t, mgr, workflowID, excluded...)
	if len(ids) != 1 {
		t.Fatalf("retry runs of %s = %v, want exactly one", workflowID, ids)
	}
	return ids[0]
}

func assertNoRetryRun(t *testing.T, mgr execution.ExecutionManager, workflowID string, excluded ...string) {
	t.Helper()
	if ids := retryRuns(t, mgr, workflowID, excluded...); len(ids) != 0 {
		t.Fatalf("expected no retry run of %s, found %v", workflowID, ids)
	}
}

func requireRunStatus(t *testing.T, mgr execution.ExecutionManager, runID string, want history.Status) {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatalf("get run %s: %v", runID, err)
	}
	if run.Status != want {
		t.Fatalf("run %s status = %s, want %s", runID, run.Status, want)
	}
}
