package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Run lifecycle 行为矩阵（内存 ExecutionManager + 真实 timer，无外部服务）。
//
// 覆盖缺口：
//   - Workflow ID 在 running 时冲突、终态后可复用；
//   - RunTimeout 触发失败后 retry 派生 Run 的版本/attempt/lineage/execution deadline；
//   - Continue-As-New 新建 Run 的版本切换与 execution deadline 继承。
//
// 仓库不存在独立的 Fork/Rerun 用户 API：Reset 内部通过 ForkBranch 复制 history。

func waitForRunStatusWithinTest(t *testing.T, mgr execution.ExecutionManager, runID string, want history.Status, timeout time.Duration) *execution.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := mgr.Get(runID)
		if err == nil && run.Status == want {
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("run %s status = %s, want %s within %s", runID, run.Status, want, timeout)
	return nil
}

func waitForNewCurrentRun(t *testing.T, mgr execution.ExecutionManager, namespace, workflowID, oldRunID string, timeout time.Duration) *execution.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := mgr.GetCurrentRunID(namespace, workflowID)
		if err == nil && current != "" && current != oldRunID {
			run, err := mgr.Get(current)
			if err != nil {
				t.Fatal(err)
			}
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no new current run for %s within %s", workflowID, timeout)
	return nil
}

// Happy Path: running 时同 WorkflowID 再启动冲突；终态后复用成功并切换 current。
func TestWorkflowIDConflictThenTerminalReuse(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	wf := &program.WorkflowConfig{ID: "lifecycle-id-reuse", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	run1, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil)); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("second start while running = %v, want ErrCurrentRunConflict", err)
	}
	current, err := mgr.GetCurrentRunID(protocol.DefaultNamespace, wf.ID)
	if err != nil || current != run1 {
		t.Fatalf("current run = %q (err %v), want %q", current, err, run1)
	}

	if err := server.TerminateWorkflow(context.Background(), "", "", run1, "catalogue"); err != nil {
		t.Fatal(err)
	}
	waitForRunStatusWithinTest(t, mgr, run1, history.StatusTerminated, 2*time.Second)

	run2, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil))
	if err != nil {
		t.Fatalf("start after terminal status must be allowed: %v", err)
	}
	if run2 == run1 {
		t.Fatal("reused run id; expected a new run")
	}
	current, err = mgr.GetCurrentRunID(protocol.DefaultNamespace, wf.ID)
	if err != nil || current != run2 {
		t.Fatalf("current run = %q (err %v), want %q", current, err, run2)
	}
	old, err := mgr.Get(run1)
	if err != nil {
		t.Fatal(err)
	}
	if old.Status != history.StatusTerminated {
		t.Fatalf("old run status = %s, want terminated", old.Status)
	}
}

// Happy Path: RunTimeout 失败后按 retry policy 新建 Run，并保留 lineage 与 execution deadline。
func TestRunTimeoutRetryCreatesRunWithInheritedLineage(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	wf := &program.WorkflowConfig{
		Program:          programtest.Noop(),
		ID:               "lifecycle-run-timeout-retry",
		ExecutionTimeout: "5s",
		RunTimeout:       "80ms",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts:        2,
			InitialInterval:    "20ms",
			BackoffCoefficient: 1,
		},
	}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	run1 := waitForRunStatusWithinTest(t, mgr, run1ID, history.StatusFailed, 3*time.Second)
	if run1.DeadlineAt == nil {
		t.Fatal("execution deadline must be set")
	}

	run2 := waitForNewCurrentRun(t, mgr, protocol.DefaultNamespace, wf.ID, run1ID, 3*time.Second)
	if run2.Attempt != 2 {
		t.Fatalf("retry attempt = %d, want 2", run2.Attempt)
	}
	if run2.Initiator != execution.InitiatorEnums.Retry {
		t.Fatalf("retry initiator = %q, want %q", run2.Initiator, execution.InitiatorEnums.Retry)
	}
	if run2.ContinuedRunID != run1ID || run2.FirstRunID != run1ID {
		t.Fatalf("lineage = continued %q first %q, want both %q", run2.ContinuedRunID, run2.FirstRunID, run1ID)
	}
	if run2.WorkflowConfigVersion != ref.Version {
		t.Fatalf("retry content = %q, want %q", run2.WorkflowConfigVersion, ref.Version)
	}
	if run2.OriginalWorkflowConfigVersion != ref.Version {
		t.Fatalf("retry original = %q, want %q", run2.OriginalWorkflowConfigVersion, ref.Version)
	}
	if run2.DeadlineAt == nil || !run2.DeadlineAt.Equal(*run1.DeadlineAt) {
		t.Fatalf("retry must inherit execution deadline: run1=%v run2=%v", run1.DeadlineAt, run2.DeadlineAt)
	}
	if run2.RunDeadlineAt != nil && run1.RunDeadlineAt != nil && !run2.RunDeadlineAt.After(*run1.RunDeadlineAt) {
		t.Fatalf("retry run deadline must reset: run1=%v run2=%v", run1.RunDeadlineAt, run2.RunDeadlineAt)
	}

	// MaxAttempts=2：第二次超时不再派生新 Run。
	run2 = waitForRunStatusWithinTest(t, mgr, run2.ID, history.StatusFailed, 3*time.Second)
	final, err := mgr.GetCurrentRunID(protocol.DefaultNamespace, wf.ID)
	if err != nil || final != run2.ID {
		t.Fatalf("current run after max attempts = %q (err %v), want %q", final, err, run2.ID)
	}
	events, err := mgr.GetEvents(run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	var failed *history.WorkflowFailedEvent
	for _, event := range events {
		if event.WorkflowFailed != nil {
			failed = event.WorkflowFailed
		}
	}
	if failed == nil {
		t.Fatal("retry run must record WorkflowFailed")
	}
	if failed.TimeoutType != TimeoutTypeRun {
		t.Fatalf("retry run timeout type = %q, want %q", failed.TimeoutType, TimeoutTypeRun)
	}
}

// Happy Path: Continue-As-New 新建 Run 采用 active latest，保留 original 与 execution deadline。
func TestContinueAsNewUsesActiveLatestAndInheritsLineage(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	refA, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "lifecycle-can-latest", Name: "A", Policy: execution.RunPolicyPinned, ExecutionTimeout: "5s",
	})

	if err != nil {
		t.Fatal(err)
	}
	run1ID, err := startedRunID(server.StartWorkflow(ctx, "", refA.WorkflowID, refA.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	run1, err := mgr.Get(run1ID)
	if err != nil {
		t.Fatal(err)
	}
	if run1.DeadlineAt == nil {
		t.Fatal("execution deadline must be set before continue-as-new")
	}
	refB, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      refA.WorkflowID, Name: "B", Policy: execution.RunPolicyPinned,
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run1ID, server.OpenWorkflowTaskID(run1ID)), run1ID, server.OpenWorkflowTaskID(run1ID), []history.Command{{
		Type:  history.CmdContinueAsNew,
		Input: map[string]any{"generation": int64(2)},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	run2 := waitForNewCurrentRun(t, mgr, protocol.DefaultNamespace, refA.WorkflowID, run1ID, 3*time.Second)
	if run2.WorkflowConfigVersion != refB.Version {
		t.Fatalf("continue-as-new content = %q, want active latest %q", run2.WorkflowConfigVersion, refB.Version)
	}
	if run2.OriginalWorkflowConfigVersion != refA.Version {
		t.Fatalf("continue-as-new original = %q, want %q", run2.OriginalWorkflowConfigVersion, refA.Version)
	}
	if run2.Attempt != 1 {
		t.Fatalf("continue-as-new attempt = %d, want 1", run2.Attempt)
	}
	if run2.DeadlineAt == nil || !run2.DeadlineAt.Equal(*run1.DeadlineAt) {
		t.Fatalf("continue-as-new must keep execution deadline: run1=%v run2=%v", run1.DeadlineAt, run2.DeadlineAt)
	}
	old, err := mgr.Get(run1ID)
	if err != nil {
		t.Fatal(err)
	}
	if old.Status != history.StatusContinuedAsNew {
		t.Fatalf("old run status = %s, want continued_as_new", old.Status)
	}
}

// Continue-As-New 的选版与策略无关：新建 Run 引用 active latest，original/execution deadline 继承。
func TestContinueAsNewVersionSelectionMatrix(t *testing.T) {
	for _, policy := range []string{execution.RunPolicyPinned, execution.RunPolicyAutoUpgrade} {
		t.Run(policy, func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewExecutionManager()
			server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
			defer server.Stop()

			refA, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
				Program: programtest.Noop(),
				ID:      "lifecycle-can-matrix-" + policy, Name: "A", Policy: policy, ExecutionTimeout: "5s",
			})

			if err != nil {
				t.Fatal(err)
			}
			run1ID, err := startedRunID(server.StartWorkflow(ctx, "", "lifecycle-can-matrix-"+policy, "lifecycle-can-matrix-"+policy, "", map[string]any{"generation": "one"}))
			if err != nil {
				t.Fatal(err)
			}
			run1, err := mgr.Get(run1ID)
			if err != nil {
				t.Fatal(err)
			}
			refB, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
				Program: programtest.Noop(),
				ID:      refA.WorkflowID, Name: "B", Policy: policy, ExecutionTimeout: "5s",
			})

			if err != nil {
				t.Fatal(err)
			}
			if refB.Version == refA.Version {
				t.Fatal("B content version must differ from A")
			}
			if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run1ID, server.OpenWorkflowTaskID(run1ID)), run1ID, server.OpenWorkflowTaskID(run1ID), []history.Command{{
				Type:  history.CmdContinueAsNew,
				Input: map[string]any{"generation": int64(2)},
			}}, ""); err != nil {
				t.Fatal(err)
			}
			run2 := waitForNewCurrentRun(t, mgr, protocol.DefaultNamespace, refA.WorkflowID, run1ID, 3*time.Second)
			if run2.WorkflowConfigVersion != refB.Version {
				t.Fatalf("CAN content = %q, want active latest %q (policy %s)", run2.WorkflowConfigVersion, refB.Version, policy)
			}
			if run2.OriginalWorkflowConfigVersion != refA.Version {
				t.Fatalf("CAN original = %q, want %q", run2.OriginalWorkflowConfigVersion, refA.Version)
			}
			if run2.Policy != policy {
				t.Fatalf("CAN policy = %q, want inherited %q", run2.Policy, policy)
			}
			if run2.Attempt != 1 {
				t.Fatalf("CAN attempt = %d, want 1", run2.Attempt)
			}
			if run2.DeadlineAt == nil || run1.DeadlineAt == nil || !run2.DeadlineAt.Equal(*run1.DeadlineAt) {
				t.Fatalf("CAN must inherit execution deadline: run1=%v run2=%v", run1.DeadlineAt, run2.DeadlineAt)
			}
		})
	}
}

// 重试耗尽时终态 WorkflowFailed 的 RetryState 说明不再重试的原因（MaximumAttemptsReached），
// TimeoutType 记录触发失败的 Run 超时。假时钟推进到 Run 超时时刻。
func TestRunTimeoutFinalFailureRetryState(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	wf := &program.WorkflowConfig{
		Program:    programtest.Noop(),
		ID:         "lifecycle-retry-state-diagnostic",
		RunTimeout: "1m",
		RetryPolicy: &program.RetryPolicy{
			MaxAttempts: 1, InitialInterval: "10s", BackoffCoefficient: 1,
		},
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute - time.Millisecond)
	if status, _ := server.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("status before the run timeout = %s, want running", status)
	}
	clk.Advance(time.Millisecond)
	waitForRunStatusWithinTest(t, mgr, runID, history.StatusFailed, 3*time.Second)
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var failed *history.WorkflowFailedEvent
	for _, event := range events {
		if event.WorkflowFailed != nil {
			failed = event.WorkflowFailed
		}
	}
	if failed == nil {
		t.Fatal("expected WorkflowFailed event")
	}
	if failed.TimeoutType != TimeoutTypeRun || failed.RetryState != "MaximumAttemptsReached" || failed.NewExecutionRunID != "" {
		t.Fatalf("final WorkflowFailed: timeoutType=%q retryState=%q next=%q, want %q/MaximumAttemptsReached/none",
			failed.TimeoutType, failed.RetryState, failed.NewExecutionRunID, TimeoutTypeRun)
	}
}
