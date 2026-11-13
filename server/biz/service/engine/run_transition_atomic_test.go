package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// txFaultManager 在设置了 fault 时用它包装每个事务，让事务里的某一次写入失败，RunInTx 整体回滚。
type txFaultManager struct {
	execution.ExecutionManager
	mu    sync.Mutex
	fault func(execution.ExecutionTx) execution.ExecutionTx
}

func newTxFaultManager() *txFaultManager {
	return &txFaultManager{ExecutionManager: memory.NewExecutionManager()}
}

func (m *txFaultManager) setFault(fault func(execution.ExecutionTx) execution.ExecutionTx) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fault = fault
}

func (m *txFaultManager) RunInTx(fn func(execution.ExecutionTx) error) error {
	m.mu.Lock()
	fault := m.fault
	m.mu.Unlock()
	return m.ExecutionManager.RunInTx(func(tx execution.ExecutionTx) error {
		if fault != nil {
			tx = fault(tx)
		}
		return fn(tx)
	})
}

type failUpdateRunTx struct{ execution.ExecutionTx }

func (failUpdateRunTx) UpdateRun(*execution.Run) error { return errInjectedWrite }

func failUpdateRun(tx execution.ExecutionTx) execution.ExecutionTx { return failUpdateRunTx{tx} }

// failChildStartedTx 只让写入 Started 的子关联失败；父 WFT 提交时写入的 Initiated 照常。
type failChildStartedTx struct{ execution.ExecutionTx }

func (t failChildStartedTx) UpsertChildWorkflowState(state *execution.ChildWorkflowState) error {
	if state.Status == execution.ChildWorkflowStatusEnums.Started {
		return errInjectedWrite
	}
	return t.ExecutionTx.UpsertChildWorkflowState(state)
}

type failWorkflowTaskCloseTx struct{ execution.ExecutionTx }

func (failWorkflowTaskCloseTx) AppendWorkflowTaskClosed(string, string, string, history.Event) (int64, bool, error) {
	return 0, false, errInjectedWrite
}

type failStateReadTx struct{ execution.ExecutionTx }

func (failStateReadTx) GetWorkflowExecutionStateForUpdate(string) (*execution.WorkflowExecutionState, error) {
	return nil, errInjectedWrite
}

func newAtomicTestServer(t *testing.T, mgr *txFaultManager, configs ...*program.WorkflowConfig) (*Server, *clocktest.Fake) {
	t.Helper()
	clk := clocktest.NewFake(testClockStart)
	server := assembleServerWithClock(mgr, &ackRecordingQueue{}, newTestTaskQueue(), clk)
	t.Cleanup(server.Stop)
	for _, config := range configs {
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, config); err != nil {
			t.Fatal(err)
		}
	}
	return server, clk
}

func mustRun(t *testing.T, mgr execution.ExecutionManager, runID string) *execution.Run {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func runEvents(t *testing.T, mgr execution.ExecutionManager, runID string) []history.Event {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// 终态命令的事件与 Run 的终态在 Workflow Task 完成的同一事务里：写 Run 失败时 History 也不变，
// WFT 保持 open，同一个完成重试后两者一起提交。
func TestTerminalCommandCommitsEventWithRunStatus(t *testing.T) {
	for _, test := range []struct {
		name    string
		command history.Command
		event   history.EventType
		status  history.Status
	}{
		{name: "complete", command: history.Command{Type: history.CmdCompleteWorkflow, Output: map[string]any{"ok": true}}, event: history.EventWorkflowCompleted, status: history.StatusCompleted},
		{name: "fail", command: history.Command{Type: history.CmdFailWorkflow, WorkflowFailure: &protocol.Failure{Message: "boom"}}, event: history.EventWorkflowFailed, status: history.StatusFailed},
		{name: "cancel", command: history.Command{Type: history.CmdCancelWorkflow}, event: history.EventWorkflowCancelled, status: history.StatusCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			mgr := newTxFaultManager()
			workflowID := "terminal-atomic-" + test.name
			server, _ := newAtomicTestServer(t, mgr, &program.WorkflowConfig{ID: workflowID, Program: programtest.Noop()})
			runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			lease := pollWorkflowTaskAs(t, server, "worker-a")
			taskCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)
			commands := []history.Command{test.command}

			mgr.setFault(failUpdateRun)
			if err := server.HandleWorkflowTaskCompleted(taskCtx, runID, lease.Task.ScheduledEventID, commands, "worker-a"); !errors.Is(err, errInjectedWrite) {
				t.Fatalf("complete err = %v, want the injected failure", err)
			}
			events := runEvents(t, mgr, runID)
			if got := countEngineEvents(events, test.event) + countEngineEvents(events, history.EventWorkflowTaskCompleted); got != 0 {
				t.Fatalf("failed commit left %d terminal or completion events", got)
			}
			if run := mustRun(t, mgr, runID); run.Status != history.StatusRunning {
				t.Fatalf("run status after a failed commit = %s, want running", run.Status)
			}

			mgr.setFault(nil)
			if err := server.HandleWorkflowTaskCompleted(taskCtx, runID, lease.Task.ScheduledEventID, commands, "worker-a"); err != nil {
				t.Fatal(err)
			}
			if got := countEngineEvents(runEvents(t, mgr, runID), test.event); got != 1 {
				t.Fatalf("%s events = %d, want 1", test.event, got)
			}
			if run := mustRun(t, mgr, runID); run.Status != test.status {
				t.Fatalf("run status = %s, want %s", run.Status, test.status)
			}
		})
	}
}

// 旧 Run 的 ContinuedAsNew 状态与接续 Run 在同一事务：写旧 Run 失败时接续 Run 不存在、旧 Run 保持 Running，
// 补偿扫描据此重建接续 Run 并关闭旧 Run。
func TestContinueAsNewClosesOldRunWithContinuedRun(t *testing.T) {
	ctx := context.Background()
	mgr := newTxFaultManager()
	server, _ := newAtomicTestServer(t, mgr, &program.WorkflowConfig{ID: "can-atomic", Program: programtest.Noop()})
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "can-atomic", "can-atomic", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	lease := pollWorkflowTaskAs(t, server, "worker-a")
	taskCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)

	mgr.setFault(failUpdateRun)
	err = server.HandleWorkflowTaskCompleted(taskCtx, runID, lease.Task.ScheduledEventID,
		[]history.Command{{Type: history.CmdContinueAsNew, Input: map[string]any{"page": int64(2)}}}, "worker-a")
	if !errors.Is(err, errInjectedWrite) {
		t.Fatalf("complete err = %v, want the injected failure", err)
	}
	var nextRunID string
	for _, event := range runEvents(t, mgr, runID) {
		if event.WorkflowContinuedAsNew != nil {
			nextRunID = event.WorkflowContinuedAsNew.NextRunID
		}
	}
	if nextRunID == "" {
		t.Fatal("the workflow task commit must keep WorkflowContinuedAsNew")
	}
	if _, err := mgr.Get(nextRunID); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("continued run after a failed commit: err = %v, want not found", err)
	}
	if run := mustRun(t, mgr, runID); run.Status != history.StatusRunning {
		t.Fatalf("old run status = %s, want running until the continued run exists", run.Status)
	}

	mgr.setFault(nil)
	if _, err := server.reconcileRun(ctx, runID, false); err != nil {
		t.Fatal(err)
	}
	if run := mustRun(t, mgr, runID); run.Status != history.StatusContinuedAsNew {
		t.Fatalf("old run status after reconcile = %s, want continued-as-new", run.Status)
	}
	if run := mustRun(t, mgr, nextRunID); run.Status != history.StatusRunning {
		t.Fatalf("continued run status = %s, want running", run.Status)
	}
}

// 子 Run 与父侧子关联的 Started 状态在同一事务：写父侧状态失败时子 Run 不存在、关联保持 Initiated，
// 父 Run 的补偿扫描据此重建子 Run。
func TestChildRunCommitsWithParentStartedState(t *testing.T) {
	ctx := context.Background()
	mgr := newTxFaultManager()
	server, _ := newAtomicTestServer(t, mgr, &program.WorkflowConfig{ID: "child-atomic-parent", Program: programtest.Noop()}, &program.WorkflowConfig{ID: "child-atomic-child", Program: programtest.Noop()})
	parentRunID, err := startedRunID(server.StartWorkflow(ctx, "", "child-atomic-parent", "child-atomic-parent", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	lease := pollWorkflowTaskAs(t, server, "worker-a")
	taskCtx := protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken)

	mgr.setFault(func(tx execution.ExecutionTx) execution.ExecutionTx { return failChildStartedTx{tx} })
	// 子 Run 的启动是提交后的动作：失败只记日志，留给补偿扫描。
	if err := server.HandleWorkflowTaskCompleted(taskCtx, parentRunID, lease.Task.ScheduledEventID,
		[]history.Command{{Type: history.CmdStartChildWorkflow, ChildID: "child-1", WorkflowID: "child-atomic-child"}}, "worker-a"); err != nil {
		t.Fatal(err)
	}
	states, err := mgr.ListChildWorkflowStates(parentRunID)
	if err != nil || len(states) != 1 {
		t.Fatalf("child states = %+v, err = %v", states, err)
	}
	if states[0].Status != execution.ChildWorkflowStatusEnums.Initiated {
		t.Fatalf("child state = %s, want initiated", states[0].Status)
	}
	childRunID := states[0].ChildRunID
	if _, err := mgr.Get(childRunID); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("child run after a failed start: err = %v, want not found", err)
	}

	mgr.setFault(nil)
	if _, err := server.reconcileRun(ctx, parentRunID, false); err != nil {
		t.Fatal(err)
	}
	state, found, err := mgr.GetChildWorkflowState(parentRunID, states[0].ScheduledEventID)
	if err != nil || !found || state.Status != execution.ChildWorkflowStatusEnums.Started || state.ChildRunID != childRunID {
		t.Fatalf("child state after reconcile = %+v, found = %v, err = %v", state, found, err)
	}
	if run := mustRun(t, mgr, childRunID); run.Status != history.StatusRunning {
		t.Fatalf("child run status = %s, want running", run.Status)
	}
}

// set-policy 的策略写入与恢复时关闭的 Workflow Task 在同一事务：关闭失败时 Run 仍暂停、策略不变。
func TestSetRunPolicyCommitsPolicyWithResumeClose(t *testing.T) {
	ctx := context.Background()
	mgr := newTxFaultManager()
	server, _ := newAtomicTestServer(t, mgr, &program.WorkflowConfig{ID: "policy-atomic", Program: programtest.Noop()})
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "policy-atomic", "policy-atomic", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	pollWorkflowTaskAs(t, server, "worker-a")
	if err := server.pauseRun(runID, "contract mismatch"); err != nil {
		t.Fatal(err)
	}

	mgr.setFault(func(tx execution.ExecutionTx) execution.ExecutionTx { return failWorkflowTaskCloseTx{tx} })
	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("set policy err = %v, want the injected failure", err)
	}
	run := mustRun(t, mgr, runID)
	if run.ConfigError != "contract mismatch" || run.PolicySource != execution.PolicySourceDefinition {
		t.Fatalf("run after a failed set-policy: config_error=%q policy_source=%q, want still paused", run.ConfigError, run.PolicySource)
	}

	mgr.setFault(nil)
	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	run = mustRun(t, mgr, runID)
	if run.ConfigError != "" || run.Policy != execution.RunPolicyPinned || run.PolicySource != execution.PolicySourceRunOverride {
		t.Fatalf("run after set-policy: config_error=%q policy=%q source=%q", run.ConfigError, run.Policy, run.PolicySource)
	}
	resumed := 0
	for _, event := range runEvents(t, mgr, runID) {
		if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause == execution.WorkflowTaskFailedCauseRunResumed {
			resumed++
		}
	}
	if resumed != 1 {
		t.Fatalf("resume closes = %d, want 1", resumed)
	}
}

// seedUpgradeAttempt 建一个 auto_upgrade 的 Run，它的 open WFT 在候选版本 v2 上 started。
func seedUpgradeAttempt(t *testing.T, mgr execution.ExecutionManager, runID string) int64 {
	t.Helper()
	run := &execution.Run{
		ID: runID, Namespace: "default", WorkflowID: runID, WorkflowType: runID,
		WorkflowConfigVersion: "v1", OriginalWorkflowConfigVersion: "v1",
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: testClockStart,
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, runID, runID, history.NewWorkflowStartedEvent(nil, runID, "", runID, runID, 0)); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowTaskStartedEvent(scheduledEventID, "worker", "build", 0)
	started.WorkflowTaskStarted.ProgramVersion = "v2"
	if _, err := mgr.Append(runID, runID, runID, started); err != nil {
		t.Fatal(err)
	}
	return scheduledEventID
}

// WorkflowTaskFailed 与 AutoUpgrade 诊断在同一事务：任一步失败时两者都不写入，WFT 保持 open，
// 由 Worker 再次上报或 Server 的 start-to-close 超时关闭；之后的失败照常写入两者。
func TestWorkflowTaskFailureCommitsWithUpgradeDiagnostic(t *testing.T) {
	for _, test := range []struct {
		name  string
		fault func(execution.ExecutionTx) execution.ExecutionTx
	}{
		{name: "run write fails", fault: failUpdateRun},
		{name: "state read fails", fault: func(tx execution.ExecutionTx) execution.ExecutionTx { return failStateReadTx{tx} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := newTxFaultManager()
			server, _ := newAtomicTestServer(t, mgr)
			runID := "upgrade-failure-atomic"
			scheduledEventID := seedUpgradeAttempt(t, mgr, runID)
			taskCtx := server.WorkflowTaskContext(context.Background(), runID, scheduledEventID)

			mgr.setFault(test.fault)
			if err := server.HandleWorkflowTaskFailed(taskCtx, runID, scheduledEventID, history.WorkflowTaskFailedCauseNonDeterministic, "replay failed"); !errors.Is(err, errInjectedWrite) {
				t.Fatalf("failure handling err = %v, want the injected failure", err)
			}
			if got := countEngineEvents(runEvents(t, mgr, runID), history.EventWorkflowTaskFailed); got != 0 {
				t.Fatalf("failed transaction left %d WorkflowTaskFailed events", got)
			}
			if run := mustRun(t, mgr, runID); run.UpgradeError != "" {
				t.Fatalf("failed transaction wrote the upgrade diagnostic %q", run.UpgradeError)
			}

			mgr.setFault(nil)
			if err := server.HandleWorkflowTaskFailed(taskCtx, runID, scheduledEventID, history.WorkflowTaskFailedCauseNonDeterministic, "replay failed"); err != nil {
				t.Fatal(err)
			}
			if got := countEngineEvents(runEvents(t, mgr, runID), history.EventWorkflowTaskFailed); got != 1 {
				t.Fatalf("WorkflowTaskFailed events = %d, want 1", got)
			}
			if run := mustRun(t, mgr, runID); run.UpgradeError == "" || run.WorkflowConfigVersion != "v1" {
				t.Fatalf("run after the failure: upgrade_error=%q version=%q, want the diagnostic and v1", run.UpgradeError, run.WorkflowConfigVersion)
			}
		})
	}
}

// 执行截止时间与 Run 截止时间随新 Run 一起写入，不在启动之后另写一次。
func TestStartWritesRunDeadlinesWithRun(t *testing.T) {
	mgr := newTxFaultManager()
	server, clk := newAtomicTestServer(t, mgr, &program.WorkflowConfig{ID: "deadline-atomic", ExecutionTimeout: "1h", RunTimeout: "10m", Program: programtest.Noop()})
	// 启动之后不允许再写 Run：截止时间必须已经在建 Run 的事务里。
	mgr.setFault(failUpdateRun)
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "deadline-atomic", "deadline-atomic", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	run := mustRun(t, mgr, runID)
	now := clk.Now()
	if run.DeadlineAt == nil || !run.DeadlineAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("execution deadline = %v, want %v", run.DeadlineAt, now.Add(time.Hour))
	}
	if run.RunDeadlineAt == nil || !run.RunDeadlineAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("run deadline = %v, want %v", run.RunDeadlineAt, now.Add(10*time.Minute))
	}
	if !timerArmed(server, timerRef{Kind: timerWorkflowExecution, RunID: runID}) || !timerArmed(server, timerRef{Kind: timerWorkflowRun, RunID: runID}) {
		t.Fatal("deadline timers must be armed after start")
	}
}
