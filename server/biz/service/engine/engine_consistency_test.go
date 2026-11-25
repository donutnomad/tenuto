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

// 本文件固定引擎层的一致性规则：重试退避按持久化时间执行、已记录的 Update 结果在 Run 关闭后仍可取回、
// Reset 子 Run 不向父 Run 报告终止、WFT 提交后的事务外动作不受 sticky 分配影响、
// 已 started 的 WFT 重投沿用原 Program 版本、Reset 搬运外部输入与建 Run 同事务、
// 只带 runID 的交互与同 Workflow 的其它操作共用一把锁、终态 Run 的策略不可改。

// 重试退避时间写在 ActivityState 上：周期补偿与重启恢复都按它挂 retry timer，到点才派发下一个 attempt。
func TestActivityRetryBackoffSurvivesReconcileAndRestart(t *testing.T) {
	server, mgr, _, clk := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "retry-backoff", history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", RetryPolicy: retryPolicy(3, time.Hour),
	})
	states, err := mgr.ListActivityStates(runID)
	if err != nil || len(states) != 1 {
		t.Fatalf("activity states = %d, err = %v", len(states), err)
	}
	st := states[0]
	if err := server.HandleActivityStarted(runID, st.ScheduledEventID, 1, st.Stamp, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(context.Background(), runID, st.ScheduledEventID, 1, st.Stamp, protocol.Failure{Message: "boom"}); err != nil {
		t.Fatal(err)
	}
	failed, ok, err := mgr.GetActivityState(runID, st.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state: ok=%v err=%v", ok, err)
	}
	if failed.Attempt != 2 || failed.NextAttemptAt == nil || !failed.NextAttemptAt.After(clk.Now().Add(time.Minute)) {
		t.Fatalf("attempt=%d nextAttemptAt=%v, want attempt 2 with a future retry time", failed.Attempt, failed.NextAttemptAt)
	}
	retryRef := activityTimerRef(timerActivityRetry, runID, st.ScheduledEventID)

	cases := []struct {
		name    string
		recover func(*Server) error
	}{
		{"reconcile", func(s *Server) error { _, err := s.DispatchPendingTasks(context.Background()); return err }},
		{"restart", func(s *Server) error { return s.RecoverRunningWorkflows(context.Background()) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			activityTasks := newTestTaskQueue()
			other := assembleServerWithClock(mgr, newTestTaskQueue(), activityTasks, clk)
			t.Cleanup(other.Stop)
			// 另一个进程从共享的定义存储读到同一份配置；测试里的定义存储按 Server 各一份，这里补登记。
			if _, err := other.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, contractWorkflowConfig("retry-backoff", nil)); err != nil {
				t.Fatal(err)
			}
			if err := tc.recover(other); err != nil {
				t.Fatal(err)
			}
			for _, lease := range activityTasks.snapshot() {
				if lease.Task.RunID == runID && lease.Task.Attempt == 2 {
					t.Fatalf("attempt 2 dispatched before its retry time: %+v", lease.Task)
				}
			}
			deadline, armed := other.timers.Deadline(retryRef)
			if !armed || !deadline.Equal(*failed.NextAttemptAt) {
				t.Fatalf("retry timer armed=%v deadline=%v, want %v", armed, deadline, *failed.NextAttemptAt)
			}
		})
	}
}

// 同一个 updateID 的重复请求在 Run 关闭后返回已记录的结果，按 runID 与按 workflowID 都一样。
func TestUpdateWorkflowRepeatOnClosedRunReturnsRecordedResult(t *testing.T) {
	srv, mgr := newServerForTest(t)
	ctx := context.Background()
	const workflowID, updateID = "update-closed-flow", "up-closed"
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: workflowID, Program: programtest.Noop()})
	update := func(runID, updateID string) (protocol.WorkflowUpdateResult, error) {
		return srv.UpdateWorkflow(ctx, "", workflowID, runID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	}
	first, err := update(runID, updateID)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.TerminateWorkflow(context.Background(), "", "", runID, "closed"); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"by run id": runID, "by workflow id": ""} {
		repeat, err := update(target, updateID)
		if err != nil {
			t.Fatalf("%s: repeat update on closed run: %v", name, err)
		}
		if repeat.UpdateID != updateID || repeat.AcceptedEventID != first.AcceptedEventID {
			t.Fatalf("%s: repeat = %+v, want the recorded update %+v", name, repeat, first)
		}
	}
	events, _ := mgr.GetEvents(runID)
	if accepted := countEngineEvents(events, history.EventWorkflowUpdateAccepted); accepted != 1 {
		t.Fatalf("accepted events = %d, want 1", accepted)
	}
	// 没有记录的 updateID 照常按 Run 状态拒绝。
	if _, err := update(runID, "up-new"); err == nil {
		t.Fatal("new update on closed run must be rejected")
	}
}

// Reset 一个运行中的子 Run：父 History 不出现 ChildWorkflowTerminated，父的子关联改指新 Run，
// 新 Run 的结果照常传播给父。
func TestResetRunningChildKeepsParentLink(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-reset-child")
	ctx := context.Background()
	childWFT := openWorkflowTaskForTest(t, mgr, childRunID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, childRunID, childWFT), childRunID, childWFT, nil, ""); err != nil {
		t.Fatal(err)
	}

	newRunID, err := srv.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: childRunID, Reason: "redo", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}

	parentHistory, _ := mgr.GetEvents(parentRunID)
	if got := countEngineEvents(parentHistory, history.EventChildWorkflowTerminated); got != 0 {
		t.Fatalf("parent got %d ChildWorkflowTerminated for a reset child", got)
	}
	st, ok, err := mgr.GetChildWorkflowState(parentRunID, schedID)
	if err != nil || !ok {
		t.Fatalf("child state: ok=%v err=%v", ok, err)
	}
	if st.IsTerminal() || st.ChildRunID != newRunID {
		t.Fatalf("child state = %+v, want open and pointing at %s", st, newRunID)
	}

	if err := srv.completeWorkflow(newRunID, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Completed || st.Output["ok"] != true {
		t.Fatalf("reset run result not propagated: %+v", st)
	}
	parentHistory, _ = mgr.GetEvents(parentRunID)
	if got := countEngineEvents(parentHistory, history.EventChildWorkflowCompleted); got != 1 {
		t.Fatalf("parent ChildWorkflowCompleted = %d, want 1", got)
	}
}

var errStickyStoreDown = errors.New("sticky store down")

type stickyAssignFailingManager struct{ execution.ExecutionManager }

func (*stickyAssignFailingManager) AssignStickyExecution(string, int64, int64, string, string, time.Time) (int64, bool, error) {
	return 0, false, errStickyStoreDown
}

// sticky 分配失败时 WFT 完成照常被确认，事务外动作（这里是 durable timer）已经执行。
func TestCompleteWorkflowActivationAcksWhenStickyAssignmentFails(t *testing.T) {
	mgr := &stickyAssignFailingManager{ExecutionManager: memory.NewExecutionManager()}
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "sticky-assign-fails", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	ctx := srv.WorkflowTaskContext(context.Background(), runID, wftID)
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	task := protocol.Task{
		Type: protocol.TaskTypeWorkflow, Namespace: protocol.DefaultNamespace, RunID: runID, ScheduledEventID: wftID,
		WorkerIdentity: "worker-1", ExecutorABIVersion: history.ExecutorABIVersionV1,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
	}
	commands := []history.Command{{Type: history.CmdScheduleTimer, TimerID: "sleep-1", Delay: time.Hour}}
	digest, err := history.DigestCommands(commands)
	if err != nil {
		t.Fatal(err)
	}

	ack, err := srv.CompleteWorkflowActivation(ctx, history.WorkflowTaskCompletionRequest{
		Task: task,
		Activation: history.ActivationResult{
			ActivationID: history.WorkflowActivationID(task), ExecutionGeneration: state.ExecutionGeneration,
			CommandDigest: digest, ActivatedThroughEventID: events[len(events)-1].ID,
		},
		Commands: commands,
	})
	if err != nil {
		t.Fatalf("completion must be acknowledged without sticky: %v", err)
	}
	if !ack.Accepted || ack.StickyGeneration != state.StickyGeneration {
		t.Fatalf("ack = %+v, want accepted with sticky generation %d", ack, state.StickyGeneration)
	}
	timers, err := mgr.ListTimerStates(runID)
	if err != nil || len(timers) != 1 {
		t.Fatalf("timer states = %d, err = %v", len(timers), err)
	}
	if !timerArmed(srv, timerRef{Kind: timerDurable, RunID: runID, ScheduledEventID: timers[0].ScheduledEventID}) {
		t.Fatal("durable timer was not armed after the commit")
	}
}

// 已 started 的 WFT 被重新投递时沿用 started 时选中的 Program 版本，期间注册的新版本留给下一个 WFT。
func TestRedeliveredStartedWorkflowTaskKeepsProgramVersion(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	srv := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	t.Cleanup(srv.Stop)
	ctx := context.Background()
	refA, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "redeliver-flow", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(srv.StartWorkflow(ctx, "", "redeliver-flow", "redeliver-flow", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.SetRunPolicy(context.Background(), "", runID, execution.RunPolicyAutoUpgrade); err != nil {
		t.Fatal(err)
	}
	queued := workflowTasks.snapshot()
	if len(queued) != 1 {
		t.Fatalf("queued workflow tasks = %d, want 1", len(queued))
	}
	pollCtx := protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "worker-1", BuildID: "build-1", WorkflowCapabilities: program.CurrentWLangCapabilities()})
	first, err := srv.PollWorkflowTask(pollCtx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Task.ProgramVersion != refA.Version {
		t.Fatalf("first lease version = %q, want %q", first.Task.ProgramVersion, refA.Version)
	}

	refB, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "redeliver-flow", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if refB.Version == refA.Version {
		t.Fatal("second registration must produce a new version")
	}
	if err := workflowTasks.Enqueue(ctx, queued[0].Task); err != nil {
		t.Fatal(err)
	}
	second, err := srv.PollWorkflowTask(pollCtx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Task.ScheduledEventID != first.Task.ScheduledEventID || second.Task.ProgramVersion != refA.Version {
		t.Fatalf("redelivered lease = event %d version %q, want event %d version %q",
			second.Task.ScheduledEventID, second.Task.ProgramVersion, first.Task.ScheduledEventID, refA.Version)
	}
}

// Reset 搬运外部输入与建新 Run 同事务：搬运失败时没有新 Run。
func TestResetWorkflowReapplyFailureCreatesNoRun(t *testing.T) {
	ctx := context.Background()
	mgr := newFailingEventManager()
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{Type: "reset-reapply-fails", Program: programtest.Noop()})
	wftID := openWorkflowTaskForTest(t, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.SignalWorkflow(ctx, "", "", runID, "approve", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.TerminateWorkflow(context.Background(), "", "", runID, "done"); err != nil {
		t.Fatal(err)
	}
	events, _ := mgr.GetEvents(runID)
	var startedID int64
	for _, event := range events {
		if event.WorkflowTaskStarted != nil && event.WorkflowTaskStarted.ScheduledEventID == wftID {
			startedID = event.ID
		}
	}

	mgr.failOn(history.EventWorkflowSignaled)
	if _, err := srv.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, ResetEventID: startedID, Reason: "redo"}); err == nil {
		t.Fatal("reset must fail when a reapplied signal cannot be written")
	}
	mgr.failOn("")

	runs, err := mgr.List(execution.StoreListFilter{IncludeTerminal: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 1 || runs.Runs[0].ID != runID {
		t.Fatalf("runs after failed reset = %d, want only the base run", len(runs.Runs))
	}
}

// 只带 runID 的 Signal 与同一个 (namespace, workflowID) 的其它操作共用一把锁。
func TestSignalByRunIDTakesWorkflowLock(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-signal-lock")
	release, err := env.server.keyedMu.Lock(context.Background(), workflowLockKey(env.run.Namespace, env.run.WorkflowID))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := env.runtime.SignalWorkflow(ctx, "", "", env.run.ID, "approve", "", nil); !errors.Is(err, context.DeadlineExceeded) {
		release()
		t.Fatalf("signal by run id err = %v, want it to wait for the workflow lock", err)
	}
	release()
	if _, err := env.runtime.SignalWorkflow(context.Background(), "", "", env.run.ID, "approve", "", nil); err != nil {
		t.Fatal(err)
	}
}

// 终态 Run 的策略不可改：请求被拒绝，已存的策略保持原值。
func TestSetRunPolicyRejectsClosedRun(t *testing.T) {
	server := newTestServer()
	current := &execution.Run{ID: "policy-closed-run", Namespace: "default", WorkflowID: "policy-closed", WorkflowType: "policy-closed", WorkflowConfigVersion: "v1", OriginalWorkflowConfigVersion: "v1", Policy: execution.RunPolicyAutoUpgrade, PolicySource: "definition", Status: history.StatusRunning, CreatedAt: time.Now()}
	createStartedRunForTest(t, server.mgr, current)
	if err := server.TerminateWorkflow(context.Background(), "", "", current.ID, "closed"); err != nil {
		t.Fatal(err)
	}
	_, err := server.SetRunPolicy(context.Background(), "default", current.ID, execution.RunPolicyPinned)
	if !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("err = %v, want an invalid-request rejection", err)
	}
	reloaded, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Policy != execution.RunPolicyAutoUpgrade || reloaded.PolicySource != "definition" {
		t.Fatalf("policy after rejected override = %q/%q", reloaded.Policy, reloaded.PolicySource)
	}
}
