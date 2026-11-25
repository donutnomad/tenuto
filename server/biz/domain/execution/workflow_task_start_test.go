package execution_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestStartWorkflowTaskBuildsHistorySnapshot(t *testing.T) {
	store := memory.NewExecutionManager()
	current := execution.NewRun(execution.NewRunOptions{ID: "run-started", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(current) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(current.ID, current.ID, current.ID, history.NewWorkflowStartedEvent(nil, current.ID, "", "flow", "Flow", 0)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AppendWorkflowTaskScheduled(current.ID, current.ID, current.ID, history.NewWorkflowTaskScheduledEvent(1000)); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	scheduledID := events[len(events)-1].ID
	state, err := store.GetWorkflowExecutionState(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	startWorkflowTask(t, store, execution.WorkflowTaskStart{
		RunID: current.ID, TreeID: current.ID, BranchID: current.ID, ScheduledEventID: scheduledID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration, ActivationID: "activation-a",
		WorkerIdentity: "worker-a", WorkerBuildID: "build-a", ProgramVersion: current.WorkflowConfigVersion,
		Timeout: 3 * time.Second, Now: startedAt,
	})
	events, err = store.GetEvents(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	started := events[len(events)-1].WorkflowTaskStarted
	if started == nil || started.ScheduledEventID != scheduledID || started.WorkerIdentity != "worker-a" || started.HistoryEventCount != int64(len(events)) || started.HistorySizeBytes <= 0 {
		t.Fatalf("started=%+v events=%d", started, len(events))
	}
	// Happy Path: Started 事件带领取时的超时，execution state 由它得出截止时间。
	if started.TimeoutMs != 3000 || !events[len(events)-1].Timestamp.Equal(startedAt) {
		t.Fatalf("started timeout=%d timestamp=%v", started.TimeoutMs, events[len(events)-1].Timestamp)
	}
	state, err = store.GetWorkflowExecutionState(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	id, deadline, ok := state.WorkflowTaskTimeoutDeadline()
	if !ok || id != scheduledID || !deadline.Equal(startedAt.Add(3*time.Second)) {
		t.Fatalf("timeout deadline: id=%d deadline=%v ok=%v", id, deadline, ok)
	}
}

// startedWorkflowTask 建一个 Run，调度并领取一个 WFT（sticky 交给 worker-a），返回 WFT 的调度事件 ID。
func startedWorkflowTask(t *testing.T, store *memory.ExecutionManager, runID string, startedAt time.Time, timeout time.Duration) int64 {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "flow-" + runID, WorkflowType: "flow", WorkflowConfigVersion: "v1", CreatedAt: startedAt})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(runID, runID, runID, history.NewWorkflowStartedEvent(nil, runID, "", "flow", "Flow", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, _, err := store.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(timeout.Milliseconds()))
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-a", "build-a", startedAt.Add(time.Hour)); err != nil || !ok {
		t.Fatalf("assign sticky: ok=%v err=%v", ok, err)
	}
	state, err = store.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	startWorkflowTask(t, store, execution.WorkflowTaskStart{
		RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: scheduledID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration, ActivationID: "activation-a",
		WorkerIdentity: "worker-a", ProgramVersion: "v1", Timeout: timeout, Now: startedAt,
	})
	return scheduledID
}

// startWorkflowTask 在一个事务里开始 WFT，要求激活被接受。
func startWorkflowTask(t *testing.T, store *memory.ExecutionManager, start execution.WorkflowTaskStart) {
	t.Helper()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		result, err := execution.StartWorkflowTask(tx, start)
		if err == nil && !result.Started {
			err = errors.New("activation rejected")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// failStartedTx 让事务里 WorkflowTaskStarted 事件的写入失败，其余操作走真实事务。
type failStartedTx struct {
	execution.ExecutionTx
}

func (t failStartedTx) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if event.WorkflowTaskStarted != nil {
		return 0, false, errors.New("injected started write failure")
	}
	return t.ExecutionTx.AppendIdempotent(runID, treeID, branchID, event)
}

func TestStartWorkflowTaskLeavesNoActivationWhenStartedWriteFails(t *testing.T) {
	// Error Case：激活已在事务内写入、Started 的写入失败：事务回滚，不留下没有截止时间的激活，
	// 下一份投递照常开始这个 WFT。
	store := memory.NewExecutionManager()
	run := execution.NewRun(execution.NewRunOptions{ID: "run-start-fail", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, run.ID, "", "flow", "Flow", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, _, err := store.AppendWorkflowTaskScheduled(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	start := execution.WorkflowTaskStart{
		RunID: run.ID, TreeID: run.ID, BranchID: run.ID, ScheduledEventID: scheduledID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration, ActivationID: "activation-crashed",
		WorkerIdentity: "worker-a", ProgramVersion: "v1", Timeout: 5 * time.Second, Now: startedAt,
	}
	err = store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.StartWorkflowTask(failStartedTx{tx}, start)
		return err
	})
	if err == nil {
		t.Fatal("the injected failure did not fail the transaction")
	}
	state, err = store.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentActivationID != "" || state.CurrentActivationAttempt != 0 {
		t.Fatalf("failed start left an activation behind: %q attempt=%d", state.CurrentActivationID, state.CurrentActivationAttempt)
	}

	start.ActivationID = "activation-retry"
	startWorkflowTask(t, store, start)
	state, err = store.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if id, deadline, ok := state.WorkflowTaskTimeoutDeadline(); state.CurrentActivationID != "activation-retry" || !ok || id != scheduledID || !deadline.Equal(startedAt.Add(5*time.Second)) {
		t.Fatalf("restarted task: activation=%q deadline=%v ok=%v", state.CurrentActivationID, deadline, ok)
	}
}

func timeoutWorkflowTask(t *testing.T, store *memory.ExecutionManager, timeout execution.WorkflowTaskTimeout) execution.WorkflowTaskCloseResult {
	t.Helper()
	var result execution.WorkflowTaskCloseResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutWorkflowTask(tx, timeout)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTimeoutWorkflowTaskWaitsForPersistedDeadline(t *testing.T) {
	store := memory.NewExecutionManager()
	startedAt := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	scheduledID := startedWorkflowTask(t, store, "run-timeout", startedAt, 10*time.Second)
	timeout := execution.WorkflowTaskTimeout{
		RunID: "run-timeout", TreeID: "run-timeout", BranchID: "run-timeout", ScheduledEventID: scheduledID,
		Message: "workflow task start-to-close timeout", ServerTimer: true,
	}

	// Edge Case: Server 的 timer 早于持久化的截止时间触发（例如另一副本推迟了它）时不做任何修改。
	timeout.Now = startedAt.Add(10*time.Second - time.Millisecond)
	if result := timeoutWorkflowTask(t, store, timeout); result.Inserted {
		t.Fatal("a timer ahead of the persisted deadline closed the workflow task")
	}

	// Happy Path: 截止时间已过，WFT 记为超时并关闭，sticky 会话结束。
	timeout.Now = startedAt.Add(10 * time.Second)
	if result := timeoutWorkflowTask(t, store, timeout); !result.Inserted {
		t.Fatal("an expired workflow task was not timed out")
	}
	state, err := store.GetWorkflowExecutionState("run-timeout")
	if err != nil {
		t.Fatal(err)
	}
	if state.WorkflowTaskOpen(scheduledID) || state.StickyWorkerIdentity != "" || state.StickyExpiresAt != nil {
		t.Fatalf("after timeout: open=%v sticky=%q expires=%v", state.WorkflowTaskOpen(scheduledID), state.StickyWorkerIdentity, state.StickyExpiresAt)
	}
	if _, _, ok := state.WorkflowTaskTimeoutDeadline(); ok {
		t.Fatal("a closed workflow task keeps its timeout deadline")
	}

	// Edge Case: Worker 上报的超时与 Server 的计时竞争，同一个 WFT 只关闭一次。
	timeout.ServerTimer = false
	if result := timeoutWorkflowTask(t, store, timeout); result.Inserted {
		t.Fatal("a second timeout closed the workflow task again")
	}
	events, err := store.GetEvents("run-timeout")
	if err != nil {
		t.Fatal(err)
	}
	timedOut := 0
	for _, event := range events {
		if event.WorkflowTaskTimedOut != nil {
			timedOut++
		}
	}
	if timedOut != 1 {
		t.Fatalf("WorkflowTaskTimedOut events = %d, want 1", timedOut)
	}
}

func TestTimeoutWorkflowTaskFromWorkerClosesBeforeDeadline(t *testing.T) {
	store := memory.NewExecutionManager()
	startedAt := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	scheduledID := startedWorkflowTask(t, store, "run-worker-timeout", startedAt, 10*time.Second)
	// Happy Path: Worker 自己上报的超时不等 Server 的截止时间。
	result := timeoutWorkflowTask(t, store, execution.WorkflowTaskTimeout{
		RunID: "run-worker-timeout", TreeID: "run-worker-timeout", BranchID: "run-worker-timeout",
		ScheduledEventID: scheduledID, Message: "context deadline exceeded", Now: startedAt.Add(time.Second),
	})
	if !result.Inserted {
		t.Fatal("the worker-reported timeout was not recorded")
	}
}

func TestStartWorkflowTaskSuggestsContinueAsNewByStoreLimits(t *testing.T) {
	// Happy Path：continue-as-new 建议按存储配置的上限给出，配置调低时 Run 在碰到硬上限之前就收到建议。
	limits := history.HistoryLimits{WarningEvents: 3, WarningBytes: 1 << 20, HardEvents: 100, HardBytes: 2 << 20, ReservedCloseEvents: 2, ReservedCloseBytes: 1 << 10}
	store, err := memory.NewExecutionManagerWithHistoryLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	run := execution.NewRun(execution.NewRunOptions{ID: "run-limits", WorkflowID: "flow", WorkflowType: "flow", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(run.ID, run.ID, run.ID, history.NewWorkflowStartedEvent(nil, run.ID, "", "flow", "Flow", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, _, err := store.AppendWorkflowTaskScheduled(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	startWorkflowTask(t, store, execution.WorkflowTaskStart{
		RunID: run.ID, TreeID: run.ID, BranchID: run.ID, ScheduledEventID: scheduledID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration, ActivationID: "a",
		ProgramVersion: "v1", Timeout: time.Second, Now: time.Now(),
	})
	events, err := store.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	started := events[len(events)-1].WorkflowTaskStarted
	if started == nil || len(started.ContinueAsNewReasons) != 1 || started.ContinueAsNewReasons[0] != history.ContinueAsNewReasonHistoryEventCount {
		t.Fatalf("started = %+v, want the event-count suggestion from the store limits", started)
	}
}
