package execution_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestCommitWorkflowTaskWritesActivityStateWithHistory(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	started := history.NewWorkflowStartedEvent(nil, "run-1", "", "workflow", "Workflow", 0)
	if _, err := store.Append("run-1", "run-1", "run-1", started); err != nil {
		t.Fatal(err)
	}
	command := history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "charge", ActivityType: "charge", TaskQueue: "payments"}
	commit := execution.WorkflowTaskCommit{
		Run: mustGetRun(t, store, "run-1"), TreeID: "run-1", BranchID: "run-1",
		CloseEvent:    history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
		CommandEvents: []history.Event{history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge", TaskQueue: "payments"})},
		Commands:      []history.Command{command},
	}
	var result execution.WorkflowTaskCompletionResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var commitErr error
		result, commitErr = execution.CommitWorkflowTask(tx, commit)
		return commitErr
	}); err != nil {
		t.Fatal(err)
	}
	if !result.ClosedInserted || len(result.CommandEvents) != 1 {
		t.Fatalf("commit result = %+v", result)
	}
	state, ok, err := store.GetActivityState("run-1", result.CommandEvents[0].EventID)
	if err != nil || !ok || state.ActivityID != "charge" || state.TaskQueue != "payments" {
		t.Fatalf("activity state = %+v, ok = %v, err = %v", state, ok, err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 3 || events[1].WorkflowTaskCompleted == nil || events[2].ActivityScheduled == nil {
		t.Fatalf("committed history = %+v, err = %v", events, err)
	}
}

func commitChangeVersion(t *testing.T, store *memory.ExecutionManager, value string, terminal bool) execution.WorkflowTaskCompletionResult {
	t.Helper()
	input := map[string]any{history.ChangeVersionSearchAttribute: value}
	commit := execution.WorkflowTaskCommit{
		Run: mustGetRun(t, store, "run-1"), TreeID: "run-1", BranchID: "run-1",
		CloseEvent:    history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
		CommandEvents: []history.Event{history.NewSearchAttributesUpsertedEvent(input)},
		Commands:      []history.Command{{Type: history.CmdUpsertSearchAttributes, Input: input}},
	}
	if terminal {
		commit.CommandEvents = append(commit.CommandEvents, history.NewWorkflowCompletedEvent(nil))
		commit.Commands = append(commit.Commands, history.Command{Type: history.CmdCompleteWorkflow})
	}
	var result execution.WorkflowTaskCompletionResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var commitErr error
		result, commitErr = execution.CommitWorkflowTask(tx, commit)
		return commitErr
	}); err != nil {
		t.Fatal(err)
	}
	return result
}

func newSearchAttributeCommitStore(t *testing.T) *memory.ExecutionManager {
	t.Helper()
	store := memory.NewExecutionManager()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "workflow", WorkflowType: "workflow", WorkflowConfigVersion: "v1", CreatedAt: time.Now()}))
	}); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestCommitWorkflowTaskLoserLeavesRunUntouched(t *testing.T) {
	store := newSearchAttributeCommitStore(t)
	if result := commitChangeVersion(t, store, "winner", false); !result.ClosedInserted {
		t.Fatalf("winner result = %+v", result)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		run := mustGetRun(t, store, "run-1")
		if err := run.MarkTerminal(history.StatusCompleted, nil); err != nil {
			return err
		}
		return tx.UpdateRun(run)
	}); err != nil {
		t.Fatal(err)
	}

	if result := commitChangeVersion(t, store, "loser", false); result.ClosedInserted {
		t.Fatalf("loser result = %+v", result)
	}
	current, err := store.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != history.StatusCompleted || current.SearchAttributes[history.ChangeVersionSearchAttribute] != "winner" {
		t.Fatalf("run after losing commit: status=%s attrs=%v", current.Status, current.SearchAttributes)
	}
}

func TestCommitWorkflowTaskKeepsTerminalProjectionWithSearchAttributes(t *testing.T) {
	store := newSearchAttributeCommitStore(t)
	commitChangeVersion(t, store, "final", true)
	state, err := store.GetWorkflowExecutionState("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != history.StatusCompleted {
		t.Fatalf("execution state status = %s", state.Status)
	}
	current, err := store.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != history.StatusCompleted || current.SearchAttributes[history.ChangeVersionSearchAttribute] != "final" {
		t.Fatalf("run status = %s, search attributes = %v; want completed with the final attributes", current.Status, current.SearchAttributes)
	}
}

func searchAttributeUpsert(id int64, key, value string) history.Event {
	event := history.NewSearchAttributesUpsertedEvent(map[string]any{key: value})
	event.ID = id
	return event
}

func TestSearchAttributesAtEventRebuildsFromStartAttributes(t *testing.T) {
	started := history.NewWorkflowStartedEvent(nil, "run-1", "", "workflow", "Workflow", 0)
	started.ID = 1
	started.WorkflowStarted.SearchAttributes = map[string]string{"customer": "a"}
	events := []history.Event{
		started,
		searchAttributeUpsert(3, "biz", "x"),
		searchAttributeUpsert(4, history.ChangeVersionSearchAttribute, "kept"),
		searchAttributeUpsert(9, "biz", "later"),
	}

	got, err := execution.SearchAttributesAtEvent(events, 5)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"customer": "a", "biz": "x", history.ChangeVersionSearchAttribute: "kept"}
	if len(got) != len(want) || got["customer"] != "a" || got["biz"] != "x" || got[history.ChangeVersionSearchAttribute] != "kept" {
		t.Fatalf("attrs at event 5 = %v, want %v", got, want)
	}
	if got, err := execution.SearchAttributesAtEvent(events, 0); err != nil || len(got) != 1 || got["customer"] != "a" {
		t.Fatalf("attrs at start = %v err=%v", got, err)
	}
}

func TestSearchAttributesAtEventRequiresWorkflowStarted(t *testing.T) {
	events := []history.Event{searchAttributeUpsert(3, "biz", "x")}
	if _, err := execution.SearchAttributesAtEvent(events, 5); err == nil {
		t.Fatal("history without WorkflowStarted was accepted")
	}
}

func TestCommitWorkflowTaskRollsBackHistoryWhenStateCannotBeBuilt(t *testing.T) {
	store := memory.NewExecutionManager()
	commit := execution.WorkflowTaskCommit{
		Run: execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "workflow", WorkflowType: "workflow"}), TreeID: "run-1", BranchID: "run-1",
		CloseEvent:    history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
		CommandEvents: []history.Event{history.NewChildWorkflowStartedEvent("child", "child-1", "run-child", "version-1", nil)},
		Commands:      []history.Command{{Type: history.CmdStartChildWorkflow, ChildID: "child-1"}},
	}
	err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, commitErr := execution.CommitWorkflowTask(tx, commit)
		return commitErr
	})
	if err == nil {
		t.Fatal("missing child target was accepted")
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 0 {
		t.Fatalf("history after rollback = %+v, err = %v", events, err)
	}
	if _, ok, stateErr := store.GetChildWorkflowState("run-1", 2); stateErr != nil && !errors.Is(stateErr, execution.ErrRunNotFound) || ok {
		t.Fatalf("child state after rollback: ok = %v, err = %v", ok, stateErr)
	}
}

// seedStartedWorkflowTask 写入 WorkflowStarted 与一个已 started 的 Workflow Task（调度事件 ID 为 2）。
func seedStartedWorkflowTask(t *testing.T, store *memory.ExecutionManager, runID string) {
	t.Helper()
	for _, event := range []history.Event{
		history.NewWorkflowStartedEvent(nil, runID, "", "workflow", "Workflow", 0),
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(2, "worker-1", "", 0),
	} {
		if _, err := store.Append(runID, runID, runID, event); err != nil {
			t.Fatal(err)
		}
	}
}

// Edge Case: 另一个副本在完成提交之前终止了 Run。提交在事务内读到终态后不写任何事件，
// Run 保持 Terminated，不会被 CompleteWorkflow 改成 Completed。
func TestCommitWorkflowTaskRejectsTerminalRun(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedWorkflowTask(t, store, "run-1")
	run, err := store.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.Terminate(tx, run, "run-1", "run-1", "operator")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var result execution.WorkflowTaskCompletionResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var commitErr error
		result, commitErr = execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
			Run: run, TreeID: "run-1", BranchID: "run-1",
			CloseEvent:    history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
			CommandEvents: []history.Event{history.NewWorkflowCompletedEvent(map[string]any{"ok": true})},
			Commands:      []history.Command{{Type: history.CmdCompleteWorkflow}},
		})
		return commitErr
	}); err != nil {
		t.Fatal(err)
	}
	if result.ClosedInserted || result.Superseded || len(result.CommandEvents) != 0 {
		t.Fatalf("commit on a terminated run = %+v, want nothing inserted", result)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 4 || events[3].WorkflowTerminated == nil {
		t.Fatalf("history = %+v, err = %v; want it to end with WorkflowTerminated", events, err)
	}
	state, err := store.GetWorkflowExecutionState("run-1")
	if err != nil || state.Status != history.StatusTerminated {
		t.Fatalf("state = %+v, err = %v; want terminated", state, err)
	}
}

// Happy Path: Program 版本提升与 UpgradeError 清除随 Workflow Task 完成同事务提交，original 与 target 不变。
// Error Case: 事务回滚时 Run 保持提交前的版本与诊断。
func TestCommitWorkflowTaskPromotesProgramVersionWithHistory(t *testing.T) {
	newStore := func(t *testing.T) *memory.ExecutionManager {
		t.Helper()
		store := memory.NewExecutionManager()
		run := execution.NewRun(execution.NewRunOptions{ID: "run-1", WorkflowID: "workflow", WorkflowType: "workflow", WorkflowConfigVersion: "vA"})
		run.OriginalWorkflowConfigVersion = "vA"
		run.TargetWorkerIdentity = "worker-a"
		run.UpgradeError = "auto upgrade to vB failed at workflow task 2"
		if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
			t.Fatal(err)
		}
		seedStartedWorkflowTask(t, store, "run-1")
		return store
	}
	commit := func(store *memory.ExecutionManager, failAfter error) error {
		run, err := store.Get("run-1")
		if err != nil {
			return err
		}
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			result, err := execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
				Run: run, TreeID: "run-1", BranchID: "run-1",
				CloseEvent:     history.NewWorkflowTaskCompletedEvent(2, "worker-1"),
				ProgramVersion: "vB",
			})
			if err != nil {
				return err
			}
			if !result.ClosedInserted {
				return errors.New("workflow task close was not inserted")
			}
			return failAfter
		})
	}

	t.Run("committed", func(t *testing.T) {
		store := newStore(t)
		if err := commit(store, nil); err != nil {
			t.Fatal(err)
		}
		run, err := store.Get("run-1")
		if err != nil {
			t.Fatal(err)
		}
		if run.WorkflowConfigVersion != "vB" || run.UpgradeError != "" {
			t.Fatalf("run after commit: version=%q upgrade_error=%q, want vB and no diagnostic", run.WorkflowConfigVersion, run.UpgradeError)
		}
		if run.OriginalWorkflowConfigVersion != "vA" || run.TargetWorkerIdentity != "worker-a" {
			t.Fatalf("original=%q target=%q, want vA and worker-a", run.OriginalWorkflowConfigVersion, run.TargetWorkerIdentity)
		}
	})

	t.Run("rolled back", func(t *testing.T) {
		store := newStore(t)
		rollback := errors.New("later write failed")
		if err := commit(store, rollback); !errors.Is(err, rollback) {
			t.Fatalf("commit error = %v, want %v", err, rollback)
		}
		run, err := store.Get("run-1")
		if err != nil {
			t.Fatal(err)
		}
		if run.WorkflowConfigVersion != "vA" || run.UpgradeError == "" {
			t.Fatalf("run after rollback: version=%q upgrade_error=%q, want vA and the diagnostic kept", run.WorkflowConfigVersion, run.UpgradeError)
		}
		events, err := store.GetEvents("run-1")
		if err != nil || len(events) != 3 {
			t.Fatalf("history after rollback = %+v, err = %v; want the workflow task still open", events, err)
		}
	})
}

func mustGetRun(t *testing.T, store *memory.ExecutionManager, runID string) *execution.Run {
	t.Helper()
	run, err := store.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// ActivityID 只与仍未关闭的 Activity 比较（对标 Temporal 的 pending activity ID 检查）：前一个 charge 未关闭时
// 再调度 charge 被拒绝、History 不变；它关闭之后同一 ID 可以再用。
func TestCommitWorkflowTaskRejectsActivityIDOfOpenActivity(t *testing.T) {
	commit := func(store *memory.ExecutionManager, closeScheduledID int64) error {
		command := history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "charge", ActivityType: "charge"}
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
				Run: mustGetRun(t, store, "run-1"), TreeID: "run-1", BranchID: "run-1",
				CloseEvent:    history.NewWorkflowTaskCompletedEvent(closeScheduledID, "worker-1"),
				CommandEvents: []history.Event{history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"})},
				Commands:      []history.Command{command},
			})
			return err
		})
	}
	store := newStoreWithRun(t, "run-1")
	for _, event := range []history.Event{
		history.NewWorkflowStartedEvent(nil, "run-1", "", "workflow", "Workflow", 0),
		history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}),
	} {
		if _, err := store.Append("run-1", "run-1", "run-1", event); err != nil {
			t.Fatal(err)
		}
	}
	err := commit(store, 3)
	if !errors.Is(err, execution.ErrScheduleActivityDuplicateID) {
		t.Fatalf("commit with open charge: err = %v, want ErrScheduleActivityDuplicateID", err)
	}
	if events, _ := store.GetEvents("run-1"); len(events) != 2 {
		t.Fatalf("rejected commit changed history: %+v", events)
	}

	if _, err := store.Append("run-1", "run-1", "run-1", history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: "charge", ScheduledEventID: 2})); err != nil {
		t.Fatal(err)
	}
	if err := commit(store, 4); err != nil {
		t.Fatalf("commit after charge closed: %v", err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int
	for _, event := range events {
		if event.ActivityScheduled != nil && event.ActivityScheduled.ActivityID == "charge" {
			scheduled++
		}
	}
	if scheduled != 2 {
		t.Fatalf("ActivityScheduled(charge) events = %d, want 2: %+v", scheduled, events)
	}
}

// 带结束 Run 命令的决策因 unhandled_events 作废后，Run 进入正在关闭状态：重做的 WFT 调度后仍接受 Signal，
// started 之后 Signal 与新 Update 以 ErrWorkflowClosing 拒绝；重做的 WFT 关闭时清除（对标 Temporal 的 workflowCloseAttempted）。
func TestSupersededCloseCommandMarksRunClosing(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedWorkflowTask(t, store, "run-1")
	run := mustGetRun(t, store, "run-1")
	signal := func() error {
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := execution.RecordSignal(tx, run, "run-1", "run-1", "tick", "", nil)
			return err
		})
	}
	update := func(id string) error {
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := execution.AcceptUpdate(tx, run, "run-1", "run-1", "add", id, nil)
			return err
		})
	}
	commit := func(scheduledEventID, seenThroughEventID int64, commands []history.Command, events []history.Event) execution.WorkflowTaskCompletionResult {
		t.Helper()
		closeEvent := history.NewWorkflowTaskCompletedEvent(scheduledEventID, "worker-1")
		closeEvent.WorkflowTaskCompleted.SeenThroughEventID = seenThroughEventID
		var result execution.WorkflowTaskCompletionResult
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			var err error
			result, err = execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
				Run: mustGetRun(t, store, "run-1"), TreeID: "run-1", BranchID: "run-1",
				CloseEvent: closeEvent, Commands: commands, CommandEvents: events,
			})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	closing := func() bool {
		t.Helper()
		state, err := store.GetWorkflowExecutionState("run-1")
		if err != nil {
			t.Fatal(err)
		}
		return state.WorkflowClosing()
	}
	appendEvent := func(event history.Event) int64 {
		t.Helper()
		id, err := store.Append("run-1", "run-1", "run-1", event)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	// 普通命令的决策照常提交，不进入正在关闭状态。
	if err := signal(); err != nil {
		t.Fatal(err)
	}
	if result := commit(2, 3, []history.Command{{Type: history.CmdScheduleTimer, TimerID: "t", Delay: time.Minute}},
		[]history.Event{history.NewTimerScheduledEvent("t", nil, time.Minute.Milliseconds())}); result.Superseded || !result.ClosedInserted {
		t.Fatalf("commit = %+v, want committed", result)
	}
	scheduled := appendEvent(history.NewWorkflowTaskScheduledEvent(0))
	started := appendEvent(history.NewWorkflowTaskStartedEvent(scheduled, "worker-1", "", 0))
	if closing() {
		t.Fatal("a committed decision marked the run closing")
	}

	// 带结束 Run 命令的决策被作废。
	if err := signal(); err != nil {
		t.Fatal(err)
	}
	completeCommands := []history.Command{{Type: history.CmdCompleteWorkflow}}
	completeEvents := []history.Event{history.NewWorkflowCompletedEvent(map[string]any{"ok": true})}
	if result := commit(scheduled, started, completeCommands, completeEvents); !result.Superseded {
		t.Fatalf("commit = %+v, want superseded", result)
	}
	if state, err := store.GetWorkflowExecutionState("run-1"); err != nil || !state.WorkflowTask.CloseAttempted {
		t.Fatalf("state = %+v, err = %v, want the close attempt recorded", state, err)
	}
	// 重做的 WFT 调度之后、started 之前，新事实照常写入并由它一起处理。
	scheduled = appendEvent(history.NewWorkflowTaskScheduledEvent(0))
	if closing() {
		t.Fatal("run closing before the retried workflow task started")
	}
	if err := signal(); err != nil {
		t.Fatal(err)
	}
	started = appendEvent(history.NewWorkflowTaskStartedEvent(scheduled, "worker-1", "", 0))
	if !closing() {
		t.Fatal("run not closing while the retried workflow task is started")
	}
	if err := signal(); !errors.Is(err, execution.ErrWorkflowClosing) || !errors.Is(err, protocol.ErrResourceExhausted) {
		t.Fatalf("signal while closing: err = %v, want ErrWorkflowClosing", err)
	}
	if err := update("u-1"); !errors.Is(err, execution.ErrWorkflowClosing) {
		t.Fatalf("update while closing: err = %v, want ErrWorkflowClosing", err)
	}
	if result := commit(scheduled, started, completeCommands, completeEvents); !result.ClosedInserted || result.Superseded {
		t.Fatalf("retried close = %+v, want committed", result)
	}
	if state, err := store.GetWorkflowExecutionState("run-1"); err != nil || state.Status != history.StatusCompleted || state.WorkflowTask.CloseAttempted {
		t.Fatalf("state after the retried close = %+v, err = %v", state, err)
	}
}

// 正在关闭的 Run 上重做的 WFT 超时：标记清除，Signal 恢复写入。
func TestWorkflowTaskTimeoutClearsClosing(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedWorkflowTask(t, store, "run-1")
	for _, event := range []history.Event{
		history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{ScheduledEventID: 2, Cause: execution.WorkflowTaskFailedCauseUnhandledEvents}),
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(5, "worker-1", "", 0),
	} {
		if _, err := store.Append("run-1", "run-1", "run-1", event); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.GetWorkflowExecutionState("run-1")
	if err != nil || !state.WorkflowClosing() {
		t.Fatalf("state = %+v, err = %v, want closing", state, err)
	}
	if _, err := store.Append("run-1", "run-1", "run-1", history.NewWorkflowTaskTimedOutEvent(5, "timeout")); err != nil {
		t.Fatal(err)
	}
	if state, err = store.GetWorkflowExecutionState("run-1"); err != nil || state.WorkflowClosing() || state.WorkflowTask.CloseAttempted {
		t.Fatalf("state = %+v, err = %v, want the close attempt cleared", state, err)
	}
}
