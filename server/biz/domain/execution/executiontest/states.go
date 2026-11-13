package executiontest

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// staleTx 让事务内的带锁读取返回调用方更早拿到的快照，复现「读出快照之后、写回之前，
// 另一个写入方已经提交」的局面。其余方法走真实事务。
type staleTx struct {
	execution.ExecutionTx
	activity *execution.ActivityState
	timer    *execution.TimerState
	child    *execution.ChildWorkflowState
}

func (t staleTx) GetActivityStateForUpdate(string, int64) (*execution.ActivityState, bool, error) {
	return t.activity, true, nil
}

func (t staleTx) GetTimerStateForUpdate(string, int64) (*execution.TimerState, bool, error) {
	return t.timer, true, nil
}

func (t staleTx) GetChildWorkflowStateForUpdate(string, int64) (*execution.ChildWorkflowState, bool, error) {
	return t.child, true, nil
}

// contractNow 是契约用例里新建 Run 的创建时间。
var contractNow = time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)

func createContractRun(t *testing.T, manager execution.ExecutionManager, runID string) {
	t.Helper()
	createRun(t, manager, execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
}

func createRun(t *testing.T, manager execution.ExecutionManager, run *execution.Run) {
	t.Helper()
	inTx(t, manager, func(tx execution.ExecutionTx) error { return tx.CreateRun(run) })
}

func inTx(t *testing.T, manager execution.ExecutionManager, fn func(tx execution.ExecutionTx) error) {
	t.Helper()
	if err := manager.RunInTx(fn); err != nil {
		t.Fatal(err)
	}
}

// StatePatchContract 断言 Activity / Timer / Child Workflow state 的更新只写经 setter 改过的字段：
// 持有旧快照的写入方不会把其它写入方已提交的字段改回去，没有改动的 upsert 不改变已存的行。
func StatePatchContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	now := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)

	t.Run("Activity", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-activity"
		createContractRun(t, manager, runID)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			return tx.UpsertActivityState(&execution.ActivityState{
				RunID: runID, ScheduledEventID: 7, ActivityID: "charge", ActivityType: "Charge", TaskQueue: "payments",
				Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now,
				RetryPolicy: &history.RetryPolicy{MaxAttempts: 3}, Input: map[string]any{"order": "o-1"},
				CreatedAt: now, UpdatedAt: now,
			})
		})
		snapshot, found, err := manager.GetActivityState(runID, 7)
		if err != nil || !found {
			t.Fatalf("snapshot: found=%v err=%v", found, err)
		}
		transition := execution.ActivityTransition{RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: now.Add(time.Second)}
		// 写入方 A 记录 Activity 开始。
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			started, err := execution.StartActivity(tx, transition, "worker-1", "build-1")
			if err == nil && !started {
				t.Fatal("StartActivity did not apply")
			}
			return err
		})
		// 写入方 B 拿着 A 提交之前的快照记录心跳。
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			recorded, err := execution.RecordActivityHeartbeat(staleTx{ExecutionTx: tx, activity: snapshot}, transition, map[string]any{"offset": int64(7)})
			if err == nil && recorded == nil {
				t.Fatal("RecordActivityHeartbeat did not apply")
			}
			return err
		})
		got, _, err := manager.GetActivityState(runID, 7)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != execution.ActivityStatusEnums.Started || got.WorkerIdentity != "worker-1" || got.StartedTime == nil || got.StartedEventID != execution.TransientEventID {
			t.Errorf("start was overwritten by the heartbeat writer: status=%s worker=%q started=%v startedEventID=%d", got.Status, got.WorkerIdentity, got.StartedTime, got.StartedEventID)
		}
		if got.LastHeartbeat["offset"] != int64(7) || got.LastHeartbeatAt == nil {
			t.Errorf("heartbeat missing: %#v at %v", got.LastHeartbeat, got.LastHeartbeatAt)
		}
		// 心跳只写 Activity state，不写 History。
		if events, err := manager.GetEvents(runID); err != nil || len(events) != 0 {
			t.Errorf("heartbeat wrote history: %+v err=%v", events, err)
		}
		if got.Input["order"] != "o-1" || got.RetryPolicy == nil || got.RetryPolicy.MaxAttempts != 3 || got.ActivityType != "Charge" {
			t.Errorf("fields nobody changed were lost: %+v", got)
		}
	})

	t.Run("Timer", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-timer"
		createContractRun(t, manager, runID)
		deadline := now.Add(time.Minute)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			return tx.UpsertTimerState(&execution.TimerState{
				RunID: runID, ScheduledEventID: 4, TimerID: "sleep-1", Deadline: deadline,
				Status: execution.TimerStatusEnums.Scheduled, Kind: execution.TimerKindEnums.UserTimer,
				TraceContext: map[string]string{"traceparent": "00-abc"}, CreatedAt: now, UpdatedAt: now,
			})
		})
		// 快照在 FireTimer 不碰的字段上与已存的行不同，相当于行在快照之后被别的写入方改过。
		snapshot, _, err := manager.GetTimerState(runID, 4)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.TimerID = "stale-timer-id"
		snapshot.Deadline = deadline.Add(time.Hour)

		// 没有经 setter 改动的 upsert 不改变已存的行。
		inTx(t, manager, func(tx execution.ExecutionTx) error { return tx.UpsertTimerState(snapshot) })
		if got, _, _ := manager.GetTimerState(runID, 4); got.TimerID != "sleep-1" || !got.Deadline.Equal(deadline) {
			t.Fatalf("upsert without changes rewrote the row: timer_id=%q deadline=%v", got.TimerID, got.Deadline)
		}

		inTx(t, manager, func(tx execution.ExecutionTx) error {
			fired, err := execution.FireTimer(staleTx{ExecutionTx: tx, timer: snapshot}, execution.TimerFire{
				RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: 4,
				Kind: execution.TimerKindEnums.UserTimer, Now: now.Add(time.Minute),
			})
			if err == nil && !fired {
				t.Fatal("FireTimer did not apply")
			}
			return err
		})
		got, _, err := manager.GetTimerState(runID, 4)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != execution.TimerStatusEnums.Fired || !got.UpdatedAt.Equal(now.Add(time.Minute)) {
			t.Errorf("fire not recorded: status=%s updated_at=%v", got.Status, got.UpdatedAt)
		}
		if got.TimerID != "sleep-1" || !got.Deadline.Equal(deadline) || got.TraceContext["traceparent"] != "00-abc" {
			t.Errorf("fields FireTimer does not change were overwritten from the snapshot: %+v", got)
		}
	})

	t.Run("ChildWorkflow", func(t *testing.T) {
		manager := newManager(t)
		const parentID = "run-parent"
		createContractRun(t, manager, parentID)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			return tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{
				RunID: parentID, ScheduledEventID: 5, ChildID: "child-1", ChildRunID: "run-child",
				WorkflowID: "child-flow", WorkflowConfigVersion: "v1",
				Status: execution.ChildWorkflowStatusEnums.Initiated, Input: map[string]any{"order": "o-1"},
				ParentClosePolicy: protocol.ParentClosePolicyRequestCancel,
				CreatedAt:         now, UpdatedAt: now,
			})
		})
		snapshot, _, err := manager.GetChildWorkflowState(parentID, 5)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.WorkflowConfigVersion = "stale-version"
		snapshot.Input = map[string]any{"order": "stale"}
		snapshot.ParentClosePolicy = protocol.ParentClosePolicyAbandon

		child := execution.NewChildRun(execution.ChildStart{
			ParentRunID: parentID, ChildID: "child-1", ChildRunID: "run-child", ScheduledEventID: 5,
			WorkflowID: "child-flow", ConfigVersion: "v1",
		}, now)
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			closed, err := execution.PropagateChildClose(staleTx{ExecutionTx: tx, child: snapshot}, execution.ChildClose{
				Child: child, ParentTreeID: parentID, ParentBranchID: parentID,
				Type: history.EventChildWorkflowCompleted, Output: map[string]any{"ok": true}, Now: now.Add(time.Second),
			})
			if err == nil && !closed {
				t.Fatal("PropagateChildClose did not apply")
			}
			return err
		})
		got, _, err := manager.GetChildWorkflowState(parentID, 5)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != execution.ChildWorkflowStatusEnums.Completed || got.Output["ok"] != true {
			t.Errorf("close not recorded: status=%s output=%#v", got.Status, got.Output)
		}
		if got.WorkflowConfigVersion != "v1" || got.Input["order"] != "o-1" || got.ParentClosePolicy != protocol.ParentClosePolicyRequestCancel {
			t.Errorf("fields the close does not change were overwritten from the snapshot: version=%q input=%#v policy=%q", got.WorkflowConfigVersion, got.Input, got.ParentClosePolicy)
		}
	})
	t.Run("Cancellation", func(t *testing.T) {
		manager := newManager(t)
		const runID = "run-cancellation"
		createContractRun(t, manager, runID)
		var queuedID, runningID, timerID, childID int64
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			var err error
			if queuedID, err = tx.Append(runID, runID, runID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "queued"})); err != nil {
				return err
			}
			if runningID, err = tx.Append(runID, runID, runID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "running"})); err != nil {
				return err
			}
			if timerID, err = tx.Append(runID, runID, runID, history.NewTimerScheduledEvent("#t1", nil, 1000)); err != nil {
				return err
			}
			if childID, err = tx.Append(runID, runID, runID, history.NewChildWorkflowStartedEvent("child-flow", "#c1", "run-child", "v1", nil)); err != nil {
				return err
			}
			for _, state := range []*execution.ActivityState{
				{RunID: runID, ScheduledEventID: queuedID, ActivityID: "queued", Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now, CreatedAt: now, UpdatedAt: now},
				{RunID: runID, ScheduledEventID: runningID, ActivityID: "running", Attempt: 1, Stamp: 1, StartedEventID: execution.TransientEventID, Status: execution.ActivityStatusEnums.Started, ScheduledTime: now, CreatedAt: now, UpdatedAt: now},
			} {
				if err := tx.UpsertActivityState(state); err != nil {
					return err
				}
			}
			if err := tx.UpsertTimerState(&execution.TimerState{RunID: runID, ScheduledEventID: timerID, TimerID: "#t1", Kind: execution.TimerKindEnums.UserTimer,
				Status: execution.TimerStatusEnums.Scheduled, Deadline: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}); err != nil {
				return err
			}
			return tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: runID, ScheduledEventID: childID, ChildID: "#c1", ChildRunID: "run-child",
				WorkflowID: "child-flow", Status: execution.ChildWorkflowStatusEnums.Started, CreatedAt: now, UpdatedAt: now})
		})
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			_, err = execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
				Run: run, TreeID: runID, BranchID: runID, Now: now.Add(time.Second),
				CloseEvent: history.NewWorkflowTaskCompletedEvent(1, "worker-1"),
				Commands: []history.Command{
					{Type: history.CmdRequestCancelActivity, ActivityID: "queued"},
					{Type: history.CmdRequestCancelActivity, ActivityID: "running"},
					{Type: history.CmdCancelTimer, TimerID: "#t1"},
					{Type: history.CmdRequestCancelChildWorkflow, ChildID: "#c1"},
				},
				CommandEvents: []history.Event{
					history.NewActivityCancelRequestedEvent("queued", 0),
					history.NewActivityCancelRequestedEvent("running", 0),
					history.NewTimerCancelledEvent("#t1", 0),
					history.NewChildWorkflowCancelRequestedEvent("#c1", "", 0),
				},
			})
			return err
		})
		if got, _, err := manager.GetActivityState(runID, queuedID); err != nil || got.Status != execution.ActivityStatusEnums.Cancelled || !got.CancelRequested {
			t.Errorf("queued activity = status %s requested %v (err %v), want cancelled", got.Status, got.CancelRequested, err)
		}
		if got, _, err := manager.GetActivityState(runID, runningID); err != nil || got.Status != execution.ActivityStatusEnums.Started || !got.CancelRequested {
			t.Errorf("running activity = status %s requested %v (err %v), want started with the request", got.Status, got.CancelRequested, err)
		}
		if got, _, err := manager.GetTimerState(runID, timerID); err != nil || got.Status != execution.TimerStatusEnums.Cancelled {
			t.Errorf("timer = %+v (err %v), want cancelled", got, err)
		}
		if got, _, err := manager.GetChildWorkflowState(runID, childID); err != nil || !got.CancelRequested {
			t.Errorf("child link = %+v (err %v), want CancelRequested", got, err)
		}
	})
	t.Run("RunCloseClosesScheduledTimers", func(t *testing.T) {
		manager := newManager(t)
		const runID, otherRunID = "run-close-timers", "run-close-timers-other"
		createContractRun(t, manager, runID)
		createContractRun(t, manager, otherRunID)
		timer := func(runID string, scheduledEventID int64, status execution.TimerStatus) *execution.TimerState {
			return &execution.TimerState{RunID: runID, ScheduledEventID: scheduledEventID, TimerID: fmt.Sprintf("#t%d", scheduledEventID),
				Kind: execution.TimerKindEnums.UserTimer, Status: status, Deadline: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
		}
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			for _, state := range []*execution.TimerState{
				timer(runID, 10, execution.TimerStatusEnums.Scheduled),
				timer(runID, 11, execution.TimerStatusEnums.Fired),
				timer(runID, 12, execution.TimerStatusEnums.Cancelled),
				timer(otherRunID, 10, execution.TimerStatusEnums.Scheduled),
			} {
				if err := tx.UpsertTimerState(state); err != nil {
					return err
				}
			}
			return nil
		})
		// 同一事务里先写入的 timer 也随 Run 关闭。
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			if err := tx.UpsertTimerState(timer(runID, 13, execution.TimerStatusEnums.Scheduled)); err != nil {
				return err
			}
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			inserted, err := execution.Terminate(tx, run, runID, runID, "done")
			if err == nil && !inserted {
				t.Fatal("Terminate did not insert")
			}
			return err
		})
		want := map[int64]execution.TimerStatus{
			10: execution.TimerStatusEnums.RunClosed, 11: execution.TimerStatusEnums.Fired,
			12: execution.TimerStatusEnums.Cancelled, 13: execution.TimerStatusEnums.RunClosed,
		}
		states, err := manager.ListTimerStates(runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(states) != len(want) {
			t.Fatalf("timer states = %d, want %d", len(states), len(want))
		}
		for _, state := range states {
			if state.Status != want[state.ScheduledEventID] {
				t.Errorf("timer %d status = %s, want %s", state.ScheduledEventID, state.Status, want[state.ScheduledEventID])
			}
			if state.Status == execution.TimerStatusEnums.RunClosed && !state.IsTerminal() {
				t.Errorf("timer %d: run_closed is not terminal", state.ScheduledEventID)
			}
		}
		if got, _, err := manager.GetTimerState(otherRunID, 10); err != nil || got.Status != execution.TimerStatusEnums.Scheduled {
			t.Errorf("another run's timer = %+v (err %v), want it to stay scheduled", got, err)
		}
	})
}

// executionStateProjection 是 execution state 里由 History 决定的部分，用于与 History 的折叠结果比较。
type executionStateProjection struct {
	NextEventID, LastEventID, HistoryEventCount, HistorySizeBytes, OpenWorkflowTaskEventID int64
	Status                                                                                 history.Status
	FailureReason                                                                          string
	SearchAttributes                                                                       map[string]string
	CurrentWaiting                                                                         execution.WaitingState
	OpenWaits                                                                              map[int64]execution.WaitingState
	Blocked                                                                                execution.BlockedInfo
	WorkflowTask                                                                           execution.WorkflowTaskProjection
	HasWorkflowStarted, CancelRequested                                                    bool
}

func projectionJSON(t *testing.T, state *execution.WorkflowExecutionState) string {
	t.Helper()
	data, err := json.Marshal(executionStateProjection{
		NextEventID: state.NextEventID, LastEventID: state.LastEventID,
		HistoryEventCount: state.HistoryEventCount, HistorySizeBytes: state.HistorySizeBytes,
		OpenWorkflowTaskEventID: state.OpenWorkflowTaskEventID, Status: state.Status, FailureReason: state.FailureReason,
		SearchAttributes: state.SearchAttributes, CurrentWaiting: state.CurrentWaiting, OpenWaits: state.OpenWaits,
		Blocked: state.Blocked, WorkflowTask: state.WorkflowTask,
		HasWorkflowStarted: state.HasWorkflowStarted, CancelRequested: state.CancelRequested,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// ExecutionStateContract 断言 execution state 的各条写入路径互不覆盖：sticky 分配、激活开始与事件追加
// 各自只写自己改的字段，每一步之后已存的投影都等于对 History 重新折叠的结果。
func ExecutionStateContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	const runID = "run-state"
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-state", WorkflowConfigVersion: "v1", CreatedAt: contractNow})
	createRun(t, manager, run)
	requireProjection := func(step string) *execution.WorkflowExecutionState {
		t.Helper()
		state, err := manager.GetWorkflowExecutionState(runID)
		if err != nil {
			t.Fatal(err)
		}
		events, err := manager.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := execution.BuildWorkflowExecutionState(run, events)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := projectionJSON(t, state), projectionJSON(t, rebuilt); got != want {
			t.Fatalf("after %s the stored projection differs from the History fold:\n got %s\nwant %s", step, got, want)
		}
		return state
	}
	appendEvent := func(step string, event history.Event) int64 {
		t.Helper()
		id, err := manager.Append(runID, runID, runID, event)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		requireProjection(step)
		return id
	}

	appendEvent("workflow started", history.NewWorkflowStartedEvent(map[string]any{"order": "o-1"}, runID, "", "wf-state", "flow", 0))
	scheduledID, inserted, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil || !inserted {
		t.Fatalf("schedule workflow task: inserted=%v err=%v", inserted, err)
	}
	state := requireProjection("workflow task scheduled")

	expires := time.Now().Add(time.Hour)
	stickyGeneration, assigned, err := manager.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-1", "build-1", expires)
	if err != nil || !assigned {
		t.Fatalf("assign sticky: assigned=%v err=%v", assigned, err)
	}
	begun, err := beginActivation(manager, runID, scheduledID, state.ExecutionGeneration, stickyGeneration, "activation-1")
	if err != nil || !begun {
		t.Fatalf("begin activation: begun=%v err=%v", begun, err)
	}
	appendEvent("search attributes", history.NewSearchAttributesUpsertedEvent(map[string]any{"customer": "c-1"}))
	appendEvent("signal", history.NewWorkflowSignaledEvent("approve", map[string]any{"by": "ops"}))
	appendEvent("cancel requested", history.NewWorkflowCancellationRequestedEvent("operator"))

	state = requireProjection("independent writes")
	if state.StickyWorkerIdentity != "worker-1" || state.StickyWorkerBuildID != "build-1" || !state.StickyValidAt(time.Now()) {
		t.Errorf("sticky assignment lost after later writes: %q/%q expires=%v", state.StickyWorkerIdentity, state.StickyWorkerBuildID, state.StickyExpiresAt)
	}
	if state.CurrentActivationID != "activation-1" || state.CurrentActivationAttempt != 1 {
		t.Errorf("activation lost after later writes: %q attempt=%d", state.CurrentActivationID, state.CurrentActivationAttempt)
	}
	if state.OpenWorkflowTaskEventID != scheduledID || state.SearchAttributes["customer"] != "c-1" || !state.CancelRequested {
		t.Errorf("projection fields lost: open_wft=%d attrs=%v cancel=%v", state.OpenWorkflowTaskEventID, state.SearchAttributes, state.CancelRequested)
	}

	// 失败的 Workflow Task 关闭 open 槽并写入 blocked 投影，sticky 仍保留。
	if _, _, err := manager.AppendWorkflowTaskClosed(runID, runID, runID, history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: scheduledID, Cause: "workflow_task_failed", Reason: "panic in workflow code",
	})); err != nil {
		t.Fatal(err)
	}
	state = requireProjection("workflow task failed")
	if !state.Blocked.Blocked || state.Blocked.ConsecutiveFailures != 1 || state.OpenWorkflowTaskEventID != 0 || state.CurrentActivationID != "" {
		t.Errorf("failed task projection: blocked=%+v open_wft=%d activation=%q", state.Blocked, state.OpenWorkflowTaskEventID, state.CurrentActivationID)
	}
	if state.StickyWorkerIdentity != "worker-1" {
		t.Errorf("sticky assignment lost when the workflow task closed: %q", state.StickyWorkerIdentity)
	}
	// 关闭之后才到达的重复投递不再为这个 WFT 开始激活，下一个 WFT 不会被陈旧的激活挡住。
	begun, err = beginActivation(manager, runID, scheduledID, state.ExecutionGeneration, state.StickyGeneration, "activation-late")
	if err != nil || begun {
		t.Fatalf("begin activation of a closed workflow task: begun=%v err=%v", begun, err)
	}
	if state = requireProjection("late activation rejected"); state.CurrentActivationID != "" {
		t.Errorf("late activation of a closed workflow task stayed current: %q", state.CurrentActivationID)
	}
	generation, cleared, err := manager.ClearStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration)
	if err != nil || !cleared || generation != state.StickyGeneration+1 {
		t.Fatalf("clear sticky: cleared=%v generation=%d err=%v", cleared, generation, err)
	}
	state = requireProjection("sticky cleared")
	if state.StickyWorkerIdentity != "" || state.StickyExpiresAt != nil || !state.Blocked.Blocked {
		t.Errorf("after clearing sticky: identity=%q expires=%v blocked=%+v", state.StickyWorkerIdentity, state.StickyExpiresAt, state.Blocked)
	}

	// 领取 WFT 记下 start-to-close 截止时间；超时关闭它时清除截止时间并结束 sticky 会话，sticky 代数不变。
	retryID, inserted, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil || !inserted {
		t.Fatalf("schedule retry workflow task: inserted=%v err=%v", inserted, err)
	}
	state = requireProjection("retry workflow task scheduled")
	if _, assigned, err := manager.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-1", "build-1", expires); err != nil || !assigned {
		t.Fatalf("assign sticky again: assigned=%v err=%v", assigned, err)
	}
	startedAt := contractNow.Add(time.Minute)
	started := history.NewWorkflowTaskStartedEvent(retryID, "worker-1", "build-1", 0)
	started.Timestamp = startedAt
	started.WorkflowTaskStarted.TimeoutMs = 10_000
	appendEvent("workflow task started", started)
	state = requireProjection("workflow task started")
	if id, deadline, ok := state.WorkflowTaskTimeoutDeadline(); !ok || id != retryID || !deadline.Equal(startedAt.Add(10*time.Second)) {
		t.Errorf("start-to-close deadline: id=%d deadline=%v ok=%v, want %d %v", id, deadline, ok, retryID, startedAt.Add(10*time.Second))
	}
	stickyGeneration = state.StickyGeneration
	if _, _, err := manager.AppendWorkflowTaskClosed(runID, runID, runID, history.NewWorkflowTaskTimedOutEvent(retryID, "workflow task start-to-close timeout")); err != nil {
		t.Fatal(err)
	}
	state = requireProjection("workflow task timed out")
	if _, _, ok := state.WorkflowTaskTimeoutDeadline(); ok || state.OpenWorkflowTaskEventID != 0 {
		t.Errorf("timed-out workflow task: open_wft=%d deadline kept=%v", state.OpenWorkflowTaskEventID, ok)
	}
	if state.StickyWorkerIdentity != "" || state.StickyExpiresAt != nil || state.StickyGeneration != stickyGeneration {
		t.Errorf("sticky after the timeout: identity=%q expires=%v generation=%d, want cleared at generation %d",
			state.StickyWorkerIdentity, state.StickyExpiresAt, state.StickyGeneration, stickyGeneration)
	}
}

// beginActivation 在一个只开始激活的事务里调用 BeginWorkflowActivation。
func beginActivation(manager execution.ExecutionManager, runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error) {
	begun := false
	err := manager.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		begun, err = tx.BeginWorkflowActivation(runID, scheduledEventID, executionGeneration, stickyGeneration, activationID)
		return err
	})
	return begun, err
}

// WorkflowTaskStartContract 固定 Workflow Task 领取的原子性：激活与 WorkflowTaskStarted 同一事务提交，
// 事务失败时两者都不留下；激活 fence 照常拒绝另一份正在进行的激活与已关闭任务的激活。
func WorkflowTaskStartContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	const runID = "run-wft-start"
	createContractRun(t, manager, runID)
	if _, err := manager.Append(runID, runID, runID, history.NewWorkflowStartedEvent(nil, runID, "", "wf-"+runID, "flow", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, _, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil {
		t.Fatal(err)
	}
	state, err := manager.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := contractNow.Add(time.Minute)
	start := func(activationID string, after func() error) (bool, error) {
		begun := false
		err := manager.RunInTx(func(tx execution.ExecutionTx) error {
			result, err := execution.StartWorkflowTask(tx, execution.WorkflowTaskStart{
				RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: scheduledID,
				ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
				ActivationID: activationID, WorkerIdentity: "worker-1", ProgramVersion: "v1", Timeout: 10 * time.Second, Now: startedAt,
			})
			begun = result.Started
			if err != nil || after == nil {
				return err
			}
			return after()
		})
		return begun, err
	}

	// Error Case：事务在写入 started 之后失败，激活与 started 一起回滚。
	failure := errors.New("injected failure after start")
	if _, err := start("activation-lost", func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("start with injected failure: err=%v", err)
	}
	state, err = manager.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentActivationID != "" || state.WorkflowTask.StartedScheduledEventID == scheduledID {
		t.Fatalf("rolled-back start left state behind: activation=%q started=%d", state.CurrentActivationID, state.WorkflowTask.StartedScheduledEventID)
	}

	// Happy Path：另一份投递随后正常开始，激活、Started 事件与截止时间一起写入。
	if begun, err := start("activation-1", nil); err != nil || !begun {
		t.Fatalf("start after rollback: begun=%v err=%v", begun, err)
	}
	state, err = manager.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if id, deadline, ok := state.WorkflowTaskTimeoutDeadline(); state.CurrentActivationID != "activation-1" || !ok || id != scheduledID || !deadline.Equal(startedAt.Add(10*time.Second)) {
		t.Fatalf("started task: activation=%q deadline=%v ok=%v", state.CurrentActivationID, deadline, ok)
	}

	// Edge Case：另一份激活正在进行时，第二份被拒绝，不追加事件。
	events, err := manager.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if begun, err := start("activation-2", nil); err != nil || begun {
		t.Fatalf("second activation: begun=%v err=%v", begun, err)
	}
	after, err := manager.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(events) {
		t.Fatalf("rejected activation appended events: %d -> %d", len(events), len(after))
	}

	// Edge Case：任务关闭之后到达的重复投递不再开始激活。
	if _, _, err := manager.AppendWorkflowTaskClosed(runID, runID, runID, history.NewWorkflowTaskTimedOutEvent(scheduledID, "timeout")); err != nil {
		t.Fatal(err)
	}
	if begun, err := start("activation-late", nil); err != nil || begun {
		t.Fatalf("activation of a closed task: begun=%v err=%v", begun, err)
	}
}

// BranchToken 返回 Run 自己的 History branch（tree 与 branch 都是 runID）的 token，供测试直接构造的 Run 使用。
func BranchToken(runID string) []byte {
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{TreeID: runID, BranchID: runID})
	if err != nil {
		panic(err)
	}
	return token
}

// UpdateRun 读出 Run，交给 mutate 经 Run 的方法修改，再在一个事务里写回。
func UpdateRun(t testing.TB, mgr execution.ExecutionManager, runID string, mutate func(run *execution.Run) error) {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if err := mutate(run); err != nil {
			return err
		}
		return tx.UpdateRun(run)
	}); err != nil {
		t.Fatal(err)
	}
}
