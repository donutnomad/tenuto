package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 取消状态机
// ============================================
// 测试策略（表驱动）:
// - RequestCancelActivity × Activity 状态（未开始、重试退避、已开始、已结束）→ 事件与状态
// - 取消请求之后的失败上报：确认取消、普通失败、没有取消请求的 CanceledError
// - CancelTimer、RequestCancelChildWorkflow 的状态迁移；ChildCancelPending 的判定
// ============================================

const cancelRunID = "run-cancel"

// storeWithScheduledActivity 返回已调度 charge（ActivityScheduled 事件 ID 为返回值）且 Activity 状态为 state 的存储。
func storeWithScheduledActivity(t *testing.T, state execution.ActivityState) (*memory.ExecutionManager, int64) {
	t.Helper()
	store := newStoreWithRun(t, cancelRunID)
	if _, err := store.Append(cancelRunID, cancelRunID, cancelRunID, history.NewWorkflowStartedEvent(nil, cancelRunID, "", "workflow", "Workflow", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := store.Append(cancelRunID, cancelRunID, cancelRunID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}))
	if err != nil {
		t.Fatal(err)
	}
	state.RunID, state.ScheduledEventID, state.ActivityID, state.ActivityType = cancelRunID, scheduledID, "charge", "charge"
	if err := store.UpsertActivityState(&state); err != nil {
		t.Fatal(err)
	}
	return store, scheduledID
}

func commitCommands(t *testing.T, store *memory.ExecutionManager, commands []history.Command, events []history.Event) execution.WorkflowTaskCompletionResult {
	t.Helper()
	commit := execution.WorkflowTaskCommit{
		Run: mustGetRun(t, store, cancelRunID), TreeID: cancelRunID, BranchID: cancelRunID,
		CloseEvent: history.NewWorkflowTaskCompletedEvent(1, "worker-1"), CommandEvents: events, Commands: commands, Now: time.Now(),
	}
	var result execution.WorkflowTaskCompletionResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.CommitWorkflowTask(tx, commit)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRequestCancelActivityByState(t *testing.T) {
	backoff := time.Now().Add(time.Minute)
	cases := []struct {
		name          string
		state         execution.ActivityState
		wantStatus    execution.ActivityStatus
		wantCancelled bool // ActivityCancelled 随请求写入
		wantRequested bool // ActivityState.CancelRequested
	}{
		{"scheduled", execution.ActivityState{Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled}, execution.ActivityStatusEnums.Cancelled, true, true},
		{"retry backoff", execution.ActivityState{Attempt: 2, Stamp: 2, Status: execution.ActivityStatusEnums.Scheduled, NextAttemptAt: &backoff}, execution.ActivityStatusEnums.Cancelled, true, true},
		{"started", execution.ActivityState{Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started, StartedEventID: execution.TransientEventID}, execution.ActivityStatusEnums.Started, false, true},
		{"completed", execution.ActivityState{Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Completed}, execution.ActivityStatusEnums.Completed, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, scheduledID := storeWithScheduledActivity(t, tc.state)
			result := commitCommands(t, store,
				[]history.Command{{Type: history.CmdRequestCancelActivity, ActivityID: "charge"}},
				[]history.Event{history.NewActivityCancelRequestedEvent("charge", 0)})
			events, err := store.GetEvents(cancelRunID)
			if err != nil {
				t.Fatal(err)
			}
			var requested, cancelled *history.Event
			for i := range events {
				switch {
				case events[i].ActivityCancelRequested != nil:
					requested = &events[i]
				case events[i].ActivityCancelled != nil:
					cancelled = &events[i]
				}
			}
			if requested == nil || requested.ActivityCancelRequested.ScheduledEventID != scheduledID {
				t.Fatalf("request event = %+v, want it to reference %d", requested, scheduledID)
			}
			if (cancelled != nil) != tc.wantCancelled || (len(result.CancelledActivities) == 1) != tc.wantCancelled {
				t.Fatalf("cancelled event = %+v, result = %+v, want cancelled=%v", cancelled, result.CancelledActivities, tc.wantCancelled)
			}
			if cancelled != nil && cancelled.ActivityCancelled.CancelRequestedEventID != requested.ID {
				t.Fatalf("cancelled event = %+v, want it to reference request %d", cancelled.ActivityCancelled, requested.ID)
			}
			state, _, err := store.GetActivityState(cancelRunID, scheduledID)
			if err != nil || state.Status != tc.wantStatus || state.CancelRequested != tc.wantRequested {
				t.Fatalf("state = status %s requested %v, want %s %v (err %v)", state.Status, state.CancelRequested, tc.wantStatus, tc.wantRequested, err)
			}
		})
	}
}

// 取消请求之后的失败上报：CanceledError 是取消确认，其余失败不再重试；没有取消请求时 CanceledError 只是普通失败。
func TestFailCancelRequestedActivity(t *testing.T) {
	retry := &history.RetryPolicy{MaxAttempts: 5}
	canceled := protocol.Failure{Message: "canceled", Type: protocol.CanceledFailureType, NonRetryable: true}
	cases := []struct {
		name            string
		cancelRequested bool
		failure         protocol.Failure
		wantStatus      execution.ActivityStatus
		wantEvent       history.EventType
		wantRetryState  string
	}{
		{"acknowledged", true, canceled, execution.ActivityStatusEnums.Cancelled, history.EventActivityCancelled, ""},
		{"other failure", true, protocol.Failure{Message: "temporary"}, execution.ActivityStatusEnums.Failed, history.EventActivityFailed, execution.ActivityCancelRetryState},
		{"canceled error without request", false, protocol.Failure{Message: "canceled", Type: protocol.CanceledFailureType}, execution.ActivityStatusEnums.Scheduled, history.EventActivityAttemptFailed, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, scheduledID := storeWithScheduledActivity(t, execution.ActivityState{
				Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started, StartedEventID: execution.TransientEventID,
				RetryPolicy: retry, CancelRequested: tc.cancelRequested,
			})
			transition := execution.ActivityTransition{RunID: cancelRunID, TreeID: cancelRunID, BranchID: cancelRunID, ScheduledEventID: scheduledID, Attempt: 1, Stamp: 1, Now: time.Now()}
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				_, err := execution.FailActivity(tx, transition, tc.failure)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			state, _, err := store.GetActivityState(cancelRunID, scheduledID)
			if err != nil || state.Status != tc.wantStatus {
				t.Fatalf("state status = %s, want %s (err %v)", state.Status, tc.wantStatus, err)
			}
			events, err := store.GetEvents(cancelRunID)
			if err != nil {
				t.Fatal(err)
			}
			last := events[len(events)-1]
			if last.EventType() != tc.wantEvent {
				t.Fatalf("last event = %s, want %s", last.EventType(), tc.wantEvent)
			}
			if last.ActivityFailed != nil && last.ActivityFailed.RetryState != tc.wantRetryState {
				t.Fatalf("retry state = %q, want %q", last.ActivityFailed.RetryState, tc.wantRetryState)
			}
		})
	}
}

func TestCancelTimerAndChildCommands(t *testing.T) {
	store := newStoreWithRun(t, cancelRunID)
	if _, err := store.Append(cancelRunID, cancelRunID, cancelRunID, history.NewWorkflowStartedEvent(nil, cancelRunID, "", "workflow", "Workflow", 0)); err != nil {
		t.Fatal(err)
	}
	timerID, err := store.Append(cancelRunID, cancelRunID, cancelRunID, history.NewTimerScheduledEvent("#t1", nil, 1000))
	if err != nil {
		t.Fatal(err)
	}
	childEvent := history.NewChildWorkflowStartedEvent("child", "#c1", "child-run", "v1", nil)
	childID, err := store.Append(cancelRunID, cancelRunID, cancelRunID, childEvent)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		if err := tx.UpsertTimerState(&execution.TimerState{RunID: cancelRunID, ScheduledEventID: timerID, TimerID: "#t1", Kind: execution.TimerKindEnums.UserTimer, Status: execution.TimerStatusEnums.Scheduled, Deadline: now.Add(time.Second)}); err != nil {
			return err
		}
		return tx.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: cancelRunID, ScheduledEventID: childID, ChildID: "#c1", ChildRunID: "child-run", Status: execution.ChildWorkflowStatusEnums.Started})
	}); err != nil {
		t.Fatal(err)
	}
	commitCommands(t, store,
		[]history.Command{{Type: history.CmdCancelTimer, TimerID: "#t1"}, {Type: history.CmdRequestCancelChildWorkflow, ChildID: "#c1"}},
		[]history.Event{history.NewTimerCancelledEvent("#t1", 0), history.NewChildWorkflowCancelRequestedEvent("#c1", "", 0)})

	timer, _, err := store.GetTimerState(cancelRunID, timerID)
	if err != nil || timer.Status != execution.TimerStatusEnums.Cancelled || !timer.IsTerminal() {
		t.Fatalf("timer = %+v err=%v, want cancelled", timer, err)
	}
	link, _, err := store.GetChildWorkflowState(cancelRunID, childID)
	if err != nil || !link.CancelRequested {
		t.Fatalf("child link = %+v err=%v, want CancelRequested", link, err)
	}
	events, err := store.GetEvents(cancelRunID)
	if err != nil {
		t.Fatal(err)
	}
	var timerCancelled, childRequested bool
	for _, event := range events {
		if event.TimerCancelled != nil && event.TimerCancelled.ScheduledEventID == timerID {
			timerCancelled = true
		}
		if event.ChildWorkflowCancelRequested != nil && event.ChildWorkflowCancelRequested.ScheduledEventID == childID &&
			event.ChildWorkflowCancelRequested.ChildRunID == "child-run" {
			childRequested = true
		}
	}
	if !timerCancelled || !childRequested {
		t.Fatalf("history = %+v, want TimerCancelled and ChildWorkflowCancelRequested with resolved ids", events)
	}
	state, err := store.GetWorkflowExecutionState(cancelRunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, open := state.OpenWaits[timerID]; open {
		t.Fatal("cancelled timer still an open wait")
	}
}

func TestChildCancelPending(t *testing.T) {
	running := &execution.WorkflowExecutionState{Status: history.StatusRunning}
	requestedChild := &execution.WorkflowExecutionState{Status: history.StatusRunning, CancelRequested: true}
	closedChild := &execution.WorkflowExecutionState{Status: history.StatusCompleted}
	requested := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Started, CancelRequested: true}
	notRequested := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Started}
	closedLink := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Cancelled, CancelRequested: true}
	cases := []struct {
		name  string
		link  *execution.ChildWorkflowState
		child *execution.WorkflowExecutionState
		want  bool
	}{
		{"requested, child running", requested, running, true},
		{"requested, child already has the request", requested, requestedChild, false},
		{"requested, child closed", requested, closedChild, false},
		{"not requested", notRequested, running, false},
		{"link closed", closedLink, running, false},
	}
	for _, tc := range cases {
		if got := execution.ChildCancelPending(tc.link, tc.child); got != tc.want {
			t.Errorf("%s: ChildCancelPending = %v, want %v", tc.name, got, tc.want)
		}
	}
}
