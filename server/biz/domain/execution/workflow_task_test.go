package execution

import (
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

type workflowTaskTestTx struct {
	close history.Event
	count int
}

func (t *workflowTaskTestTx) AppendWorkflowTaskClosed(_ string, _, _ string, event history.Event) (int64, bool, error) {
	if t.close.EventType() != "" && t.close.FlatScheduledEventID() == event.FlatScheduledEventID() {
		return t.close.ID, false, nil
	}
	t.count++
	event.ID = int64(t.count)
	t.close = event
	return event.ID, true, nil
}

func (t *workflowTaskTestTx) AppendIdempotent(string, string, string, history.Event) (int64, bool, error) {
	return 0, false, nil
}
func (t *workflowTaskTestTx) Append(string, string, string, history.Event) (int64, error) {
	return 0, nil
}
func (t *workflowTaskTestTx) AppendWorkflowTaskCompleted(string, string, string, history.Event, []history.Event) (WorkflowTaskCompletionResult, error) {
	return WorkflowTaskCompletionResult{}, nil
}
func (t *workflowTaskTestTx) UpsertActivityState(*ActivityState) error { return nil }
func (t *workflowTaskTestTx) UpdateActivityState(*ActivityState) error { return nil }
func (t *workflowTaskTestTx) GetActivityStateForUpdate(string, int64) (*ActivityState, bool, error) {
	return nil, false, nil
}
func (t *workflowTaskTestTx) UpsertTimerState(*TimerState) error { return nil }
func (t *workflowTaskTestTx) UpdateTimerState(*TimerState) error { return nil }
func (t *workflowTaskTestTx) CloseScheduledTimers(string) error  { return nil }
func (t *workflowTaskTestTx) GetTimerStateForUpdate(string, int64) (*TimerState, bool, error) {
	return nil, false, nil
}
func (t *workflowTaskTestTx) UpsertChildWorkflowState(*ChildWorkflowState) error { return nil }
func (t *workflowTaskTestTx) GetChildWorkflowStateForUpdate(string, int64) (*ChildWorkflowState, bool, error) {
	return nil, false, nil
}
func (t *workflowTaskTestTx) CreateRun(*Run) error { return nil }
func (t *workflowTaskTestTx) UpdateRun(*Run) error { return nil }
func (t *workflowTaskTestTx) GetRunForUpdate(runID string) (*Run, error) {
	return nil, fmt.Errorf("run %s: %w", runID, ErrRunNotFound)
}
func (t *workflowTaskTestTx) SyncRunSearchAttributes(string) error { return nil }
func (t *workflowTaskTestTx) GetWorkflowExecutionStateForUpdate(string) (*WorkflowExecutionState, error) {
	return &WorkflowExecutionState{}, nil
}
func (t *workflowTaskTestTx) SetResetRunID(string, string) error { return nil }
func (t *workflowTaskTestTx) BeginWorkflowActivation(string, int64, int64, int64, string) (bool, error) {
	return true, nil
}
func (t *workflowTaskTestTx) HistoryLimits() history.HistoryLimits {
	return history.DefaultHistoryLimits()
}
func (t *workflowTaskTestTx) SetCurrentRunCAS(string, string, string, string) (bool, error) {
	return true, nil
}

func TestWorkflowTaskFailureRules(t *testing.T) {
	if got := WorkflowTaskFailureBackoff(1, time.Second, time.Minute); got != time.Second {
		t.Fatalf("first backoff = %s", got)
	}
	if got := WorkflowTaskFailureBackoff(10, time.Second, time.Minute); got != time.Minute {
		t.Fatalf("capped backoff = %s", got)
	}
	if got := WorkflowTaskFailureBackoff(1, 0, 0); got != DefaultWorkflowTaskFailureBackoffBase {
		t.Fatalf("default backoff = %s", got)
	}
}

func projectTestState(t *testing.T, events ...history.Event) *WorkflowExecutionState {
	t.Helper()
	state, err := BuildWorkflowExecutionState(&Run{ID: "run", Status: history.StatusRunning}, events)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// Happy Path：execution state 投影出 open WFT、失败退避与待处理事实，读取方不扫 history。
func TestWorkflowTaskStateProjection(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 2, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 1, ProgramVersion: "v1"}},
		{ID: 3, WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: 1, Cause: "nondeterministic", TraceContext: map[string]string{"trace": "first"}}},
		{ID: 4, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
	}
	state := projectTestState(t, events...)
	if state.WorkflowTaskOpen(1) || !state.WorkflowTaskOpen(4) {
		t.Fatal("workflow task open projection disagrees with history")
	}
	if got := state.CurrentWorkflowTaskScheduledEventID(); got != 4 {
		t.Fatalf("current scheduled event = %d, want 4", got)
	}
	if got := state.WorkflowTaskProgramVersion(1); got != "v1" {
		t.Fatalf("started program version = %q, want v1", got)
	}
	if state.Blocked.Blocked || state.NeedsWorkflowTask() {
		t.Fatal("open workflow task must take precedence over failure recovery")
	}
	state.WorkflowTask.LastFailureTraceContext["trace"] = "changed"
	if events[2].WorkflowTaskFailed.TraceContext["trace"] != "first" {
		t.Fatal("failure trace context shares the History map")
	}

	events = append(events, history.Event{ID: 5, WorkflowTaskTimedOut: &history.WorkflowTaskTimedOutEvent{ScheduledEventID: 4}})
	state = projectTestState(t, events...)
	if got := state.CurrentWorkflowTaskScheduledEventID(); got != 1 {
		t.Fatalf("last started fallback = %d, want 1", got)
	}
	if state.Blocked.Blocked || state.WorkflowTask.LastFailedAt != nil {
		t.Fatal("timeout must break the failed workflow task streak")
	}
	events = append(events, history.Event{ID: 6, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 10}})
	if !projectTestState(t, events...).NeedsWorkflowTask() {
		t.Fatal("activity completion after the last task must require activation")
	}
}

func TestBlockedProjectionUsesFullHistory(t *testing.T) {
	first := time.Unix(10, 0)
	second := time.Unix(20, 0)
	events := []history.Event{
		{ID: 1, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 2, Timestamp: first, WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: 1, Cause: "first"}},
		{ID: 3, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 4, Timestamp: second, WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: 3, Cause: "latest", Reason: "retry"}},
	}
	state := projectTestState(t, events...)
	blocked := state.BlockedInfo()
	if !blocked.Blocked || blocked.ConsecutiveFailures != 2 || blocked.Cause != "latest" || blocked.Reason != "retry" || blocked.Since == nil || !blocked.Since.Equal(first) {
		t.Fatalf("blocked projection = %+v", blocked)
	}
	if state.WorkflowTask.LastFailedAt == nil || !state.WorkflowTask.LastFailedAt.Equal(second) {
		t.Fatalf("last failed at = %v, want %v", state.WorkflowTask.LastFailedAt, second)
	}
	events = append(events, history.Event{ID: 5, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}})
	if projectTestState(t, events...).BlockedInfo().Blocked {
		t.Fatal("new open task must clear blocked visibility")
	}
}

// Edge Case: 连续失败中被终止的 Run 不再显示为 Blocked，blocked_only 列表也不再返回它。
func TestClosedRunIsNotBlocked(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 2, Timestamp: time.Unix(10, 0), WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: 1, Cause: "activity_route_unavailable"}},
	}
	if !projectTestState(t, events...).BlockedInfo().Blocked {
		t.Fatal("failed workflow task must block the run")
	}
	events = append(events, history.Event{ID: 3, WorkflowTerminated: &history.WorkflowTerminatedEvent{Reason: "operator"}})
	state := projectTestState(t, events...)
	if state.Blocked != (BlockedInfo{}) {
		t.Fatalf("blocked = %+v, want cleared after the run closed", state.Blocked)
	}
}

// Edge Case: Reset 写在新 Run 开头的标记表示「这个 Workflow Task 由新 Run 重做」，不计入连续失败，
// 新 Run 不显示为 Blocked，并且有待处理的事实等着下一个 Workflow Task。
func TestResetMarkerIsNotCountedAsWorkflowTaskFailure(t *testing.T) {
	events := []history.Event{
		history.NewWorkflowStartedEvent(nil, "base-run", "", "workflow", "Workflow", 0),
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(2, "worker-1", "", 0),
		history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
			ScheduledEventID: 2, StartedEventID: 3,
			Cause: WorkflowTaskFailedCauseResetWorkflow, BaseRunID: "base-run", ResetRunID: "reset-run",
		}),
	}
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	state, err := BuildWorkflowExecutionState(&Run{ID: "reset-run", Status: history.StatusRunning}, events)
	if err != nil {
		t.Fatal(err)
	}
	if state.Blocked.Blocked || state.Blocked.ConsecutiveFailures != 0 {
		t.Fatalf("blocked = %+v, want a fresh reset run with no failure streak", state.Blocked)
	}
	if !state.IsReset {
		t.Fatal("the reset marker must mark the run as reset")
	}
	if !state.NeedsWorkflowTask() {
		t.Fatal("the reset run must be waiting for its next workflow task")
	}
	if state.WorkflowTask.LastFailedAt != nil {
		t.Fatalf("last failed at = %v, want none", state.WorkflowTask.LastFailedAt)
	}
}

// Happy Path: CurrentWaiting 取 open 等待里调度事件 ID 最大的一项；每项的 ScheduledEventID 就是它在 OpenWaits 里的键，
// 关闭最新一项后回到次新的一项。
func TestCurrentWaitingIsTheLatestOpenWait(t *testing.T) {
	run := NewRun(NewRunOptions{ID: "run-waiting", WorkflowID: "wf", WorkflowType: "wf", WorkflowConfigVersion: "v1"})
	events := []history.Event{
		history.NewWorkflowStartedEvent(nil, run.ID, "", "wf", "Flow", 0),
		history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}),
		history.NewChildWorkflowStartedEvent("child", "child-1", "run-child", "v1", nil),
		history.NewTimerScheduledEvent("sleep", nil, 1000),
	}
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	state, err := BuildWorkflowExecutionState(run, events)
	if err != nil {
		t.Fatal(err)
	}
	for id, waiting := range state.OpenWaits {
		if waiting.ScheduledEventID != id {
			t.Fatalf("open wait keyed %d records scheduled event %d", id, waiting.ScheduledEventID)
		}
	}
	if state.CurrentWaiting.Type != WaitingTimer || state.CurrentWaiting.ScheduledEventID != 4 {
		t.Fatalf("current waiting = %+v, want the timer scheduled at 4", state.CurrentWaiting)
	}
	fired := history.NewTimerFiredEvent("sleep", 4)
	fired.ID = 5
	size, err := history.CanonicalEventSize(fired)
	if err != nil {
		t.Fatal(err)
	}
	ApplyEventToExecutionState(state, fired, size)
	if state.CurrentWaiting.Type != WaitingChildWorkflow || state.CurrentWaiting.ScheduledEventID != 3 {
		t.Fatalf("current waiting after the timer fired = %+v, want the child started at 3", state.CurrentWaiting)
	}
}

// WFT 完成时，Worker 没看到的事实（ID 大于 SeenThroughEventID）仍待处理：Run 需要下一个 WFT。
func TestWorkflowTaskCompletedKeepsUnseenFactsPending(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 3, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 2}},
		{ID: 4, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "late"}},
	}
	state := projectTestState(t, events...)
	if got := state.WorkflowTaskStartedEventID(2); got != 3 {
		t.Fatalf("started event = %d, want 3", got)
	}
	if !state.UnseenFacts(3) || state.UnseenFacts(4) {
		t.Fatal("unseen facts disagree with the signal at event 4")
	}
	seenAll := projectTestState(t, append(events, history.Event{ID: 5, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 2, SeenThroughEventID: 4}})...)
	if seenAll.NeedsWorkflowTask() {
		t.Fatal("a completion that saw the signal must not ask for another workflow task")
	}
	unseen := projectTestState(t, append(events, history.Event{ID: 5, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 2, SeenThroughEventID: 3}})...)
	if !unseen.NeedsWorkflowTask() {
		t.Fatal("the signal the completion did not see must still need a workflow task")
	}
}
