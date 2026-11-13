package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// newStoreWithRun 返回已建好 runID 这条 Run 的内存存储；append 只接受已存在的 Run。
func newStoreWithRun(t *testing.T, runID string) *memory.ExecutionManager {
	t.Helper()
	store := memory.NewExecutionManager()
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "workflow", WorkflowConfigVersion: "v1"}))
	}); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestCompleteActivityBackfillsTransientStartAtomically(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	startedAt := time.Now().Add(-time.Second)
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: execution.TransientEventID, StartedTime: &startedAt,
	}); err != nil {
		t.Fatal(err)
	}
	completion := execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now()}
	var accepted bool
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		accepted, err = execution.CompleteActivity(tx, completion, map[string]any{"paid": true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("current activity completion was skipped")
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 2 || events[0].ActivityStarted == nil || events[1].ActivityCompleted == nil || events[1].ActivityCompleted.StartedEventID != events[0].ID {
		t.Fatalf("completion history = %+v, err = %v", events, err)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Status != execution.ActivityStatusEnums.Completed || state.StartedEventID != events[0].ID {
		t.Fatalf("completed state = %+v, ok = %v, err = %v", state, ok, err)
	}
	stale := completion
	stale.Stamp = 0
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		accepted, err = execution.CompleteActivity(tx, stale, nil)
		return err
	}); err != nil || accepted {
		t.Fatalf("stale completion accepted = %v, err = %v", accepted, err)
	}
}

func TestFailActivityAdvancesStampThenCloses(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: execution.TransientEventID,
		RetryPolicy:    &history.RetryPolicy{MaxAttempts: 2},
	}); err != nil {
		t.Fatal(err)
	}
	completion := execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now()}
	var result execution.ActivityFailureResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.FailActivity(tx, completion, protocol.Failure{Message: "temporary"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result.Terminal || result.RetryState == nil || result.RetryState.Attempt != 2 || result.RetryState.Stamp != 2 || result.RetryState.StartedEventID != execution.EmptyEventID {
		t.Fatalf("retry state = %+v", result)
	}
	completion.Attempt, completion.Stamp = 2, 2
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.FailActivity(tx, completion, protocol.Failure{Message: "still failing"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Terminal || result.RetryState != nil {
		t.Fatalf("terminal result = %+v", result)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Status != execution.ActivityStatusEnums.Failed || state.Stamp != 2 {
		t.Fatalf("failed state = %+v, ok = %v, err = %v", state, ok, err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 3 || events[0].ActivityStarted == nil || events[1].ActivityAttemptFailed == nil || events[2].ActivityFailed == nil || events[2].ActivityFailed.RetryState != "MaximumAttemptsReached" {
		t.Fatalf("failure history = %+v, err = %v", events, err)
	}
}

func TestStartAndHeartbeatActivityRespectAttemptStamp(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 4, Status: execution.ActivityStatusEnums.Scheduled,
		StartedEventID: execution.EmptyEventID, RetryPolicy: &history.RetryPolicy{MaxAttempts: 1},
	}); err != nil {
		t.Fatal(err)
	}
	progress := execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 4, Now: time.Now()}
	var accepted bool
	start := func() error {
		return store.RunInTx(func(tx execution.ExecutionTx) error {
			var err error
			accepted, err = execution.StartActivity(tx, progress, "worker-1", "build-3")
			return err
		})
	}
	if err := start(); err != nil || !accepted {
		t.Fatalf("first start accepted = %v, err = %v", accepted, err)
	}
	if err := start(); err != nil || accepted {
		t.Fatalf("duplicate start accepted = %v, err = %v", accepted, err)
	}
	stale := progress
	stale.Stamp = 3
	var recorded *execution.ActivityState
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		recorded, err = execution.RecordActivityHeartbeat(tx, stale, map[string]any{"step": 1})
		return err
	}); err != nil || recorded != nil {
		t.Fatalf("stale heartbeat recorded = %+v, err = %v", recorded, err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		recorded, err = execution.RecordActivityHeartbeat(tx, progress, map[string]any{"step": 2})
		return err
	}); err != nil || recorded == nil || recorded.LastHeartbeatAt == nil || !recorded.LastHeartbeatAt.Equal(progress.Now) {
		t.Fatalf("current heartbeat recorded = %+v, err = %v", recorded, err)
	}
	// 心跳只记在 Activity state 上，History 里只有 ActivityStarted。
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 1 || events[0].ActivityStarted == nil {
		t.Fatalf("progress history = %+v, err = %v", events, err)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Status != execution.ActivityStatusEnums.Started || state.LastHeartbeat["step"] != 2 ||
		state.LastHeartbeatAt == nil || !state.LastHeartbeatAt.Equal(progress.Now) {
		t.Fatalf("progress state = %+v, ok = %v, err = %v", state, ok, err)
	}
}

func TestResetActivityAdvancesStampAndClearsHeartbeat(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	heartbeatAt := time.Now().Add(-time.Minute)
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 3, Stamp: 8, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: 12, LastHeartbeat: map[string]any{"step": 2}, LastHeartbeatAt: &heartbeatAt,
	}); err != nil {
		t.Fatal(err)
	}
	reset := execution.ActivityReset{
		ActivityTransition: execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Now: time.Now()},
		Identity:           "operator", Reason: "retry", ResetHeartbeat: true,
	}
	var next *execution.ActivityState
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		next, err = execution.ResetActivity(tx, reset)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if next.Attempt != 1 || next.Stamp != 9 || next.StartedEventID != execution.EmptyEventID || next.LastHeartbeat != nil || !next.ActivityReset {
		t.Fatalf("reset state = %+v", next)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 1 || events[0].ActivityReset == nil || events[0].ActivityReset.Attempt != 3 {
		t.Fatalf("reset history = %+v, err = %v", events, err)
	}
}

func TestTimeoutActivityCommitsTimeoutAndRetryTogether(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 2, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: execution.TransientEventID, HeartbeatTimeoutMs: 1,
		ScheduledTime: time.Now().Add(-time.Minute),
		RetryPolicy:   &history.RetryPolicy{MaxAttempts: 2},
	}); err != nil {
		t.Fatal(err)
	}
	transition := execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 2, Now: time.Now()}
	var result execution.ActivityFailureResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutActivity(tx, transition, protocol.TimeoutTypeHeartbeat)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Terminal || result.RetryState == nil || result.RetryState.Attempt != 2 {
		t.Fatalf("timeout result = %+v", result)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 3 || events[0].ActivityStarted == nil || events[1].ActivityTimedOut == nil || events[2].ActivityAttemptFailed == nil {
		t.Fatalf("timeout history = %+v, err = %v", events, err)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Attempt != 2 || state.Stamp != 3 || state.Status != execution.ActivityStatusEnums.Scheduled {
		t.Fatalf("timeout state = %+v, ok = %v, err = %v", state, ok, err)
	}
}

// Happy Path: ScheduleToClose 到期时无视剩余重试次数，事务内直接写终态 ActivityFailed。
func TestTimeoutActivityScheduleToCloseSkipsRetry(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 2, Stamp: 2, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: execution.TransientEventID, ScheduleToCloseTimeoutMs: 1,
		ScheduledTime: time.Now().Add(-time.Minute),
		RetryPolicy:   &history.RetryPolicy{MaxAttempts: 0},
	}); err != nil {
		t.Fatal(err)
	}
	var result execution.ActivityFailureResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutActivity(tx, execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 2, Stamp: 2, Now: time.Now()}, protocol.TimeoutTypeScheduleToClose)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || !result.Terminal || result.RetryState != nil {
		t.Fatalf("timeout result = %+v", result)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 3 || events[1].ActivityTimedOut == nil || events[2].ActivityFailed == nil {
		t.Fatalf("timeout history = %+v, err = %v", events, err)
	}
	if events[1].ActivityTimedOut.RetryState != "Timeout" || events[2].ActivityFailed.RetryState != "Timeout" {
		t.Fatalf("retry state = %q / %q, want Timeout", events[1].ActivityTimedOut.RetryState, events[2].ActivityFailed.RetryState)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Attempt != 2 || state.Stamp != 2 || state.Status != execution.ActivityStatusEnums.Failed {
		t.Fatalf("timeout state = %+v, ok = %v, err = %v", state, ok, err)
	}
}

// 校验针对本次触发的超时类型：未到期或仅其他类型到期的触发保持事务不变。
func TestTimeoutActivityChecksFiredTimeoutAgainstPersistedDeadline(t *testing.T) {
	now := time.Now()
	stale := now.Add(-time.Hour)
	fresh := now.Add(-time.Millisecond)
	started := execution.ActivityStatusEnums.Started
	scheduled := execution.ActivityStatusEnums.Scheduled
	const minuteMs = int64(60_000)
	tests := []struct {
		name    string
		state   execution.ActivityState
		reason  protocol.TimeoutType
		applied bool
	}{
		{"Edge Case: heartbeat refreshed by another replica", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale, LastHeartbeatAt: &fresh, HeartbeatTimeoutMs: minuteMs}, protocol.TimeoutTypeHeartbeat, false},
		{"Happy Path: heartbeat really expired", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale, HeartbeatTimeoutMs: minuteMs}, protocol.TimeoutTypeHeartbeat, true},
		{"Edge Case: heartbeat timer with only start-to-close expired", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale, LastHeartbeatAt: &fresh, HeartbeatTimeoutMs: minuteMs, StartToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeHeartbeat, false},
		{"Happy Path: start-to-close expired despite fresh heartbeat", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale, LastHeartbeatAt: &fresh, HeartbeatTimeoutMs: minuteMs, StartToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeStartToClose, true},
		{"Edge Case: start-to-close timer after a later restart", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &fresh, StartToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeStartToClose, false},
		{"Happy Path: first attempt schedule-to-start expired", execution.ActivityState{Status: scheduled, Attempt: 1, ScheduledTime: stale, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart, true},
		{"Edge Case: retry attempt counts schedule-to-start from its backoff", execution.ActivityState{Status: scheduled, Attempt: 2, ScheduledTime: stale, NextAttemptAt: &fresh, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart, false},
		{"Happy Path: retry attempt schedule-to-start expired", execution.ActivityState{Status: scheduled, Attempt: 2, ScheduledTime: stale, NextAttemptAt: ptrTime(now.Add(-2 * time.Minute)), ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart, true},
		{"Edge Case: reset counts schedule-to-start from the reset", execution.ActivityState{Status: scheduled, Attempt: 1, ActivityReset: true, ScheduledTime: stale, NextAttemptAt: &fresh, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart, false},
		{"Edge Case: schedule-to-start after start", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale, ScheduleToStartTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToStart, false},
		{"Edge Case: schedule-to-close not due", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: now, StartedTime: &now, ScheduleToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeScheduleToClose, false},
		{"Edge Case: type without configured timeout", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: stale, StartedTime: &stale}, protocol.TimeoutTypeHeartbeat, false},
		{"Edge Case: deadline equals fire time", execution.ActivityState{Status: started, Attempt: 1, ScheduledTime: now.Add(-time.Minute), StartedTime: ptrTime(now.Add(-time.Minute)), StartToCloseTimeoutMs: minuteMs}, protocol.TimeoutTypeStartToClose, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newStoreWithRun(t, "run-1")
			state := tt.state
			state.RunID, state.ScheduledEventID, state.ActivityID, state.ActivityType, state.Stamp = "run-1", 7, "charge", "charge", 1
			if err := store.UpsertActivityState(&state); err != nil {
				t.Fatal(err)
			}
			var result execution.ActivityFailureResult
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				var err error
				result, err = execution.TimeoutActivity(tx, execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: state.Attempt, Stamp: 1, Now: now}, tt.reason)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if result.Applied != tt.applied {
				t.Fatalf("Applied = %v, want %v", result.Applied, tt.applied)
			}
			events, err := store.GetEvents("run-1")
			if err != nil {
				t.Fatal(err)
			}
			if !tt.applied && len(events) != 0 {
				t.Fatalf("ignored trigger wrote history: %+v", events)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// Happy Path: ScheduleToStart 到期时无视剩余重试次数，事务内直接写终态 ActivityFailed。
func TestTimeoutActivityScheduleToStartSkipsRetry(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled,
		ScheduleToStartTimeoutMs: 1, ScheduledTime: time.Now().Add(-time.Minute),
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 3},
	}); err != nil {
		t.Fatal(err)
	}
	var result execution.ActivityFailureResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutActivity(tx, execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now()}, protocol.TimeoutTypeScheduleToStart)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || !result.Terminal || result.RetryState != nil {
		t.Fatalf("timeout result = %+v, want a terminal failure without retry", result)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) != 2 || events[0].ActivityTimedOut == nil || events[1].ActivityFailed == nil {
		t.Fatalf("timeout history = %+v, err = %v", events, err)
	}
	if events[0].ActivityTimedOut.RetryState != "Timeout" || events[1].ActivityFailed.RetryState != "Timeout" {
		t.Fatalf("retry state = %q / %q, want Timeout", events[0].ActivityTimedOut.RetryState, events[1].ActivityFailed.RetryState)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok || state.Attempt != 1 || state.Status != execution.ActivityStatusEnums.Failed {
		t.Fatalf("timeout state = %+v, ok = %v, err = %v", state, ok, err)
	}
}

// Happy Path: 失败后排定重试时，下一次 attempt 的最早时间随状态持久化；attempt 开始后清空。
func TestFailActivityPersistsNextAttemptTime(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started,
		StartedEventID: execution.TransientEventID,
		RetryPolicy:    &history.RetryPolicy{MaxAttempts: 3, InitialIntervalMs: (10 * time.Minute).Milliseconds(), MaxIntervalMs: time.Hour.Milliseconds(), BackoffCoefficient: 2},
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond)
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.FailActivity(tx, execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: now}, protocol.Failure{Message: "temporary"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	state, ok, err := store.GetActivityState("run-1", 7)
	if err != nil || !ok {
		t.Fatalf("state ok = %v, err = %v", ok, err)
	}
	if state.NextAttemptAt == nil || !state.NextAttemptAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("next attempt at = %v, want %v", state.NextAttemptAt, now.Add(10*time.Minute))
	}
	if got := execution.ActivityRecoveryFor(state, now.Add(time.Minute)); got != execution.ActivityRecoveryAwaitRetry {
		t.Fatalf("recovery inside the backoff = %v, want await retry", got)
	}
	if got := execution.ActivityRecoveryFor(state, now.Add(11*time.Minute)); got != execution.ActivityRecoveryRedispatch {
		t.Fatalf("recovery after the backoff = %v, want redispatch", got)
	}

	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.StartActivity(tx, execution.ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 2, Stamp: 2, Now: now.Add(10 * time.Minute)}, "worker-1", "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	state, _, err = store.GetActivityState("run-1", 7)
	if err != nil || state.Status != execution.ActivityStatusEnums.Started || state.NextAttemptAt != nil {
		t.Fatalf("state after start = %+v, err = %v; want started with no pending backoff", state, err)
	}
}
