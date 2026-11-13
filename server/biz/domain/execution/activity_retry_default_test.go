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
// Activity 的缺省重试策略（与 Temporal 相同）
// ============================================
// - Happy Path: 没有重试策略的 Activity 不限次数重试，间隔 1s 起按 2 倍增长、封顶 100s
// - Happy Path: 只给 InitialInterval 的策略，MaxInterval 取 100 倍 InitialInterval
// - Edge Case: 下一次 attempt 会落在 ScheduleToClose 之后时不再重试，RetryState 为 Timeout
// - Edge Case: 生效策略不限次数时 attempt 开始只记 transient start；MaxAttempts=1 时立即写 ActivityStarted
// - Happy Path: 超时的种类单独记在事件与 Failure 上，Message 是给人读的说明
// ============================================

func seedStartedActivity(t *testing.T, store *memory.ExecutionManager, state execution.ActivityState) {
	t.Helper()
	state.RunID, state.ScheduledEventID, state.ActivityID, state.ActivityType = "run-1", 7, "charge", "charge"
	state.Status = execution.ActivityStatusEnums.Started
	state.StartedEventID = execution.TransientEventID
	if err := store.UpsertActivityState(&state); err != nil {
		t.Fatal(err)
	}
}

// failUntil 让 attempt 1..attempts 逐次失败，返回每次失败后排定的重试等待。
func failUntil(t *testing.T, store *memory.ExecutionManager, now time.Time, attempts int) []time.Duration {
	t.Helper()
	var delays []time.Duration
	for attempt := 1; attempt <= attempts; attempt++ {
		state, _, err := store.GetActivityState("run-1", 7)
		if err != nil {
			t.Fatal(err)
		}
		var result execution.ActivityFailureResult
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			result, err = execution.FailActivity(tx, execution.ActivityTransition{
				RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7,
				Attempt: state.Attempt, Stamp: state.Stamp, Now: now,
			}, protocol.Failure{Message: "temporary"})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if result.Terminal || result.RetryState == nil {
			t.Fatalf("attempt %d closed the activity: %+v", attempt, result)
		}
		delays = append(delays, result.RetryState.NextAttemptAt.Sub(now))
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			_, err := execution.StartActivity(tx, execution.ActivityTransition{
				RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7,
				Attempt: result.RetryState.Attempt, Stamp: result.RetryState.Stamp, Now: now,
			}, "worker-1", "")
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	return delays
}

func TestActivityWithoutRetryPolicyRetriesWithTemporalDefaults(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedActivity(t, store, execution.ActivityState{Attempt: 1, Stamp: 1})
	delays := failUntil(t, store, time.Now(), 10)
	want := []time.Duration{1, 2, 4, 8, 16, 32, 64, 100, 100, 100}
	for i := range want {
		if delays[i] != want[i]*time.Second {
			t.Fatalf("retry delays = %v, want %v seconds", delays, want)
		}
	}
}

func TestPartialRetryPolicyFillsDefaults(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedActivity(t, store, execution.ActivityState{Attempt: 1, Stamp: 1,
		RetryPolicy: &history.RetryPolicy{InitialIntervalMs: (5 * time.Second).Milliseconds()}})
	delays := failUntil(t, store, time.Now(), 9)
	want := []time.Duration{5, 10, 20, 40, 80, 160, 320, 500, 500}
	for i := range want {
		if delays[i] != want[i]*time.Second {
			t.Fatalf("retry delays = %v, want %v seconds", delays, want)
		}
	}
}

func TestEffectiveActivityRetryPolicy(t *testing.T) {
	got := execution.EffectiveActivityRetryPolicy(nil)
	if got.MaxAttempts != 0 || got.InitialIntervalMs != 1000 || got.BackoffCoefficient != 2 || got.MaxIntervalMs != 100_000 ||
		len(got.NonRetryableErrorTypes) != 0 || len(got.NonRetryableErrors) != 0 {
		t.Fatalf("default policy = %+v", got)
	}
	got = execution.EffectiveActivityRetryPolicy(&history.RetryPolicy{MaxAttempts: 3, InitialIntervalMs: 200, NonRetryableErrorTypes: []string{"Fatal"}})
	if got.MaxAttempts != 3 || got.InitialIntervalMs != 200 || got.BackoffCoefficient != 2 || got.MaxIntervalMs != 20_000 || got.NonRetryableErrorTypes[0] != "Fatal" {
		t.Fatalf("partial policy = %+v", got)
	}
}

func TestRetryStopsWhenNextAttemptPassesScheduleToClose(t *testing.T) {
	now := time.Now()
	store := newStoreWithRun(t, "run-1")
	seedStartedActivity(t, store, execution.ActivityState{Attempt: 1, Stamp: 1,
		ScheduledTime: now.Add(-59500 * time.Millisecond), ScheduleToCloseTimeoutMs: time.Minute.Milliseconds()})
	var result execution.ActivityFailureResult
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.FailActivity(tx, execution.ActivityTransition{
			RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: now,
		}, protocol.Failure{Message: "temporary"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Terminal {
		t.Fatalf("result = %+v, want the activity closed", result)
	}
	events, err := store.GetEvents("run-1")
	if err != nil || len(events) == 0 || events[len(events)-1].ActivityFailed == nil || events[len(events)-1].ActivityFailed.RetryState != "Timeout" {
		t.Fatalf("history = %+v, err = %v; want ActivityFailed with RetryState Timeout", events, err)
	}
}

func TestStartActivityWritesStartedEventOnlyForSingleAttempt(t *testing.T) {
	for name, tc := range map[string]struct {
		policy  *history.RetryPolicy
		started bool
	}{
		"default policy retries": {policy: nil, started: false},
		"max attempts 1":         {policy: &history.RetryPolicy{MaxAttempts: 1}, started: true},
	} {
		t.Run(name, func(t *testing.T) {
			store := newStoreWithRun(t, "run-1")
			if err := store.UpsertActivityState(&execution.ActivityState{
				RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
				Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, RetryPolicy: tc.policy,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.RunInTx(func(tx execution.ExecutionTx) error {
				_, err := execution.StartActivity(tx, execution.ActivityTransition{
					RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now(),
				}, "worker-1", "")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			events, err := store.GetEvents("run-1")
			if err != nil {
				t.Fatal(err)
			}
			if got := len(events) == 1 && events[0].ActivityStarted != nil; got != tc.started {
				t.Fatalf("ActivityStarted written = %v, want %v (events %+v)", got, tc.started, events)
			}
		})
	}
}

func TestTimeoutRecordsTimeoutTypeSeparately(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedActivity(t, store, execution.ActivityState{Attempt: 1, Stamp: 1, StartToCloseTimeoutMs: 1,
		StartedTime: ptrTime(time.Now().Add(-time.Minute)), RetryPolicy: &history.RetryPolicy{MaxAttempts: 1}})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.TimeoutActivity(tx, execution.ActivityTransition{
			RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now(),
		}, protocol.TimeoutTypeStartToClose)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	var timedOut *history.ActivityTimedOutEvent
	var failed *history.ActivityFailedEvent
	for _, event := range events {
		if event.ActivityTimedOut != nil {
			timedOut = event.ActivityTimedOut
		}
		if event.ActivityFailed != nil {
			failed = event.ActivityFailed
		}
	}
	if timedOut == nil || timedOut.TimeoutType != protocol.TimeoutTypeStartToClose || timedOut.Failure == nil ||
		timedOut.Failure.TimeoutType != protocol.TimeoutTypeStartToClose || timedOut.Failure.Message != "activity StartToClose timeout" {
		t.Fatalf("ActivityTimedOut = %+v", timedOut)
	}
	if failed == nil || failed.Failure == nil || failed.Failure.TimeoutType != protocol.TimeoutTypeStartToClose {
		t.Fatalf("ActivityFailed = %+v, want the timeout failure", failed)
	}
}

// 超时只由 Server 判定：Worker 上报的失败带超时种类时按普通失败处理。
func TestReportedFailureCannotClaimTimeout(t *testing.T) {
	store := newStoreWithRun(t, "run-1")
	seedStartedActivity(t, store, execution.ActivityState{Attempt: 1, Stamp: 1, RetryPolicy: &history.RetryPolicy{MaxAttempts: 1}})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.FailActivity(tx, execution.ActivityTransition{
			RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now(),
		}, protocol.Failure{Message: "boom", TimeoutType: protocol.TimeoutTypeStartToClose})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents("run-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.ActivityTimedOut != nil {
			t.Fatalf("reported failure produced ActivityTimedOut: %+v", events)
		}
		if event.ActivityFailed != nil && event.ActivityFailed.Failure.TimeoutType != "" {
			t.Fatalf("reported failure kept timeout type: %+v", event.ActivityFailed.Failure)
		}
	}
}
