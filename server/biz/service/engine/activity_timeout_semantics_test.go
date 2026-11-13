package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 本文件补齐四类 Activity 超时的第二 attempt、Heartbeat 续期、迟到结果，
// 以及重复投递的实测行为。Server 用假时钟，超时与退避只在 clk.Advance 时到期。

func newTimeoutServer(t *testing.T) (*Server, execution.ExecutionManager, *testTaskQueue, *clocktest.Fake) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	tasks := newTestTaskQueue()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), tasks)
	t.Cleanup(server.Stop)
	return server, mgr, tasks, clk
}

// requireEventCount 断言 runID 的 History 里恰有 want 个 typ 事件。
func requireEventCount(t *testing.T, mgr execution.ExecutionManager, runID string, typ history.EventType, want int) {
	t.Helper()
	if got := countEngineEvents(mustEvents(t, mgr, runID), typ); got != want {
		t.Fatalf("%s events = %d, want %d", typ, got, want)
	}
}

func retryPolicy(maxAttempts int, every time.Duration) *protocol.ParsedRetryPolicy {
	return &protocol.ParsedRetryPolicy{MaxAttempts: maxAttempts, InitialInterval: every, BackoffCoefficient: 1}
}

// StartToClose / Heartbeat 超时对每个 attempt 重新安装：
// attempt 2 被 worker 领取并再次沉默后，必须再次触发同一类超时。
func TestActivityTimeoutRearmsOnRetryAttempt(t *testing.T) {
	const (
		timeout = 25 * time.Millisecond
		backoff = 10 * time.Millisecond
	)
	cases := []struct {
		name   string
		cmd    history.Command
		stc    time.Duration
		hb     time.Duration
		reason protocol.TimeoutType
	}{
		{
			name:   "start-to-close",
			cmd:    history.Command{Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", StartToCloseTimeout: timeout, RetryPolicy: retryPolicy(3, backoff)},
			stc:    timeout,
			reason: protocol.TimeoutTypeStartToClose,
		},
		{
			name:   "heartbeat",
			cmd:    history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "step", ActivityType: "slow", HeartbeatTimeout: timeout, RetryPolicy: retryPolicy(3, backoff)},
			hb:     timeout,
			reason: protocol.TimeoutTypeHeartbeat,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, mgr, tasks, clk := newTimeoutServer(t)
			runID, _ := scheduleTimedCommand(t, server, mgr, "rearm-"+tc.name, tc.cmd)

			first := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
			if _, err := server.HandleActivityStartedWithMetadata(first, tc.stc, tc.hb); err != nil {
				t.Fatal(err)
			}
			clk.Advance(timeout)
			requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 1)

			clk.Advance(backoff)
			second := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 2 })
			if _, err := server.HandleActivityStartedWithMetadata(second, tc.stc, tc.hb); err != nil {
				t.Fatal(err)
			}
			// attempt 2 的超时从它自己的开始时刻起算。
			clk.Advance(timeout - time.Millisecond)
			requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 1)
			clk.Advance(time.Millisecond)
			requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 2)
			requireEventCount(t, mgr, runID, history.EventActivityAttemptFailed, 2)

			events, err := mgr.GetEvents(runID)
			if err != nil {
				t.Fatal(err)
			}
			if !hasTimeoutReason(events, 2, tc.reason) {
				t.Fatalf("attempt 2 ActivityTimedOut.Reason = %v, want %q twice", timeoutReasons(events), tc.reason)
			}
			// attempt 3 仍会被排（MaxAttempts=3）。
			clk.Advance(backoff)
			waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 3 })
		})
	}
}

func timeoutReasons(events []history.Event) []protocol.TimeoutType {
	var reasons []protocol.TimeoutType
	for _, event := range events {
		if event.ActivityTimedOut != nil {
			reasons = append(reasons, event.ActivityTimedOut.TimeoutType)
		}
	}
	return reasons
}

func hasTimeoutReason(events []history.Event, count int, reason protocol.TimeoutType) bool {
	seen := 0
	for _, got := range timeoutReasons(events) {
		if got == reason {
			seen++
		}
	}
	return seen == count
}

// Heartbeat 续期：持续心跳不触发超时；停止心跳后按 HB 超时。
func TestHeartbeatRenewalThenExpiry(t *testing.T) {
	const heartbeatTimeout = 40 * time.Millisecond
	server, mgr, tasks, clk := newTimeoutServer(t)
	cmd := history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		HeartbeatTimeout: heartbeatTimeout, RetryPolicy: retryPolicy(3, 10*time.Millisecond),
	}
	runID, _ := scheduleTimedCommand(t, server, mgr, "heartbeat-renewal", cmd)
	task := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(task, 0, heartbeatTimeout); err != nil {
		t.Fatal(err)
	}

	// 续期窗口 100ms > 2×HB：期间每 10ms 心跳一次，不应超时。心跳只更新 Activity state，不写 History。
	before := len(mustEvents(t, mgr, runID))
	for i := range 10 {
		clk.Advance(10 * time.Millisecond)
		if err := server.HandleActivityHeartbeat(task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"step": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 0)
	if after := len(mustEvents(t, mgr, runID)); after != before {
		t.Fatalf("heartbeats appended %d history events, want 0", after-before)
	}
	state, ok, err := server.GetActivityState(runID, task.ScheduledEventID)
	if err != nil || !ok || state.LastHeartbeat["step"] != int64(9) || state.LastHeartbeatAt == nil || !state.LastHeartbeatAt.Equal(clk.Now()) {
		t.Fatalf("heartbeat state = %+v ok=%v err=%v", state, ok, err)
	}

	// 停止心跳：从 state 里最后一次心跳的时间起算，HB 到期时超时。
	clk.Advance(heartbeatTimeout - time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 0)
	clk.Advance(time.Millisecond)
	events := mustEvents(t, mgr, runID)
	if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != (protocol.TimeoutTypeHeartbeat) {
		t.Fatalf("timeout reasons = %v, want [%s]", reasons, (protocol.TimeoutTypeHeartbeat))
	}
	// ActivityTimedOut 带最后一次心跳的详情（Temporal TimeoutFailureInfo.last_heartbeat_details）。
	for _, event := range events {
		if event.ActivityTimedOut != nil && event.ActivityTimedOut.LastHeartbeatDetails["step"] != int64(9) {
			t.Fatalf("ActivityTimedOut.LastHeartbeatDetails = %v, want step 9", event.ActivityTimedOut.LastHeartbeatDetails)
		}
	}
	// 重试的 attempt 从 state 读到上一次 attempt 最后的心跳详情。
	clk.Advance(10 * time.Millisecond)
	retry := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 2 })
	if retry.LastHeartbeatDetails["step"] != int64(9) {
		t.Fatalf("retry LastHeartbeatDetails = %v, want step 9", retry.LastHeartbeatDetails)
	}
}

// 没有心跳过的 attempt 超时时，ActivityTimedOut 不带心跳详情。
func TestStartToCloseTimeoutWithoutHeartbeatHasNoDetails(t *testing.T) {
	server, mgr, tasks, clk := newTimeoutServer(t)
	cmd := history.Command{Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", StartToCloseTimeout: 20 * time.Millisecond, RetryPolicy: retryPolicy(1, time.Millisecond)}
	runID, _ := scheduleTimedCommand(t, server, mgr, "timeout-no-heartbeat", cmd)
	task := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(task, 20*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(20 * time.Millisecond)
	for _, event := range mustEvents(t, mgr, runID) {
		if event.ActivityTimedOut != nil && event.ActivityTimedOut.LastHeartbeatDetails != nil {
			t.Fatalf("ActivityTimedOut.LastHeartbeatDetails = %v, want none", event.ActivityTimedOut.LastHeartbeatDetails)
		}
	}
	requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 1)
}

// 迟到 Heartbeat / 迟到 Completed 在 attempt 超时并进入 retry 后不得改变状态，
// 也不得写入重复完成事件。
func TestLateHeartbeatAndCompletionAfterTimeoutAreIgnored(t *testing.T) {
	server, mgr, tasks, clk := newTimeoutServer(t)
	cmd := history.Command{
		Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		StartToCloseTimeout: 20 * time.Millisecond, RetryPolicy: retryPolicy(2, 10*time.Millisecond),
	}
	runID, _ := scheduleTimedCommand(t, server, mgr, "late-attempt-result", cmd)
	first := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(first, 20*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(20 * time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityAttemptFailed, 1)

	// attempt 1 已超时；迟到的 heartbeat / completed 携带旧 stamp。
	if err := server.HandleActivityHeartbeat(first.RunID, first.ScheduledEventID, first.Attempt, first.Stamp, map[string]any{"late": true}); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(context.Background(), first.RunID, first.ScheduledEventID, first.Attempt, first.Stamp, map[string]any{"late": true}); err != nil {
		t.Fatal(err)
	}
	state, ok, err := server.GetActivityState(runID, first.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state missing: ok=%v err=%v", ok, err)
	}
	if state.Attempt != 2 || state.Status != execution.ActivityStatusEnums.Scheduled {
		t.Fatalf("late results changed state: attempt=%d status=%s", state.Attempt, state.Status)
	}
	events, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(events, history.EventActivityCompleted); got != 0 {
		t.Fatalf("late completion wrote %d ActivityCompleted events, want 0", got)
	}
	if state.LastHeartbeat != nil {
		t.Fatalf("late heartbeat recorded details %+v", state.LastHeartbeat)
	}
}

// 四类超时的 Reason 与终态 RetryState 精确断言：
// ScheduleToClose 用 MaxAttempts=1 直接终态，RetryState 记录 MaximumAttemptsReached。
func TestActivityTimeoutReasonsAndTerminalRetryState(t *testing.T) {
	cases := []struct {
		name    string
		cmd     history.Command
		start   bool
		stc     time.Duration
		hb      time.Duration
		timeout time.Duration
		reason  protocol.TimeoutType
	}{
		{
			name:    "schedule-to-start",
			cmd:     history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "step", ActivityType: "slow", ScheduleToStartTimeout: 20 * time.Millisecond, RetryPolicy: retryPolicy(1, 0)},
			timeout: 20 * time.Millisecond,
			reason:  protocol.TimeoutTypeScheduleToStart,
		},
		{
			name:    "start-to-close",
			cmd:     history.Command{Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", StartToCloseTimeout: 20 * time.Millisecond, RetryPolicy: retryPolicy(1, 0)},
			start:   true,
			stc:     20 * time.Millisecond,
			timeout: 20 * time.Millisecond,
			reason:  protocol.TimeoutTypeStartToClose,
		},
		{
			name:    "heartbeat",
			cmd:     history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "step", ActivityType: "slow", HeartbeatTimeout: 20 * time.Millisecond, RetryPolicy: retryPolicy(1, 0)},
			start:   true,
			hb:      20 * time.Millisecond,
			timeout: 20 * time.Millisecond,
			reason:  protocol.TimeoutTypeHeartbeat,
		},
		{
			name:    "schedule-to-close",
			cmd:     history.Command{Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", ScheduleToCloseTimeout: 30 * time.Millisecond, RetryPolicy: retryPolicy(1, 0)},
			timeout: 30 * time.Millisecond,
			reason:  protocol.TimeoutTypeScheduleToClose,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, mgr, tasks, clk := newTimeoutServer(t)
			runID, _ := scheduleTimedCommand(t, server, mgr, "reason-"+tc.name, tc.cmd)
			if tc.start {
				task := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
				if _, err := server.HandleActivityStartedWithMetadata(task, tc.stc, tc.hb); err != nil {
					t.Fatal(err)
				}
			}
			clk.Advance(tc.timeout - time.Millisecond)
			requireEventCount(t, mgr, runID, history.EventActivityFailed, 0)
			clk.Advance(time.Millisecond)
			requireEventCount(t, mgr, runID, history.EventActivityFailed, 1)
			events := mustEvents(t, mgr, runID)
			if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != tc.reason {
				t.Fatalf("ActivityTimedOut.Reason = %v, want [%s]", reasons, tc.reason)
			}
			var failed *history.ActivityFailedEvent
			for _, event := range events {
				if event.ActivityFailed != nil {
					failed = event.ActivityFailed
				}
			}
			if failed == nil {
				t.Fatal("missing ActivityFailed event")
			}
			if failed.RetryState != "MaximumAttemptsReached" {
				t.Fatalf("ActivityFailed.RetryState = %q, want MaximumAttemptsReached", failed.RetryState)
			}
		})
	}
}

// 重复完成是幂等的：同一 attempt/stamp 的第二次 Completed 不写第二个事件、不改状态。
func TestDuplicateActivityCompletionIsIdempotent(t *testing.T) {
	server, mgr, tasks, _ := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "duplicate-completion", history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
	})
	task := waitForQueuedActivity(t, tasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := server.HandleActivityStartedWithMetadata(task, time.Second, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	events, _ := mgr.GetEvents(runID)
	if got := countEngineEvents(events, history.EventActivityCompleted); got != 1 {
		t.Fatalf("duplicate completion wrote %d ActivityCompleted events, want 1", got)
	}
	state, _, _ := server.GetActivityState(runID, task.ScheduledEventID)
	if state.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("state = %s, want completed", state.Status)
	}
}
