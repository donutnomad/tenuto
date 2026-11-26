package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Activity 超时与重试的组合矩阵。
//
// 现有覆盖：超时去重/关闭后迟到超时（server_test.go）、崩溃恢复后 timer 重建
// （integration/recovery_test.go）、ScheduleToStart 打戳（telemetry_schedule_to_start_test.go）。
// 本文件补：每种超时触发后 attempt 推进与 retry 入队的组合，以及
// “重试 attempt 是否重新获得该超时”的边界。

func startTimedActivityRun(t *testing.T, server *Server, mgr execution.ExecutionManager, workflowID string) (string, int64) {
	t.Helper()
	wf := contractWorkflowConfig(workflowID, nil)
	runID, wftID := startContractRun(t, mgr, server, wf)
	return runID, wftID
}

func scheduleTimedCommand(t *testing.T, server *Server, mgr execution.ExecutionManager, workflowID string, cmd history.Command) (string, int64) {
	t.Helper()
	runID, wftID := startTimedActivityRun(t, server, mgr, workflowID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []history.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	return runID, 0
}

// StartToClose 与 Heartbeat 超时各触发一次 attempt 失败，并按 retry policy 在退避到期后派发下一个 attempt。
// 假时钟驱动：推进到超时时刻之前不失败，退避到期之前不派发新 attempt。
func TestActivityTimeoutTriggersRetryAttempt(t *testing.T) {
	const backoff = 30 * time.Second
	retryPolicy := &protocol.ParsedRetryPolicy{MaxAttempts: 3, InitialInterval: backoff, BackoffCoefficient: 1}
	cases := []struct {
		name    string
		cmd     history.Command
		stc, hb time.Duration
	}{
		{
			name: "start-to-close",
			cmd:  history.Command{Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow", StartToCloseTimeout: time.Minute, RetryPolicy: retryPolicy},
			stc:  time.Minute,
		},
		{
			name: "heartbeat",
			cmd:  history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "step", ActivityType: "slow", HeartbeatTimeout: time.Minute, RetryPolicy: retryPolicy},
			hb:   time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			activityTasks := newTestTaskQueue()
			server, clk := newFakeClockServer(mgr, newTestTaskQueue(), activityTasks)
			defer server.Stop()

			runID, _ := scheduleTimedCommand(t, server, mgr, "timeout-"+tc.name, tc.cmd)
			task := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
			if _, err := server.HandleActivityStartedWithMetadata(task, tc.stc, tc.hb); err != nil {
				t.Fatal(err)
			}

			clk.Advance(time.Minute - time.Millisecond)
			if events, _ := mgr.GetEvents(runID); countEngineEvents(events, history.EventActivityTimedOut) != 0 {
				t.Fatal("activity timed out before its deadline")
			}
			clk.Advance(time.Millisecond)
			events, _ := mgr.GetEvents(runID)
			if countEngineEvents(events, history.EventActivityTimedOut) != 1 || countEngineEvents(events, history.EventActivityAttemptFailed) != 1 {
				t.Fatalf("after the deadline: ActivityTimedOut=%d ActivityAttemptFailed=%d, want 1/1",
					countEngineEvents(events, history.EventActivityTimedOut), countEngineEvents(events, history.EventActivityAttemptFailed))
			}

			clk.Advance(backoff - time.Millisecond)
			if queued := activityTasks.snapshot(); len(queued) != 1 {
				t.Fatalf("attempt 2 dispatched before its backoff: %d queued", len(queued))
			}
			clk.Advance(time.Millisecond)
			retry := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 2 })
			if retry.ActivityID != "step" {
				t.Fatalf("retry step = %q", retry.ActivityID)
			}
			state, ok, err := server.GetActivityState(runID, retry.ScheduledEventID)
			if err != nil || !ok {
				t.Fatalf("activity state missing: ok=%v err=%v", ok, err)
			}
			if state.Attempt != 2 || state.Status != execution.ActivityStatusEnums.Scheduled {
				t.Fatalf("state attempt/status = %d/%s, want 2/scheduled", state.Attempt, state.Status)
			}
		})
	}
}

// ScheduleToStart 到期即终态失败：RetryPolicy 还有余量也不派发新 attempt。
func TestScheduleToStartTimeoutFailsActivityWithoutRetry(t *testing.T) {
	server, mgr, activityTasks, clk := newTimeoutServer(t)

	cmd := history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "never-picked",
		ScheduleToStartTimeout: 20 * time.Millisecond,
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts: 3, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1,
		},
	}
	runID, _ := scheduleTimedCommand(t, server, mgr, "timeout-s2s-retry", cmd)

	clk.Advance(20*time.Millisecond - time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityFailed, 0)
	clk.Advance(time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityFailed, 1)
	// 跨过任意多个 retry 间隔与 ScheduleToStart 周期，确认没有第二个 attempt 被派发。
	clk.Advance(time.Hour)
	events := mustEvents(t, mgr, runID)
	queued := activityTasks.snapshot()
	if failed, attemptFailed := countEngineEvents(events, history.EventActivityFailed), countEngineEvents(events, history.EventActivityAttemptFailed); failed != 1 || attemptFailed != 0 || len(queued) != 1 {
		t.Fatalf("ActivityFailed=%d ActivityAttemptFailed=%d queuedAttempts=%d, want 1/0/1", failed, attemptFailed, len(queued))
	}
	if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != (protocol.TimeoutTypeScheduleToStart) {
		t.Fatalf("timeout reasons = %v, want [%s]", reasons, (protocol.TimeoutTypeScheduleToStart))
	}
}

// Integration: 内存 total timer 到期时，即使 RetryPolicy 不限次数，Activity 也只失败一次，
// 不派发新 attempt，Run 按 Workflow 逻辑继续推进。
func TestScheduleToCloseTimerEndsRetries(t *testing.T) {
	server, mgr, activityTasks, clk := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "timeout-s2c-retry", history.Command{
		Type: history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		ScheduleToCloseTimeout: 60 * time.Millisecond,
		RetryPolicy:            retryPolicy(0, 10*time.Millisecond),
	})

	clk.Advance(60*time.Millisecond - time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityFailed, 0)
	clk.Advance(time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityFailed, 1)
	requireEventCount(t, mgr, runID, history.EventWorkflowTaskScheduled, 2)
	// 跨过多个完整总期限，确认终态后没有新增派发。
	clk.Advance(time.Hour)

	events := mustEvents(t, mgr, runID)
	if got := countEngineEvents(events, history.EventActivityFailed); got != 1 {
		t.Fatalf("ActivityFailed = %d, want 1", got)
	}
	if got := countEngineEvents(events, history.EventActivityAttemptFailed); got != 0 {
		t.Fatalf("ActivityAttemptFailed = %d, want 0", got)
	}
	if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != (protocol.TimeoutTypeScheduleToClose) {
		t.Fatalf("timeout reasons = %v, want [%s]", reasons, (protocol.TimeoutTypeScheduleToClose))
	}
	if queued := activityTasks.snapshot(); len(queued) != 1 {
		t.Fatalf("queued attempts = %d, want 1", len(queued))
	}
	completeRun(t, server, mgr, runID)
}

// Integration: 另一副本持有的定时器按旧心跳基准触发时，持久化心跳已刷新，Activity 继续运行；
// 心跳停止后，真正到期仍然超时。
func TestStaleHeartbeatTimerFromOtherReplicaIgnored(t *testing.T) {
	const heartbeatTimeout = 400 * time.Millisecond
	holder, mgr, _, clk := newTimeoutServer(t)
	other := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(other.Stop)

	runID, _ := scheduleTimedCommand(t, holder, mgr, "stale-heartbeat", history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		HeartbeatTimeout: heartbeatTimeout, RetryPolicy: retryPolicy(1, 0),
	})
	states, err := mgr.ListActivityStates(runID)
	if err != nil || len(states) != 1 {
		t.Fatalf("activity states = %d, err = %v", len(states), err)
	}
	st := states[0]
	if err := holder.HandleActivityStarted(runID, st.ScheduledEventID, st.Attempt, st.Stamp, 0, heartbeatTimeout); err != nil {
		t.Fatal(err)
	}
	// other 的周期重建按当前持久化状态装定时器，之后的心跳只刷新 holder 的内存定时器。
	if _, _, err := other.Reconcile(context.Background(), DefaultOverdueTimerGrace, DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	if !timerArmed(other, activityTimerRef(timerActivityHeartbeat, runID, st.ScheduledEventID)) {
		t.Fatal("other replica must hold a rebuilt heartbeat timer")
	}

	// 每 50ms 心跳一次，跨过 other 按启动时刻装的截止时间。
	for range 12 {
		clk.Advance(50 * time.Millisecond)
		if err := holder.HandleActivityHeartbeat(runID, st.ScheduledEventID, st.Attempt, st.Stamp, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := countEngineEvents(mustEvents(t, mgr, runID), history.EventActivityTimedOut); got != 0 {
		t.Fatalf("stale timer timed out a heartbeating activity: %d ActivityTimedOut", got)
	}
	if current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID); !current.IsStarted() {
		t.Fatalf("activity status = %s, want started", current.Status)
	}

	// 心跳停止：从最后一次心跳起算到期时超时。
	clk.Advance(heartbeatTimeout - time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 0)
	clk.Advance(time.Millisecond)
	requireEventCount(t, mgr, runID, history.EventActivityTimedOut, 1)
	if current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID); !current.IsTerminal() {
		t.Fatalf("activity status = %s, want terminal after real heartbeat timeout", current.Status)
	}
}

// ScheduleToStart 约束每个 attempt，从该 attempt 可派发（退避到期）时起算；两个副本按持久化状态挂同一个截止时间。
// 到期后 Activity 失败且不重试。
func TestScheduleToStartArmedForEachAttempt(t *testing.T) {
	const (
		scheduleToStart = time.Minute
		backoff         = 30 * time.Second
	)
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), activityTasks)
	t.Cleanup(server.Stop)
	other := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(other.Stop)

	runID, _ := scheduleTimedCommand(t, server, mgr, "per-attempt-s2s", history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		ScheduleToStartTimeout: scheduleToStart, RetryPolicy: retryPolicy(3, backoff),
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
	failedAt := clk.Now()
	// lookahead 覆盖第 2 次 attempt 的 ScheduleToStart 截止时间（退避 30s + 1m）。
	if _, _, err := other.Reconcile(context.Background(), DefaultOverdueTimerGrace, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	ref := activityTimerRef(timerActivityScheduleToStart, runID, st.ScheduledEventID)
	want := failedAt.Add(backoff + scheduleToStart)
	for name, srv := range map[string]*Server{"holder": server, "other": other} {
		if deadline, armed := srv.timers.Deadline(ref); !armed || !deadline.Equal(want) {
			t.Fatalf("%s schedule-to-start for attempt 2: deadline=%v armed=%v, want %v", name, deadline, armed, want)
		}
	}

	// Edge Case: 退避期加上一个 ScheduleToStart 周期之内不超时。
	clk.Advance(backoff + scheduleToStart - time.Millisecond)
	if events, _ := mgr.GetEvents(runID); countEngineEvents(events, history.EventActivityTimedOut) != 0 {
		t.Fatal("attempt 2 timed out before its schedule-to-start deadline")
	}
	if current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID); current.Attempt != 2 || !current.IsScheduled() {
		t.Fatalf("state = %+v; want attempt 2 scheduled", current)
	}

	// Happy Path: 到期后 Activity 失败，不派发 attempt 3。
	clk.Advance(time.Millisecond)
	events, _ := mgr.GetEvents(runID)
	if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != (protocol.TimeoutTypeScheduleToStart) {
		t.Fatalf("timeout reasons = %v, want [%s]", reasons, (protocol.TimeoutTypeScheduleToStart))
	}
	if got := countEngineEvents(events, history.EventActivityFailed); got != 1 {
		t.Fatalf("ActivityFailed = %d, want 1", got)
	}
	current, _, _ := mgr.GetActivityState(runID, st.ScheduledEventID)
	if current.Attempt != 2 || !current.IsTerminal() {
		t.Fatalf("state = %+v; want attempt 2 terminal", current)
	}
	for _, task := range activityTasks.snapshot() {
		if task.Task.Attempt > 2 {
			t.Fatalf("attempt %d dispatched after a schedule-to-start timeout", task.Task.Attempt)
		}
	}
}

// Reset 出的 attempt 同样受 ScheduleToStart 约束，从 reset 时刻起算。
func TestScheduleToStartCountsFromActivityReset(t *testing.T) {
	const scheduleToStart = time.Minute
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	runID, _ := scheduleTimedCommand(t, server, mgr, "reset-s2s", history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity, ActivityID: "step", ActivityType: "slow",
		ScheduleToStartTimeout: scheduleToStart, RetryPolicy: retryPolicy(3, time.Second),
	})
	states, err := mgr.ListActivityStates(runID)
	if err != nil || len(states) != 1 {
		t.Fatalf("activity states = %d, err = %v", len(states), err)
	}
	st := states[0]
	if err := server.HandleActivityStarted(runID, st.ScheduledEventID, 1, st.Stamp, 0, 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Minute)
	resetAt := clk.Now()
	if err := server.ResetActivity("", runID, st.ActivityID, execution.ResetActivityOptions{Reason: "retry by hand"}); err != nil {
		t.Fatal(err)
	}
	ref := activityTimerRef(timerActivityScheduleToStart, runID, st.ScheduledEventID)
	if deadline, armed := server.timers.Deadline(ref); !armed || !deadline.Equal(resetAt.Add(scheduleToStart)) {
		t.Fatalf("schedule-to-start after reset: deadline=%v armed=%v, want %v", deadline, armed, resetAt.Add(scheduleToStart))
	}
	clk.Advance(scheduleToStart)
	events, _ := mgr.GetEvents(runID)
	if reasons := timeoutReasons(events); len(reasons) != 1 || reasons[0] != (protocol.TimeoutTypeScheduleToStart) {
		t.Fatalf("timeout reasons = %v, want [%s]", reasons, (protocol.TimeoutTypeScheduleToStart))
	}
}
