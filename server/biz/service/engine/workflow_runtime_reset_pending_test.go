package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// Workflow Reset：分叉点上未结束的 Activity 与 Timer
// ============================================
// 测试策略（假时钟驱动）：
// - Happy Path: 未开始的 Activity 在新 Run 里以 Attempt 1 重新派发，Schedule-To-Close 从 reset 时刻起算，能完成。
// - Happy Path: 已开始的 Activity 在新 Run 里收到不可重试的 ResetWorkflowError，带上旧 Run 的最后一次心跳。
// - Happy Path: Timer 按原 deadline 触发，reset 时已过期的立即触发。
// - Error Case: 旧 Run 上仍在执行的 attempt 迟到的完成被拒绝，旧 Run History 不变。
// ============================================

// closeSecondWorkflowTask 再完成一个不带命令的 Workflow Task，last_workflow_task 的分叉点落在已调度的 Activity/Timer 之后。
func closeSecondWorkflowTask(t *testing.T, server *Server, mgr execution.ExecutionManager, runID string) {
	t.Helper()
	wftID := appendOpenWorkflowTaskForTest(t, mgr, runID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, nil, ""); err != nil {
		t.Fatal(err)
	}
}

func resetLastWorkflowTask(t *testing.T, server *Server, runID string) string {
	t.Helper()
	newRunID, err := server.ResetWorkflow(context.Background(), execution.ResetWorkflowRequest{
		RunID: runID, Reason: "bad deploy", ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatalf("ResetWorkflow: %v", err)
	}
	return newRunID
}

// requireOldAttemptRejected 断言旧 Run 上的 attempt 迟到的完成不进入任何 History：走引擎入口时被丢弃，
// 绕过入口直接在事务里提交时被终态 fence 以 ErrRunClosed 拒绝。
func requireOldAttemptRejected(t *testing.T, server *Server, mgr execution.ExecutionManager, oldRunID string, task protocol.Task) {
	t.Helper()
	before := len(mustEvents(t, mgr, oldRunID))
	_ = server.HandleActivityCompleted(context.Background(), oldRunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"late": true})
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.CompleteActivity(tx, execution.ActivityTransition{
			RunID: oldRunID, TreeID: oldRunID, BranchID: oldRunID,
			ScheduledEventID: task.ScheduledEventID, Attempt: task.Attempt, Stamp: task.Stamp, Now: server.clock.Now(),
		}, map[string]any{"late": true})
		return err
	})
	if !errors.Is(err, execution.ErrRunClosed) {
		t.Fatalf("late completion on the replaced run: err = %v, want ErrRunClosed", err)
	}
	if after := mustEvents(t, mgr, oldRunID); len(after) != before || countEngineEvents(after, history.EventActivityCompleted) != 0 {
		t.Fatalf("replaced run history changed by a late completion: %+v", after)
	}
}

func TestResetRedispatchesUnstartedActivity(t *testing.T) {
	server, mgr, activityTasks, clk := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "reset-pending-unstarted", history.Command{
		Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "slow",
		ScheduleToCloseTimeout: time.Minute, StartToCloseTimeout: time.Minute, RetryPolicy: retryPolicy(3, time.Second),
	})
	oldTask := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.RunID == runID })
	// 旧 Run 上的 Worker 已领取并开始执行（transient start），分叉点上这个 Activity 仍是未开始。
	if _, err := server.HandleActivityStartedWithMetadata(oldTask, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	closeSecondWorkflowTask(t, server, mgr, runID)

	clk.Advance(40 * time.Second)
	resetAt := clk.Now()
	newRunID := resetLastWorkflowTask(t, server, runID)

	state, ok, err := server.GetActivityState(newRunID, oldTask.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("new run activity state: ok=%v err=%v", ok, err)
	}
	if state.Attempt != 1 || state.Stamp != 1 || !state.IsScheduled() || !state.ScheduledTime.Equal(resetAt) {
		t.Fatalf("new run activity state = %+v, want attempt 1 scheduled at the reset", state)
	}
	if deadline, armed := server.timers.Deadline(activityTimerRef(timerActivityScheduleToClose, newRunID, oldTask.ScheduledEventID)); !armed || !deadline.Equal(resetAt.Add(time.Minute)) {
		t.Fatalf("schedule-to-close timer = %v armed=%v, want one minute after the reset", deadline, armed)
	}
	newTask := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.RunID == newRunID })
	if newTask.Attempt != 1 || newTask.ActivityID != "charge" || !newTask.ScheduledTime.Equal(resetAt) {
		t.Fatalf("redispatched task = %+v", newTask)
	}

	// 原调度时刻起的一分钟已过（reset 后 20s），新 Run 的 Activity 仍未超时。
	clk.Advance(59 * time.Second)
	requireEventCount(t, mgr, newRunID, history.EventActivityFailed, 0)

	requireOldAttemptRejected(t, server, mgr, runID, oldTask)

	if _, err := server.HandleActivityStartedWithMetadata(newTask, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(context.Background(), newRunID, newTask.ScheduledEventID, newTask.Attempt, newTask.Stamp, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	requireEventCount(t, mgr, newRunID, history.EventActivityCompleted, 1)
	if state, _, _ := server.GetActivityState(newRunID, newTask.ScheduledEventID); state.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("new run activity status = %s, want completed", state.Status)
	}
}

func TestResetUnstartedActivityScheduleToCloseRestartsAtReset(t *testing.T) {
	server, mgr, activityTasks, clk := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "reset-pending-s2c", history.Command{
		Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "slow",
		ScheduleToCloseTimeout: time.Minute, RetryPolicy: retryPolicy(3, time.Second),
	})
	waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.RunID == runID })
	closeSecondWorkflowTask(t, server, mgr, runID)
	clk.Advance(40 * time.Second)
	newRunID := resetLastWorkflowTask(t, server, runID)

	clk.Advance(time.Minute - time.Millisecond)
	requireEventCount(t, mgr, newRunID, history.EventActivityFailed, 0)
	clk.Advance(time.Millisecond)
	waitForEngineEventCount(t, mgr, newRunID, history.EventActivityFailed, 1, 5*time.Second)
}

func TestResetFailsStartedActivity(t *testing.T) {
	server, mgr, activityTasks, _ := newTimeoutServer(t)
	runID, _ := scheduleTimedCommand(t, server, mgr, "reset-pending-started", history.Command{
		Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "slow",
		StartToCloseTimeout: time.Minute, RetryPolicy: retryPolicy(1, 0),
	})
	oldTask := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.RunID == runID })
	// MaxAttempts=1：开始时立即写 ActivityStarted，分叉点上这个 Activity 已开始。
	if _, err := server.HandleActivityStartedWithMetadata(oldTask, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityHeartbeat(runID, oldTask.ScheduledEventID, oldTask.Attempt, oldTask.Stamp, map[string]any{"offset": 7}); err != nil {
		t.Fatal(err)
	}
	closeSecondWorkflowTask(t, server, mgr, runID)

	newRunID := resetLastWorkflowTask(t, server, runID)

	events := mustEvents(t, mgr, newRunID)
	var failed *history.ActivityFailedEvent
	var startedID int64
	for i, event := range events {
		if event.ActivityStarted != nil && event.ActivityStarted.ScheduledEventID == oldTask.ScheduledEventID {
			startedID = event.ID
		}
		if event.ActivityFailed != nil {
			failed = event.ActivityFailed
			if i == 0 || events[i-1].WorkflowTaskFailed == nil {
				t.Fatalf("ActivityFailed must follow the reset marker: %+v", events)
			}
		}
	}
	if failed == nil || failed.ScheduledEventID != oldTask.ScheduledEventID || failed.StartedEventID != startedID || startedID == 0 {
		t.Fatalf("new run ActivityFailed = %+v (started %d), want the started attempt failed", failed, startedID)
	}
	if failed.Failure == nil || failed.Failure.Type != protocol.ResetWorkflowFailureType || !failed.Failure.NonRetryable ||
		failed.RetryState != "NonRetryableFailure" || fmt.Sprint(failed.LastHeartbeatDetails["offset"]) != "7" {
		t.Fatalf("ActivityFailed = %+v failure = %+v, want a non-retryable ResetWorkflowError with the heartbeat", failed, failed.Failure)
	}
	if _, ok, _ := server.GetActivityState(newRunID, oldTask.ScheduledEventID); ok {
		t.Fatal("a failed activity must not be carried into the new run")
	}
	if got := countEngineEvents(events, history.EventWorkflowTaskScheduled); got < 1 || events[len(events)-1].WorkflowTaskScheduled == nil {
		t.Fatalf("reset must schedule a workflow task after the failure: %+v", events)
	}
	if queued := activityTasks.snapshot(); len(queued) != 1 {
		t.Fatalf("queued activity tasks = %d, want only the original attempt", len(queued))
	}

	requireOldAttemptRejected(t, server, mgr, runID, oldTask)
}

func TestResetKeepsTimerDeadlines(t *testing.T) {
	server, mgr, _, clk := newTimeoutServer(t)
	start := clk.Now()
	runID, wftID := startTimedActivityRun(t, server, mgr, "reset-pending-timer")
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []history.Command{
		{Type: history.CmdScheduleTimer, TimerID: "short", Delay: 20 * time.Second},
		{Type: history.CmdScheduleTimer, TimerID: "long", Delay: 2 * time.Minute},
	}, ""); err != nil {
		t.Fatal(err)
	}
	closeSecondWorkflowTask(t, server, mgr, runID)

	// short 在旧 Run 里分叉点之后触发；reset 时它在新 Run 里已过期。
	clk.Advance(30 * time.Second)
	waitForEngineEventCount(t, mgr, runID, history.EventTimerFired, 1, 5*time.Second)
	newRunID := resetLastWorkflowTask(t, server, runID)

	waitForEngineEventCount(t, mgr, newRunID, history.EventTimerFired, 1, 5*time.Second)
	fired := firedTimerIDs(t, mgr, newRunID)
	if len(fired) != 1 || fired[0] != "short" {
		t.Fatalf("fired timers right after reset = %v, want [short]", fired)
	}

	advanceTo(clk, start.Add(2*time.Minute-time.Millisecond))
	if fired := firedTimerIDs(t, mgr, newRunID); len(fired) != 1 {
		t.Fatalf("long timer fired before its original deadline: %v", fired)
	}
	advanceTo(clk, start.Add(2*time.Minute))
	waitForEngineEventCount(t, mgr, newRunID, history.EventTimerFired, 2, 5*time.Second)
}

func firedTimerIDs(t *testing.T, mgr execution.ExecutionManager, runID string) []string {
	t.Helper()
	var ids []string
	for _, event := range mustEvents(t, mgr, runID) {
		if event.TimerFired != nil {
			ids = append(ids, event.TimerFired.TimerID)
		}
	}
	return ids
}

