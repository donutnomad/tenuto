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

// Happy Path: 对一个 attempt=2 的 pending activity 调用 ResetActivity，
// attempt 重置回 1、写入 ActivityReset 事件、activity 重新入队。
func TestResetActivity_PendingActivityResetsToAttempt1(t *testing.T) {
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	actQueue := memory.NewActivityQueueRouter(16)
	server := assembleServerWithActivityRouter(mgr, wfQueue, actQueue)
	defer server.Stop()

	run := &execution.Run{
		ID:           "run-reset-activity-happy",
		WorkflowID:   "wf",
		WorkflowType: "wf",
		Status:       history.StatusRunning,
		CreatedAt:    time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	retryPolicy := &protocol.ParsedRetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    10 * time.Millisecond,
		BackoffCoefficient: 1,
	}

	// 调度 activity，写入 ActivityScheduled 事件
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
		ActivityID:   "step-reset",
		ActivityType: "demo.Task",
		TaskQueue:    "default",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 通过 startActivityTimers 挂上 Activity timer
	server.startActivityTimers(run.ID, history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity,
		ActivityID:          "step-reset",
		ActivityType:        "demo.Task",
		TaskQueue:           "default",
		RetryPolicy:         retryPolicy,
	}, scheduledEventID, server.clock.Now())

	// 记录 attempt=1 started
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityStartedEvent(history.ActivityStartedEvent{
		ActivityID:       "step-reset",
		ScheduledEventID: scheduledEventID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：fail/reset 权威读 state，seed 一个 started 状态（带 retry）供其读取。
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledEventID, "step-reset", history.ToRetryPolicy(retryPolicy))

	// 模拟 attempt=1 失败触发重试，让 attempt 推进到 2。严格 stamp：透传真实 stamp(1)。
	if err := server.HandleActivityFailed(context.Background(), run.ID, scheduledEventID, 1, 1, protocol.Failure{Message: "transient error"}); err != nil {
		t.Fatalf("HandleActivityFailed: %v", err)
	}

	// 确认 attempt 已经是 2（setup 验证）
	attemptBefore := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if attemptBefore != 2 {
		t.Fatalf("setup: expected attempt=2, got attempt=%d", attemptBefore)
	}

	// 调用 ResetActivity
	err = server.ResetActivity("", run.ID, "step-reset", execution.ResetActivityOptions{Reason: "manual retry"})
	if err != nil {
		t.Fatalf("ResetActivity: %v", err)
	}

	// 断言 1：持久化 attempt == 1
	gotAttempt := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if gotAttempt != 1 {
		t.Fatalf("attempt = %d, want 1", gotAttempt)
	}

	// 断言 2：写入了 ActivityReset 事件
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range history {
		if e.ActivityReset != nil && e.ActivityReset.ScheduledEventID == scheduledEventID {
			found = true
			if e.ActivityReset.Attempt != 2 {
				t.Errorf("reset event Attempt = %d, want 2 (pre-reset attempt)", e.ActivityReset.Attempt)
			}
			if e.ActivityReset.Reason != "manual retry" {
				t.Errorf("reset event Reason = %q, want %q", e.ActivityReset.Reason, "manual retry")
			}
		}
	}
	if !found {
		t.Error("expected ActivityReset event in history")
	}

	// 断言 3：activity 任务重新入队（attempt=1），从 MemoryActivityQueueRouter Dequeue 验证
	dequeueCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	lease, err := actQueue.Dequeue(dequeueCtx, "default")
	if err != nil {
		t.Fatalf("expected re-enqueued activity task after ResetActivity, Dequeue error: %v", err)
	}
	if lease.Task.Attempt != 1 {
		t.Fatalf("expected re-enqueued task attempt=1, got attempt=%d", lease.Task.Attempt)
	}
	if lease.Task.ScheduledEventID != scheduledEventID {
		t.Fatalf("expected re-enqueued task scheduledEventID=%d, got %d", scheduledEventID, lease.Task.ScheduledEventID)
	}
}

// Error Case: 对已终态（已写 ActivityFailed）的 activity 调用 ResetActivity 返回错误。
func TestResetActivity_TerminalActivityRejected(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:           "run-reset-terminal",
		WorkflowID:   "wf",
		WorkflowType: "wf",
		Status:       history.StatusRunning,
		CreatedAt:    time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	// 调度 activity
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
		ActivityID:   "step-terminal",
		ActivityType: "demo.Task",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 写入 ActivityFailed（终态）
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityFailedEvent(history.ActivityFailedEvent{
		ActivityID:       "step-terminal",
		ScheduledEventID: scheduledEventID,
		Attempt:          1,
		Error:            "exhausted",
		Failure:          &protocol.Failure{Message: "exhausted"},
	})); err != nil {
		t.Fatal(err)
	}

	err = server.ResetActivity("", run.ID, "step-terminal", execution.ResetActivityOptions{})
	if err == nil {
		t.Fatal("expected error for terminal activity, got nil")
	}
}

// Error Case: workflow 非 Running（completed）时 ResetActivity 返回错误。
func TestResetActivity_NonRunningWorkflowRejected(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	// 构造一个已 Completed 的 run
	run := &execution.Run{
		ID:           "run-reset-completed",
		WorkflowID:   "wf",
		WorkflowType: "wf",
		Status:       history.StatusRunning,
		CreatedAt:    time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	// 调度 activity（activity 存在但 workflow 非 Running）
	_, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
		ActivityID:   "step-completed-wf",
		ActivityType: "demo.Task",
	}))
	if err != nil {
		t.Fatal(err)
	}
	markRunTerminalForTest(t, mgr, run.ID, history.StatusCompleted, nil)

	err = server.ResetActivity("", run.ID, "step-completed-wf", execution.ResetActivityOptions{})
	if err == nil {
		t.Fatal("expected error for non-running workflow, got nil")
	}
}

// Error Case: ActivityID 没有对应的未关闭 activity 时返回错误。
func TestResetActivity_UnknownActivityRejected(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:           "run-reset-unknown-activity",
		WorkflowID:   "wf",
		WorkflowType: "wf",
		Status:       history.StatusRunning,
		CreatedAt:    time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	err := server.ResetActivity("", run.ID, "missing", execution.ResetActivityOptions{})
	if err == nil {
		t.Fatal("expected error for unknown activity, got nil")
	}
}
