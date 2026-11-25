package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// ResetActivity 服务测试
// ============================================
// 测试策略:
// - Happy Path: 通过 service → server.ResetActivity 全链路，
//   pending activity 的 attempt 被重置回 1
// - Error Case: 对已终态 activity 调用 ResetActivity 应返回错误
// ============================================

func TestResetActivityService_HappyPath(t *testing.T) {
	// 通过 service → server.ResetActivity 全链路，
	// pending activity 的 attempt 被重置回 1
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	actQueue := memory.NewActivityQueueRouter(16)
	server := assembleServerWithActivityRouter(mgr, wfQueue, actQueue)
	defer server.Stop()

	ctx := context.Background()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-ra-http", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	run := &execution.Run{
		ID:                    "run-ra-http-happy",
		WorkflowID:            "wf-ra-http",
		WorkflowType:          "wf-ra-http",
		WorkflowConfigVersion: "v1",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
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
		ActivityID:   "step-http",
		ActivityType: "demo.Task",
		TaskQueue:    "default",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 挂上 Activity timer
	server.startActivityTimers(run.ID, history.Command{
		StartToCloseTimeout: time.Minute,
		Type:                history.CmdScheduleActivity,
		ActivityID:          "step-http",
		ActivityType:        "demo.Task",
		TaskQueue:           "default",
		RetryPolicy:         retryPolicy,
	}, scheduledEventID, server.clock.Now())

	// 写入 ActivityStarted（attempt=1）
	if _, err := mgr.Append(run.ID, run.ID, run.ID, history.NewActivityStartedEvent(history.ActivityStartedEvent{
		ActivityID:       "step-http",
		ScheduledEventID: scheduledEventID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：fail/reset 权威读 state，seed 一个 started 状态（带 retry）供其读取。
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledEventID, "step-http", history.ToRetryPolicy(retryPolicy))

	// 触发重试，推进 attempt 到 2。严格 stamp：透传真实 stamp(1)。
	if err := server.HandleActivityFailed(ctx, run.ID, scheduledEventID, 1, 1, protocol.Failure{Message: "transient"}); err != nil {
		t.Fatalf("HandleActivityFailed: %v", err)
	}

	// 确认 attempt 已推进到 2
	attemptBefore := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if attemptBefore != 2 {
		t.Fatalf("setup: expected attempt=2, got %d", attemptBefore)
	}

	// 通过运行时服务调用 ResetActivity
	if err := server.ResetActivity("", run.ID, activityIDForTest(t, server, run.ID, scheduledEventID), execution.ResetActivityOptions{Reason: "http-retry"}); err != nil {
		t.Fatalf("client.ResetActivity: %v", err)
	}

	// 断言持久化 attempt 已重置回 1
	gotAttempt := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if gotAttempt != 1 {
		t.Fatalf("attempt = %d, want 1", gotAttempt)
	}

	// 断言 ActivityReset 事件已写入 history
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range history {
		if e.ActivityReset != nil && e.ActivityReset.ScheduledEventID == scheduledEventID {
			found = true
			if e.ActivityReset.Reason != "http-retry" {
				t.Errorf("reset event Reason = %q, want %q", e.ActivityReset.Reason, "http-retry")
			}
		}
	}
	if !found {
		t.Error("expected ActivityReset event in history after ResetActivity")
	}
}

func TestResetActivityService_TerminalActivityReturnsError(t *testing.T) {
	// 对已写 ActivityFailed（终态）的 activity 调用 ResetActivity 应返回错误。
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:           "run-ra-http-terminal",
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

	// 写入 ActivityFailed（终态），使 activityAlreadyClosed 返回 true
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
		t.Fatal("expected error for terminal activity")
	}
}
