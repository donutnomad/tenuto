package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 复现测试：activityAttemptFailed 后使用已取消的 ctx，重试不发生
// Error Case: retryActivity 闭包捕获了已取消的请求 ctx，
// 导致 time.AfterFunc 延迟触发时 enqueueRetryActivity → dispatchActivityTask → actQueue.Enqueue 因 ctx.Done() 返回错误，
// 错误被 _, _ = 丢弃，attempt 2 永不入队。
func TestActivityRetryFails_WithCancelledContext(t *testing.T) {
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()

	// 关键：使用 MemoryActivityQueueRouter（会检查 ctx.Done()），复现 demo 生产场景
	actQueue := memory.NewActivityQueueRouter(16)
	server := assembleServerWithActivityRouter(mgr, wfQueue, actQueue)
	defer server.Stop()

	run := &execution.Run{ID: "run-retry-ctx-bug", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	retryPolicy := &protocol.ParsedRetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    50 * time.Millisecond,
		BackoffCoefficient: 1,
	}

	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{
		ActivityID:   "settle",
		ActivityType: "demo.Flaky",
		TaskQueue:    "default",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 通过 startActivityTimers 注册 RetryPolicy
	server.startActivityTimers(run.ID, historypkg.Command{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "settle",
		ActivityType:        "demo.Flaky",
		TaskQueue:           "default",
		RetryPolicy:         retryPolicy,
	}, scheduledID, server.clock.Now())

	// 记录 attempt=1 started（需要先 started 才能 failed）
	_, err = mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ActivityID:       "settle",
		ScheduledEventID: scheduledID,
		Attempt:          1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：fail 权威读 state，seed 一个 started 状态（带 retry）供其读取并触发重试。
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledID, "settle", historypkg.ToRetryPolicy(retryPolicy))

	// 模拟 HTTP 请求 ctx：立即取消（模拟 HTTP 响应返回后 ctx 被取消）
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消，模拟 HTTP 请求完成

	// 调用 HandleActivityFailed，使用已取消的 ctx（复现 HTTP handler 行为）。
	// 严格 stamp：透传 seed state 的真实 stamp(1)。
	err = server.HandleActivityFailed(cancelledCtx, run.ID, scheduledID, 1, 1, protocol.Failure{Message: "transient settlement failure"})
	if err != nil {
		t.Fatalf("HandleActivityFailed returned error: %v", err)
	}

	// 等待 retryDelay（50ms）+ buffer，让 time.AfterFunc 触发
	time.Sleep(200 * time.Millisecond)

	// 验证：activityAttemptFailed 事件必须存在（说明重试流程被触发）
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	attemptFailedCount := countEngineEvents(history, historypkg.EventActivityAttemptFailed)
	if attemptFailedCount == 0 {
		t.Fatal("expected activityAttemptFailed event to be recorded")
	}

	// 关键断言：验证 attempt 2 是否被成功入队到 MemoryActivityQueueRouter
	dequeueCtx, dequeueCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dequeueCancel()
	lease, err := actQueue.Dequeue(dequeueCtx, "default")
	if err != nil {
		// Bug 复现：因为 ctx 取消，attempt 2 没有入队，Dequeue 超时
		t.Fatalf("BUG REPRODUCED: attempt 2 was NOT enqueued after retry delay.\n"+
			"Root cause: retryActivity closure captured cancelled HTTP request ctx,\n"+
			"enqueueRetryActivity → dispatchActivityTask → actQueue.Enqueue returned ctx.Err(), error silently discarded by _, _ =\n"+
			"Dequeue error: %v", err)
	}
	if lease.Task.Attempt != 2 {
		t.Fatalf("expected attempt=2 task in queue, got attempt=%d", lease.Task.Attempt)
	}
	t.Logf("attempt 2 task correctly enqueued: runID=%s scheduledEventID=%d attempt=%d",
		lease.Task.RunID, lease.Task.ScheduledEventID, lease.Task.Attempt)
}

// 对照测试：使用有效的 context.Background()，验证重试正常工作
func TestActivityRetrySucceeds_WithValidContext(t *testing.T) {
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	actQueue := memory.NewActivityQueueRouter(16)
	server := assembleServerWithActivityRouter(mgr, wfQueue, actQueue)
	defer server.Stop()

	run := &execution.Run{ID: "run-retry-ctx-ok", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	retryPolicy := &protocol.ParsedRetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    50 * time.Millisecond,
		BackoffCoefficient: 1,
	}

	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{
		ActivityID:   "settle",
		ActivityType: "demo.Flaky",
		TaskQueue:    "default",
	}))
	if err != nil {
		t.Fatal(err)
	}

	server.startActivityTimers(run.ID, historypkg.Command{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "settle",
		ActivityType:        "demo.Flaky",
		TaskQueue:           "default",
		RetryPolicy:         retryPolicy,
	}, scheduledID, server.clock.Now())

	_, err = mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ActivityID:       "settle",
		ScheduledEventID: scheduledID,
		Attempt:          1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledID, "settle", historypkg.ToRetryPolicy(retryPolicy))

	// 使用有效的长生命周期 ctx（等价于 server 级 ctx）。严格 stamp：透传真实 stamp(1)。
	err = server.HandleActivityFailed(context.Background(), run.ID, scheduledID, 1, 1, protocol.Failure{Message: "transient failure"})
	if err != nil {
		t.Fatalf("HandleActivityFailed returned error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	dequeueCtx, dequeueCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dequeueCancel()
	lease, err := actQueue.Dequeue(dequeueCtx, "default")
	if err != nil {
		t.Fatalf("attempt 2 was NOT enqueued with valid ctx: %v", err)
	}
	if lease.Task.Attempt != 2 {
		t.Fatalf("expected attempt=2, got attempt=%d", lease.Task.Attempt)
	}
	t.Logf("OK: attempt 2 correctly enqueued with valid ctx: attempt=%d", lease.Task.Attempt)
}
