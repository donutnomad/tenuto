package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestResetActivity_RedispatchAfterReset 回归测试（Integration）：ResetActivity 后 worker 能重新执行 activity。
// 场景：attempt 1 通过 HandleWorkflowTaskCompleted 正式调度 → worker 领取 → 失败（写入 ActivityAttemptFailed(attempt=1)）
// → 重试 timer 触发 attempt 2 → worker 领取 → 失败（attempt=3，等待 重试 timer 30s）
// → 用户调 ResetActivity → activity 重新入队（attempt=1）
// → worker 调 BeginActivityTask(attempt=1) 必须返回 Execute=true，不能被旧的 ActivityAttemptFailed(attempt=1) 误判阻止。
// Attempt 是否已结束按 ActivityState 判定，ResetActivity 之后旧 attempt 的失败事件不影响新的 attempt 1。
func TestResetActivity_RedispatchAfterReset(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	actQueue := memory.NewActivityQueueRouter(16)
	server := assembleServerWithActivityRouter(mgr, wfQueue, actQueue)
	defer server.Stop()

	// 注册 workflow 配置
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "wf-reset-real-world",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step-settle": {Symbol: "step-settle", Type: "demo.Flaky", TaskQueue: "default"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	run := &execution.Run{
		ID:                    "run-reset-real-world",
		WorkflowID:            "wf-reset-real-world",
		WorkflowType:          "wf-reset-real-world",
		WorkflowConfigVersion: "v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	retryPolicy := &protocol.ParsedRetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    30 * time.Second, // 长 interval，确保 重试 timer 不会自动触发
		BackoffCoefficient: 1.0,
	}

	// Step 1: 通过 HandleWorkflowTaskCompleted 真正调度 activity
	// 这会调用 dispatchActivityTask，产生 dispatch key {attempt:1, status:Dispatched}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "step-settle",
		ActivityType:        "demo.Flaky",
		TaskQueue:           "default",
		RetryPolicy:         retryPolicy,
	}}, "")
	if err != nil {
		t.Fatalf("HandleWorkflowTaskCompleted: %v", err)
	}

	// 找到 scheduledEventID
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var scheduledEventID int64
	for _, e := range history {
		if e.ActivityScheduled != nil {
			scheduledEventID = e.ID
		}
	}
	if scheduledEventID == 0 {
		t.Fatal("expected ActivityScheduled event")
	}
	t.Logf("scheduledEventID=%d", scheduledEventID)

	// 验证 attempt == 1
	if got := activityAttemptForTest(t, server, run.ID, scheduledEventID); got != 1 {
		t.Fatalf("expected attempt=1 after schedule, got %d", got)
	}

	// Step 2: Worker 领取 attempt=1 的任务（Dequeue），此时 dispatch{attempt:1}=Dispatched
	deqCtx, deqCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer deqCancel()
	lease1, err := actQueue.Dequeue(deqCtx, "default")
	if err != nil {
		t.Fatalf("Dequeue attempt=1: %v", err)
	}
	if lease1.Task.Attempt != 1 {
		t.Fatalf("expected attempt=1 task, got attempt=%d", lease1.Task.Attempt)
	}
	t.Logf("Worker got attempt=%d task", lease1.Task.Attempt)

	// Step 3: HandleActivityStarted for attempt=1。严格 stamp：从权威 state 读真实 stamp 透传
	//（旧 enqueue 路径不携带 stamp，故此处直接读 state，模拟 worker 用 dispatch task 的真实 stamp）。
	st1, _, _ := server.GetActivityState(run.ID, scheduledEventID)
	if err := server.HandleActivityStarted(run.ID, scheduledEventID, 1, st1.Stamp, 60*time.Second, 0); err != nil {
		t.Fatalf("HandleActivityStarted attempt=1: %v", err)
	}

	// Step 4: attempt=1 失败 → retryActivity 写入 ActivityAttemptFailed(attempt=1)，attempt=2，设置 重试 timer（30s）
	if err := server.HandleActivityFailed(ctx, run.ID, scheduledEventID, 1, st1.Stamp, protocol.Failure{Message: "transient error attempt 1"}); err != nil {
		t.Fatalf("HandleActivityFailed attempt=1: %v", err)
	}

	// 验证 ActivityAttemptFailed(attempt=1) 已写入历史（这是 bug 的关键前提）
	history, _ = mgr.GetEvents(run.ID)
	hasAttempt1Failed := false
	for _, e := range history {
		if e.ActivityAttemptFailed != nil && e.ActivityAttemptFailed.ScheduledEventID == scheduledEventID && historypkg.NormalizeActivityAttempt(e.ActivityAttemptFailed.Attempt) == 1 {
			hasAttempt1Failed = true
		}
	}
	if !hasAttempt1Failed {
		t.Fatal("setup: expected ActivityAttemptFailed(attempt=1) in history")
	}
	t.Logf("setup: ActivityAttemptFailed(attempt=1) written to history — this is the key prerequisite for the bug")

	attemptAfterFail1 := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if attemptAfterFail1 != 2 {
		t.Fatalf("expected attempt=2 after attempt1 failure, got %d", attemptAfterFail1)
	}

	// Step 5: 手动模拟 重试 timer 到期触发（30s 太长，直接走 state 重派路径，携带 state 最新 attempt+stamp）
	stRetry, _, _ := server.GetActivityState(run.ID, scheduledEventID)
	dispatched, err := server.redispatchActivityTask(ctx, server.activityTaskFromState(stRetry))
	if err != nil {
		t.Fatalf("redispatchActivityTask attempt=2: %v", err)
	}
	t.Logf("redispatchActivityTask dispatched=%v (attempt=%d)", dispatched, stRetry.Attempt)

	// Step 6: Worker 领取 attempt=2 的任务
	deqCtx2, deqCancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer deqCancel2()
	lease2, err := actQueue.Dequeue(deqCtx2, "default")
	if err != nil {
		t.Fatalf("Dequeue attempt=2: %v", err)
	}
	if lease2.Task.Attempt != 2 {
		t.Fatalf("expected attempt=2 task, got attempt=%d", lease2.Task.Attempt)
	}
	t.Logf("Worker got attempt=%d task", lease2.Task.Attempt)

	// Step 7: HandleActivityStarted for attempt=2。严格 stamp：读 state 真实 stamp（retry 后已 ++）。
	st2, _, _ := server.GetActivityState(run.ID, scheduledEventID)
	if err := server.HandleActivityStarted(run.ID, scheduledEventID, 2, st2.Stamp, 60*time.Second, 0); err != nil {
		t.Fatalf("HandleActivityStarted attempt=2: %v", err)
	}

	// Step 8: attempt=2 失败 → ActivityAttemptFailed(attempt=2) 写入历史，attempt=3，设置 重试 timer（30s）
	if err := server.HandleActivityFailed(ctx, run.ID, scheduledEventID, 2, st2.Stamp, protocol.Failure{Message: "transient error attempt 2"}); err != nil {
		t.Fatalf("HandleActivityFailed attempt=2: %v", err)
	}

	attemptAfterFail2 := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if attemptAfterFail2 != 3 {
		t.Fatalf("expected attempt=3 after attempt2 failure, got %d", attemptAfterFail2)
	}
	t.Logf("After attempt=2 failure, attempt=%d (重试 timer pending 30s — this is when ResetActivity is called)", attemptAfterFail2)

	// 此时 history 中有：ActivityAttemptFailed(attempt=1)、ActivityAttemptFailed(attempt=2)

	// Step 9: 用户调用 ResetActivity（attempt=3，等待 重试 timer 30s 触发）
	err = server.ResetActivity("", run.ID, activityIDForTest(t, server, run.ID, scheduledEventID), execution.ResetActivityOptions{Reason: "manual reset after 2 failures"})
	if err != nil {
		t.Fatalf("ResetActivity: %v", err)
	}

	// 验证 attempt 已被重置为 1
	attemptAfterReset := activityAttemptForTest(t, server, run.ID, scheduledEventID)
	if attemptAfterReset != 1 {
		t.Errorf("after ResetActivity: expected attempt=1, got %d", attemptAfterReset)
	}
	t.Logf("after ResetActivity: attempt=%d (correct)", attemptAfterReset)

	// Step 10: 任务重新入队，worker 从 actQueue Dequeue 到 attempt=1 的任务
	deqCtx3, deqCancel3 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer deqCancel3()
	lease3, err := actQueue.Dequeue(deqCtx3, "default")
	if err != nil {
		t.Fatalf("after ResetActivity, worker cannot Dequeue activity task: %v", err)
	}
	t.Logf("after ResetActivity, got task: attempt=%d scheduledEventID=%d", lease3.Task.Attempt, lease3.Task.ScheduledEventID)
	if lease3.Task.Attempt != 1 {
		t.Errorf("expected attempt=1 after reset, got attempt=%d", lease3.Task.Attempt)
	}

	// Step 11: 关键验证 - BeginActivityTask(attempt=1) 必须返回 Execute=true。
	// 修复前：activityAttemptClosed 不感知 ActivityReset，误判 attempt=1 已关闭（reason="attempt_closed"）→ worker 永久不执行。
	// 修复后：只看 ActivityReset 事件之后的 AttemptFailed/AttemptTimedOut，reset 前的旧记录被跳过。
	runtimeService := NewActivityRuntimeService(server)
	beginResult, err := runtimeService.BeginActivityTask(ctx, lease3.Task)
	if err != nil {
		t.Fatalf("BeginActivityTask: %v", err)
	}
	t.Logf("BeginActivityTask result: Execute=%v Reason=%q", beginResult.Execute, beginResult.Reason)
	if !beginResult.Execute {
		t.Errorf("FIX REGRESSION: BeginActivityTask(attempt=1) returned Execute=false (reason=%q) after ResetActivity. "+
			"activityAttemptClosed is incorrectly treating pre-reset ActivityAttemptFailed(attempt=1) as still valid.", beginResult.Reason)
	} else {
		t.Logf("FIX VERIFIED: BeginActivityTask(attempt=1) returned Execute=true after ResetActivity")
	}
}
