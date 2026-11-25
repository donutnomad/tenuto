package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Timer 触发的状态变更走 keyed lock 并发测试（补步骤 7 审查发现的 P1）
// ============================================
// 背景: WFT(HandleWorkflowTaskCompleted/TimedOut) 已走 per-workflowID keyed lock 串行，但
// timer goroutine（time.AfterFunc）触发的状态变更原本绕过 keyed lock，与并发 WFT/signal 竞态。
// 现各 timer handler（handleWorkflowTimeout/handleRunTimeout/handleUserTimerFired/
// handleRetryBackoffFired/handleActivityTimeoutAttempt）入口统一拿同一把 keyedMu(workflowID)。
//
// 测试策略:
// - 串行化: timer fire（handleUserTimerFired 写 TimerFired 事件）与 SignalWorkflow 同 workflowID
//   并发，keyed lock 串行化两者 → signal 不丢、TimerFired 幂等、event ID 唯一、-race 干净、不死锁。
// - 重入死锁回归: timer handler 在持 keyedMu 时走下游（failWorkflow/prepareWorkflowFailure/startRetryRun），
//   这些下游均不拿 keyed lock、派 WFT 仅异步入队，故不重入；靠 -timeout 暴露潜在死锁。
// 锁顺序: keyedMu(workflowID)（timer handler 不触 commandMu，无 ABBA）。
// ============================================

// TestTimerKeyedLock_UserTimerFired_ConcurrentWithSignal_Serialized 验证 timer fire 与 signal
// 同 workflowID 并发时由 keyed lock 串行化：signal 全部落库不丢，TimerFired 幂等至多一次，不死锁、-race 干净。
func TestTimerKeyedLock_UserTimerFired_ConcurrentWithSignal_Serialized(t *testing.T) {
	mgr := memory.NewExecutionManager()
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)

	ref, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-timer-signal", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "run-timer-signal"
	run := &execution.Run{
		ID:                    runID,
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            runID,
		Status:                history.StatusRunning,
		IsCurrent:             true, // signal 解析 current 需其为 current
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	appendOpenWorkflowTaskForTest(t, mgr, runID)
	workflowID := run.WorkflowID

	// 构造一个 scheduled 的 UserTimer（TimerState），由测试手动 handleUserTimerFired 触发。
	const schedID int64 = 9001
	now := time.Now()
	if err := mgr.UpsertTimerState(&execution.TimerState{
		RunID:            runID,
		ScheduledEventID: schedID,
		TimerID:          "sleep-1",
		Kind:             execution.TimerKindEnums.UserTimer,
		Deadline:         now.Add(time.Hour),
		Status:           execution.TimerStatusEnums.Scheduled,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	const n = 24
	var wg sync.WaitGroup

	// 并发组 1：N goroutine 同 workflowID signal（runID 空，锁内解析 current）。
	signalErrs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, signalErrs[idx] = srv.SignalWorkflow(context.Background(), protocol.DefaultNamespace, workflowID, "", "sig", "", map[string]any{"i": int64(idx)})
		}(i)
	}

	// 并发组 2：M goroutine 对同一 UserTimer 并发 handleUserTimerFired（模拟 timer 到期），
	// 同 (runID,schedID) 重复 fire 应被幂等去重（TimerFired 至多一次），且与 signal 经 keyed lock 串行不死锁。
	const m = 8
	for range m {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.handleUserTimerFired(context.Background(), runID, schedID, "sleep-1")
		}()
	}

	wg.Wait()

	for i, e := range signalErrs {
		if e != nil {
			t.Fatalf("concurrent signal %d failed: %v", i, e)
		}
	}

	events := mustEvents(t, mgr, runID)
	// signal 不丢：N 个 signal 事件全部落库。
	if got := countSignaledEvents(events); got != n {
		t.Fatalf("expected %d signal events (none lost), got %d", n, got)
	}
	// timer fire 幂等：同一 (runID,schedID) 至多落 1 个 TimerFired。
	if got := countEngineEvents(events, history.EventTimerFired); got != 1 {
		t.Fatalf("expected exactly 1 TimerFired event (idempotent under concurrent fire), got %d", got)
	}
	// 事件 ID 唯一（keyed lock + 原子事务保证不交错损坏）。
	seen := make(map[int64]bool, len(events))
	for _, e := range events {
		if seen[e.ID] {
			t.Fatalf("duplicate event id %d (events interleaved/corrupted)", e.ID)
		}
		seen[e.ID] = true
	}
}

// TestTimerKeyedLock_WorkflowTimeout_NoReentrantDeadlock 是重入死锁回归：handleWorkflowTimeout
// 在持 keyedMu(workflowID) 时走 failWorkflow → prepareWorkflowFailure → workflowFailedAfterCommit。这些下游
// 仅 RunInTx，不拿 keyed lock，派 WFT 仅异步入队，故不重入。若误重入则本测试卡死（靠 -timeout 暴露）。
func TestTimerKeyedLock_WorkflowTimeout_NoReentrantDeadlock(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-timer-wftimeout", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-timer-wftimeout",
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-timer-wftimeout",
		Status:                history.StatusRunning,
		IsCurrent:             true,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleWorkflowTimeout(run.ID) // 持 keyedMu 跑完整失败链，不得重入死锁
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleWorkflowTimeout deadlocked (reentrant keyed lock?)")
	}

	got, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != history.StatusFailed {
		t.Fatalf("expected run failed after workflow timeout, got %s", got.Status)
	}
	if countEngineEvents(mustEvents(t, mgr, run.ID), history.EventWorkflowFailed) != 1 {
		t.Fatal("expected exactly one WorkflowFailed event")
	}
	_ = ctx
}

// TestTimerKeyedLock_RunTimeout_WithRetry_NoReentrantDeadlock 是重入死锁回归（retry 分支）：
// handleRunTimeout 在持 keyedMu 时经 prepareWorkflowFailure → startRetryRun 建同 workflowID 新 run。
// startRetryRun 仅 RunInTx + 排异步 RetryBackoff timer，不拿 keyed lock，故不重入。
func TestTimerKeyedLock_RunTimeout_WithRetry_NoReentrantDeadlock(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Program:     programtest.Noop(),
		Type:        "wf-timer-runtimeout-retry",
		RetryPolicy: &program.RetryPolicy{MaxAttempts: 3, InitialInterval: "10ms"},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-timer-runtimeout-retry",
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowType,
		WorkflowType:          ref.WorkflowType,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-timer-runtimeout-retry",
		Status:                history.StatusRunning,
		IsCurrent:             true, // retry CAS expectedOld=旧run 需其为 current
		Attempt:               1,
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, run)

	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleRunTimeout(run.ID) // 持 keyedMu 走 retry 链，建同 workflowID 新 run，不得重入死锁
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleRunTimeout deadlocked on retry path (reentrant keyed lock?)")
	}

	// retry 路径：旧 run 进入 Failed，且产生了一个新的 retry run（current 已切走）。
	oldRun, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldRun.Status != history.StatusFailed {
		t.Fatalf("expected old run failed after run timeout retry, got %s", oldRun.Status)
	}
}
