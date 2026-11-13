package engine

import (
	"context"
	"fmt"
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
// WFT 走 keyed lock 并发测试
// ============================================
// 测试策略:
// - 并发: 同 workflowID 下 WFT 处理（HandleWorkflowTaskCompleted）与 SignalWorkflow 并发，
//   per-WorkflowID keyed lock 单独串行化两者 → signal 不丢、WFT 不重复处理、-race 干净、不死锁。
// - 不同 workflowID 的 WFT 并发: 摘 commandMu 后不再被全局串行，各自走各自的 keyed lock，
//   验证并发提升下不死锁、状态正确。
// - 重入死锁回归: WFT 处理触发 CmdContinueAsNew（同 workflowID 开新代）在持锁状态下跑通，
//   证明 startContinuedRunWithRef → createStartedRun 不重入 keyed lock。
// 锁顺序: keyedMu(workflowID) -> mu / timers 内部锁。
// ============================================

// TestWFTKeyedLock_ConcurrentWithSignal_Serialized 验证同 workflowID 下 WFT 处理与 signal 并发时
// 由 per-workflow keyed lock 串行化：N 个 signal 全部落库不丢，并发 WFT 完成不死锁、不数据竞争。
func TestWFTKeyedLock_ConcurrentWithSignal_Serialized(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-wft-signal", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-wft-signal",
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-wft-signal",
		Status:                history.StatusRunning,
		IsCurrent:             true,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	const n = 24
	var wg sync.WaitGroup

	// 并发组 1：N goroutine 同 workflowID signal（runID 空，每次锁内解析 current）。
	signalErrs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, signalErrs[idx] = server.SignalWorkflow(ctx, protocol.DefaultNamespace, ref.WorkflowID, "", "sig", "", map[string]any{"i": int64(idx)})
		}(i)
	}

	// 并发组 2：M goroutine 对同一 WFT 并发 HandleWorkflowTaskCompleted（benign marker 命令，保持
	// run Running）。同 scheduledEventID 的重复完成应被幂等去重（WFT 不重复处理），不与 signal 死锁。
	const m = 8
	wftErrs := make([]error, m)
	for j := range m {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wftErrs[idx] = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []history.Command{{
				Type:       history.CmdRecordMarker,
				MarkerName: "noop",
				MarkerID:   "noop-marker",
			}}, "")
		}(j)
	}

	wg.Wait()

	for i, e := range signalErrs {
		if e != nil {
			t.Fatalf("concurrent signal %d failed: %v", i, e)
		}
	}
	for j, e := range wftErrs {
		if e != nil {
			t.Fatalf("concurrent WFT complete %d failed: %v", j, e)
		}
	}

	events := mustEvents(t, mgr, run.ID)
	// signal 不丢：N 个 signal 事件全部落库。
	if got := countSignaledEvents(events); got != n {
		t.Fatalf("expected %d signal events (none lost), got %d", n, got)
	}
	// WFT 不重复处理：同一 scheduledEventID 的 WFT 至多 close 一次（幂等去重）。
	if got := countEngineEvents(events, history.EventWorkflowTaskCompleted); got > 1 {
		t.Fatalf("WFT must be processed at most once (idempotent), got %d completed events", got)
	}
	// 事件 ID 唯一（per-workflow 锁 + 原子事务保证不乱）。
	seen := make(map[int64]bool, len(events))
	for _, e := range events {
		if seen[e.ID] {
			t.Fatalf("duplicate event id %d (events interleaved/corrupted)", e.ID)
		}
		seen[e.ID] = true
	}
}

// TestWFTKeyedLock_ContinueAsNew_NoReentrantDeadlock 是重入死锁回归：WFT 处理在持有
// keyedMu(workflowID) 时触发 CmdContinueAsNew（同 workflowID 开新代）。CAN 走
// startContinuedRunWithRef → createStartedRun（仅 RunInTx，不拿 keyed lock），故不重入。
// 若 createStartedRun 误拿同 workflowID keyed lock，本测试会卡死（靠 -timeout 暴露）。
func TestWFTKeyedLock_ContinueAsNew_NoReentrantDeadlock(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-wft-can", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-wft-can",
		Namespace:             protocol.DefaultNamespace,
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-wft-can",
		Status:                history.StatusRunning,
		IsCurrent:             true, // CAN 切代 expectedOld=旧run 需其为 current
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	// 在持 keyedMu(wf-wft-can) 的 WFT 处理内触发 CAN（同 workflowID 开新代）→ 必须不重入死锁。
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []history.Command{{
		Type:       history.CmdContinueAsNew,
		WorkflowID: ref.WorkflowID,
	}}, ""); err != nil {
		t.Fatal(err)
	}

	oldRun, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldRun.Status != history.StatusContinuedAsNew {
		t.Fatalf("expected old run continued-as-new, got %s", oldRun.Status)
	}
	if countEngineEvents(mustEvents(t, mgr, run.ID), history.EventWorkflowContinuedAsNew) != 1 {
		t.Fatal("expected exactly one continue-as-new event")
	}
}

// TestWFTKeyedLock_DistinctWorkflowIDs_ConcurrentNoDeadlock 验证摘掉 commandMu 后的收益：
// 不同 workflowID 的 WFT 处理不再被全局串行，各自走各自的 keyed lock 桶并发执行。
// （原 commandMu 全局锁会让这些 WFT 排队；现在仅同 workflowID 串行，不同 workflowID 并发。）
// 断言：每个 run 各自的 WFT 都成功 close 一次、状态正确、-race 干净、不死锁（靠 -timeout 暴露）。
func TestWFTKeyedLock_DistinctWorkflowIDs_ConcurrentNoDeadlock(t *testing.T) {
	// Happy Path: 不同 workflowID 的 WFT 并发处理，互不串行、状态各自正确。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)

	const n = 16
	runIDs := make([]string, n)
	taskIDs := make([]int64, n)
	for i := range n {
		ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: fmt.Sprintf("wf-distinct-%d", i), Program: programtest.Noop()})
		if err != nil {
			t.Fatal(err)
		}
		runID := fmt.Sprintf("run-distinct-%d", i)
		run := &execution.Run{
			ID:                    runID,
			Namespace:             protocol.DefaultNamespace,
			WorkflowID:            ref.WorkflowID,
			WorkflowType:          ref.WorkflowID,
			WorkflowConfigVersion: ref.Version,
			FirstRunID:            runID,
			Status:                history.StatusRunning,
			IsCurrent:             true,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
		runIDs[i] = runID
		taskIDs[i] = appendOpenWorkflowTaskForTest(t, mgr, runID)
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runIDs[idx], taskIDs[idx]), runIDs[idx], taskIDs[idx], []history.Command{{
				Type:       history.CmdRecordMarker,
				MarkerName: "noop",
				MarkerID:   "noop-marker",
			}}, "")
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("WFT complete for run %d failed: %v", i, e)
		}
	}
	// 每个不同 workflowID 的 run 各自 WFT close 恰好一次、仍 Running。
	for i, runID := range runIDs {
		events := mustEvents(t, mgr, runID)
		if got := countEngineEvents(events, history.EventWorkflowTaskCompleted); got != 1 {
			t.Fatalf("run %d: expected exactly 1 WFT completed event, got %d", i, got)
		}
		r, err := mgr.Get(runID)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != history.StatusRunning {
			t.Fatalf("run %d: expected still running, got %s", i, r.Status)
		}
	}
}
