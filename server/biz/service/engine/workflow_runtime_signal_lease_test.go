package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// SignalWorkflow 迁移到 withWorkflowLease 测试
// ============================================
// 测试策略:
// - Happy Path: 传具体 runID（老路径）→ signal 落到该 run，行为完全不变
// - Happy Path: runID 空 + workflowID → 解析 current run，signal 落到 current
// - 核心价值: 切代（CAN/retry）后按 workflowID signal 自动跟随新 current run（不是旧 run）
// - 并发: N goroutine 同 workflowID 并发 signal，事件不丢不乱（per-workflow 锁串行化，-race）
// - Edge Case: current 是终态 run → signal 解析到它并拒绝（不报"无 current"）
// ============================================

func newSignalRuntime(t *testing.T) (*Server, execution.ExecutionManager) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)
	return server, mgr
}

func TestSignalWorkflow_RunIDPath_BehaviorUnchanged(t *testing.T) {
	// Happy Path: 传具体 runID（老路径），namespace/workflowID 留空 → signal 落到该 run，行为不变
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	seedRun(t, mgr, "run-1", "wf-a", history.StatusRunning, true)

	eventID, err := svc.SignalWorkflow(ctx, "", "", "run-1", "payment", "", map[string]any{"amount": int64(7)})
	if err != nil {
		t.Fatal(err)
	}
	if eventID == 0 {
		t.Fatal("expected non-zero signal event id")
	}
	events, _ := mgr.GetEvents("run-1")
	if countSignaledEvents(events) != 1 {
		t.Fatalf("expected 1 signal event on run-1, got %+v", events)
	}
}

func TestSignalWorkflow_RunIDEmpty_ResolvesCurrent(t *testing.T) {
	// Happy Path: runID 空 + workflowID → 锁内解析 current，signal 落到 current run
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	// 旧代非 current，新代 current —— signal 必须落到 current
	seedRun(t, mgr, "run-old", "wf-a", history.StatusContinuedAsNew, false)
	seedRun(t, mgr, "run-current", "wf-a", history.StatusRunning, true)

	eventID, err := svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-a", "", "payment", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if eventID == 0 {
		t.Fatal("expected non-zero signal event id")
	}
	if got := countSignaledEvents(mustEvents(t, mgr, "run-current")); got != 1 {
		t.Fatalf("expected signal on current run, got %d on run-current", got)
	}
	if got := countSignaledEvents(mustEvents(t, mgr, "run-old")); got != 0 {
		t.Fatalf("signal must NOT land on old run, got %d on run-old", got)
	}
}

func TestSignalWorkflow_AfterContinueAsNew_FollowsNewCurrent(t *testing.T) {
	// 核心价值: 切代（CAN/retry）后按 workflowID signal 自动跟随新 current run。
	// 模拟：gen1 是 current → 经真 CAS 切代到 gen2 → 旧 runID 已失效；
	// 按 workflowID（runID 空）signal 必须落到 gen2（新 current），不是 gen1。
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	seedRun(t, mgr, "gen1", "wf-can", history.StatusRunning, true)
	seedRun(t, mgr, "gen2", "wf-can", history.StatusRunning, false)

	// 用真 SetCurrentRunCAS 把 current 从 gen1 切到 gen2（对标 CAN/retry 开新代）。
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		ok, err := tx.SetCurrentRunCAS(protocol.DefaultNamespace, "wf-can", "gen1", "gen2")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatal("CAS gen1→gen2 should succeed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 切代后，按 workflowID signal（runID 空）→ 必须跟随到新 current gen2。
	if _, err := svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-can", "", "follow", "", nil); err != nil {
		t.Fatal(err)
	}
	if got := countSignaledEvents(mustEvents(t, mgr, "gen2")); got != 1 {
		t.Fatalf("signal must follow new current gen2, got %d on gen2", got)
	}
	if got := countSignaledEvents(mustEvents(t, mgr, "gen1")); got != 0 {
		t.Fatalf("signal must NOT land on stale gen1, got %d on gen1", got)
	}
}

func TestSignalWorkflow_ConcurrentSameWorkflow_Serialized(t *testing.T) {
	// 并发: N goroutine 同 workflowID 并发 signal（runID 空，每次解析 current）。
	// per-workflow 锁保证串行化，事件不丢不乱（事件 ID 唯一、计数等于 N）。用 -race 跑。
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	seedRun(t, mgr, "run-c", "wf-c", history.StatusRunning, true)

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-c", "", "sig", "", map[string]any{"i": int64(idx)})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent signal %d failed: %v", i, err)
		}
	}
	events := mustEvents(t, mgr, "run-c")
	if got := countSignaledEvents(events); got != n {
		t.Fatalf("expected %d signal events (none lost), got %d", n, got)
	}
	// 事件 ID 不得重复（per-workflow 锁 + 原子事务保证不乱）。
	seen := make(map[int64]bool, len(events))
	for _, e := range events {
		if e.WorkflowSignaled == nil {
			continue
		}
		if seen[e.ID] {
			t.Fatalf("duplicate signal event id %d (events interleaved/corrupted)", e.ID)
		}
		seen[e.ID] = true
	}
}

func TestSignalWorkflow_TerminalCurrent_Rejected(t *testing.T) {
	// Edge Case: current 指向终态 run（CAN 后 current 不清，仍指向它）。
	// 按 workflowID signal 必须解析到它并拒绝（返回终态错误），而不是报"无 current"。
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	seedRun(t, mgr, "run-done", "wf-done", history.StatusCompleted, true)

	_, err := svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-done", "", "late", "", nil)
	if err == nil {
		t.Fatal("expected error signaling terminal current run, got nil")
	}
	// 不能是"无 current"——必须是解析到终态 run 后的拒绝。
	if errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("must resolve terminal current and reject, not report no-current: %v", err)
	}
	if got := countSignaledEvents(mustEvents(t, mgr, "run-done")); got != 0 {
		t.Fatalf("terminal run must not receive signal, got %d", got)
	}
}

func TestSignalWorkflow_RunIDPath_TerminalRejected_Unchanged(t *testing.T) {
	// Error Case: 老路径传具体终态 runID → 仍按原语义拒绝（行为不变）。
	ctx := context.Background()
	svc, mgr := newSignalRuntime(t)
	seedRun(t, mgr, "run-term", "wf-t", history.StatusTerminated, true)

	_, err := svc.SignalWorkflow(ctx, "", "", "run-term", "x", "", nil)
	if err == nil {
		t.Fatal("expected error signaling terminal run via runID path")
	}
}
