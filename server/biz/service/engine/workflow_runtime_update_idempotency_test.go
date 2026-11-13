package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// updateID 幂等测试
// ============================================
// 测试策略（对标方案2：lease 前预取快照 + AppendIdempotent DedupeKey 兜底）:
// - Happy Path: 同 updateID 二次命中 completed → 返回既有 Output，历史不新增事件
// - Happy Path: 同 updateID 二次命中 accepted（未完成）→ 不产生第二条 accepted
// - Happy Path: client 传 updateID → 历史里就是这个 ID（非内部 uuid）
// - Edge Case (-race): 同 updateID 并发 N 次 → 历史恰好一条 accepted（keyed lock 串行 + DedupeKey 兜底）
// ============================================

// countUpdateEvents 统计历史里指定 updateID 的 accepted / completed 事件条数。
func countUpdateEvents(t *testing.T, env *updateWaitTestEnv, updateID string) (accepted, completed int) {
	t.Helper()
	events, err := env.server.GetEvents(env.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.WorkflowUpdateAccepted != nil && ev.WorkflowUpdateAccepted.UpdateID == updateID {
			accepted++
		}
		if ev.WorkflowUpdateCompleted != nil && ev.WorkflowUpdateCompleted.UpdateID == updateID {
			completed++
		}
	}
	return accepted, completed
}

// TestUpdateWorkflow_SameUpdateID_Completed_ReturnsCached
// Happy Path: 同 updateID 二次命中已 completed 的 update，直接返回既有 Output（幂等命中），
// 不重复 Append —— 历史里恰好一条 accepted + 一条 completed。
func TestUpdateWorkflow_SameUpdateID_Completed_ReturnsCached(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-idem-completed")
	ctx := context.Background()
	const updateID = "up-idem-completed"

	// 第一次：driver 完成 WFT 落 completed，拿到 Output。
	driverDone := env.driveUpdateComplete(t, ctx, map[string]any{"result": "first"})
	first, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	<-driverDone
	if first.Stage != protocol.UpdateReachedStageEnums.Completed || updateOutput(first)["result"] != "first" {
		t.Fatalf("first call not completed: %+v", first)
	}

	acc, comp := countUpdateEvents(t, env, updateID)
	if acc != 1 || comp != 1 {
		t.Fatalf("after first: want 1 accepted + 1 completed, got accepted=%d completed=%d", acc, comp)
	}

	// 第二次：同 updateID，命中快照的 completed，直接回既有 Output，不新增事件。
	second, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("second call must return cached Completed, got %q", second.Stage)
	}
	if second.Outcome == nil || second.Outcome.Success == nil || second.Outcome.Failure != nil {
		t.Fatalf("second call must return cached outcome, got %+v", second)
	}
	var cached map[string]any
	if err := second.Outcome.Success.Decode(&cached); err != nil || cached["result"] != "first" {
		t.Fatalf("second call must return cached output, got %+v (err=%v)", cached, err)
	}

	acc, comp = countUpdateEvents(t, env, updateID)
	if acc != 1 || comp != 1 {
		t.Fatalf("after second: history must be unchanged (1 accepted + 1 completed), got accepted=%d completed=%d", acc, comp)
	}
}

// TestUpdateWorkflow_SameUpdateID_Accepted_NoDoubleAppend
// Happy Path: update 只 accepted 未完成时，同 updateID 二次请求不产生第二条 accepted。
func TestUpdateWorkflow_SameUpdateID_Accepted_NoDoubleAppend(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-idem-accepted")
	ctx := context.Background()
	const updateID = "up-idem-accepted"

	// 第一次：accept-only（不驱动完成），只落 accepted。
	first, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if first.Stage != protocol.UpdateReachedStageEnums.Accepted || first.AcceptedEventID == 0 {
		t.Fatalf("first call not accepted: %+v", first)
	}

	acc, comp := countUpdateEvents(t, env, updateID)
	if acc != 1 || comp != 0 {
		t.Fatalf("after first: want 1 accepted + 0 completed, got accepted=%d completed=%d", acc, comp)
	}

	// 第二次：同 updateID，命中快照的 accepted，不重复 Append。AcceptedEventID 与首次一致。
	second, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("second call must be Accepted, got %q", second.Stage)
	}
	if second.AcceptedEventID != first.AcceptedEventID {
		t.Fatalf("second AcceptedEventID %d != first %d (must reuse existing accepted)", second.AcceptedEventID, first.AcceptedEventID)
	}

	acc, comp = countUpdateEvents(t, env, updateID)
	if acc != 1 || comp != 0 {
		t.Fatalf("after second: must not double-append accepted, got accepted=%d completed=%d", acc, comp)
	}
}

// TestUpdateWorkflow_ClientProvidedUpdateID_Honored
// Happy Path: client 传入的 updateID 生效，历史里落的就是这个 ID（不是内部生成的 uuid）。
func TestUpdateWorkflow_ClientProvidedUpdateID_Honored(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-client-updateid")
	ctx := context.Background()
	const clientUpdateID = "client-supplied-42"

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", clientUpdateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.UpdateID != clientUpdateID {
		t.Fatalf("result.UpdateID = %q, want client-supplied %q", result.UpdateID, clientUpdateID)
	}

	acc, _ := countUpdateEvents(t, env, clientUpdateID)
	if acc != 1 {
		t.Fatalf("history must contain exactly one accepted with client updateID, got %d", acc)
	}
}

// TestUpdateWorkflow_EmptyUpdateID_GeneratesUnique
// Happy Path: updateID 空 = 原行为，内部生成非空唯一 uuid（加法不改法：空则老语义）。
func TestUpdateWorkflow_EmptyUpdateID_GeneratesUnique(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-empty-updateid")
	ctx := context.Background()

	r1, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", "", protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "low"}))
	if err != nil {
		t.Fatal(err)
	}
	if r1.UpdateID == "" || r2.UpdateID == "" {
		t.Fatalf("empty updateID must be generated: r1=%q r2=%q", r1.UpdateID, r2.UpdateID)
	}
	if r1.UpdateID == r2.UpdateID {
		t.Fatalf("two empty-updateID calls must generate distinct ids, both %q", r1.UpdateID)
	}
}

// TestUpdateIdempotency_ConcurrentDuplicateSingleAccepted
// Edge Case (-race): 同 updateID 并发 N 次，最终历史恰好一条 accepted。
// 验证 keyed lock（per-workflowID 串行）+ AppendIdempotent DedupeKey（存储层 first-wins）双保险：
// 即使快照检查在并发窗口漏网，DedupeKey 也兜住，绝不产生第二条 accepted。
func TestUpdateIdempotency_ConcurrentDuplicateSingleAccepted(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-idem-concurrent")
	ctx := context.Background()
	const updateID = "up-idem-concurrent"
	const n = 20

	var wg sync.WaitGroup
	var okCount atomic.Int64
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 尽量同时起跑，制造并发窗口
			result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
			if err != nil {
				t.Errorf("concurrent update failed: %v", err)
				return
			}
			if result.UpdateID != updateID {
				t.Errorf("updateID mismatch: %q", result.UpdateID)
				return
			}
			okCount.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if okCount.Load() != n {
		t.Fatalf("expected all %d calls to succeed, got %d", n, okCount.Load())
	}

	// 核心断言：历史里恰好一条 accepted。
	acc, comp := countUpdateEvents(t, env, updateID)
	if acc != 1 {
		t.Fatalf("concurrent same updateID must produce exactly ONE accepted, got %d (completed=%d)", acc, comp)
	}
}

// TestUpdateIdempotency_SnapshotHitAvoidsAppend
// Edge Case: 预置历史里的 accepted（模拟已存在），后续同 updateID 请求走快照命中路径，
// 不新增事件——直接验证 lease 前快照预取的短路作用（不依赖 driver 时序）。
func TestUpdateIdempotency_SnapshotHitAvoidsAppend(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-idem-snapshot")
	ctx := context.Background()
	const updateID = "up-snapshot"

	// 预置一条 accepted（不经 UpdateWorkflow，直接进历史）。
	if _, err := env.server.mgr.Append(env.run.ID, env.run.ID, env.run.ID,
		history.NewWorkflowUpdateAcceptedEvent("set_priority", updateID, updateArgs(map[string]any{"priority": "high"}))); err != nil {
		t.Fatal(err)
	}

	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted {
		t.Fatalf("snapshot hit must return Accepted, got %q", result.Stage)
	}

	acc, _ := countUpdateEvents(t, env, updateID)
	if acc != 1 {
		t.Fatalf("snapshot hit must not append second accepted, got %d", acc)
	}
}
