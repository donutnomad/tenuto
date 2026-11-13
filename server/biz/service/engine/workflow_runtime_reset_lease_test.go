package engine

import (
	"context"
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
// ResetWorkflow 迁移到 per-(namespace, workflowID) keyed lock 测试
// ============================================
// 测试策略:
// - 核心价值: reset 持锁期间，同 workflowID 的 signal 被 keyed lock 挡住，等 reset 完成后 signal
//   解析到「新 current」（不是正被 reset 的旧 run）。这正是补 keyed lock 的目的——杜绝
//   「terminate 旧 current → CAS 切新 run」窗口间 signal 解析到旧 current 的漂移竞态。
// - 重入死锁回归: reset 在锁内的事务里终止被取代的 Run，不再拿锁，跑通不卡死（-timeout 短能暴露死锁）。
// - 并发不死锁: reset 与同 workflowID signal 并发跑多轮，signal 永远落在 current（不落终态旧 run）。
// ============================================

func newResetLeaseRuntime(t *testing.T) (*Server, *Server, execution.ExecutionManager) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(server.Stop)
	return server, server, mgr
}

// TestResetWorkflow_HoldsKeyedLock_BlocksConcurrentSignal 钉死「reset 期间持有 per-(namespace, workflowID) 锁，
// 同 namespace 同 workflowID 的 signal 被挡住，直到锁释放后才解析 current」——keyed lock 串行化的核心价值。
//
// 用手动持锁模拟「reset 进行中」：测试持有 (namespace, workflowID) 的 keyed lock，验证 SignalWorkflow(runID 空)
// 在锁释放前无法完成；释放后 signal 解析到 current 并落事件。与真实 reset 同走 server.keyedMu，
// 行为等价（reset 内部正是用同一把锁覆盖 terminate→CAS 全程）。
func TestResetWorkflow_HoldsKeyedLock_BlocksConcurrentSignal(t *testing.T) {
	// 核心价值: 持锁期间 signal 阻塞，释放后解析 current 成功落事件。
	ctx := context.Background()
	svc, server, mgr := newResetLeaseRuntime(t)
	seedRun(t, mgr, "run-cur", "wf-lock", history.StatusRunning, true)

	// 模拟 reset 进行中：手动持有同 (namespace, workflowID) 的 keyed lock。
	// ⚠️ 必须走 workflowLockKey——与被测路径共用同一 key 构造，否则锁不上同一实体，测试形同虚设。
	release, err := server.keyedMu.Lock(ctx, workflowLockKey(protocol.DefaultNamespace, "wf-lock"))
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		// runID 空 → 锁内解析 current。锁被持有时这里必须阻塞。
		_, e := svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-lock", "", "sig", "", nil)
		done <- e
	}()

	// 锁未释放期间，signal 必须仍阻塞（给足调度时间确认它没偷跑完成）。
	select {
	case e := <-done:
		release()
		t.Fatalf("signal completed while keyed lock held (lock not serializing): err=%v", e)
	case <-time.After(50 * time.Millisecond):
		// 符合预期：signal 被锁挡住。
	}

	// 释放锁 → signal 应解锁完成。
	release()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("signal after lock release failed: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signal did not complete after lock release (deadlock?)")
	}

	if got := countSignaledEvents(mustEvents(t, mgr, "run-cur")); got != 1 {
		t.Fatalf("expected 1 signal on current run after lock release, got %d", got)
	}
}

// TestResetWorkflow_ConcurrentSignal_FollowsNewCurrent 钉死「reset 与同 workflowID signal 并发时，
// signal 永远落在 current run（reset 后的新代或 reset 前的旧代），绝不落在被 reset 终止的旧 run 之外的
// 已终态代」。keyed lock 保证 signal 的「解析 current → 落事件」与 reset 的「terminate→CAS 切代」
// 不交错，无漂移：signal 要么在 reset 前抢到锁落到旧 current，要么在 reset 后抢到锁落到新 current。
//
// 同时是重入死锁回归：reset 在锁内的事务里终止被取代的 Run，不再拿锁；若终止时再拿同一把锁会在此死锁，
// -timeout 短即可暴露。
func TestResetWorkflow_ConcurrentSignal_FollowsNewCurrent(t *testing.T) {
	// 核心价值 + 重入死锁回归: reset 与 signal 并发，signal 落在某个 current（不漂移），全程不死锁。
	ctx := context.Background()
	svc, server, mgr := newResetLeaseRuntime(t)

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-reset-conc", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	gen1, err := startedRunID(svc.StartWorkflow(ctx, "", "wf-reset-conc", "wf-reset-conc", "", map[string]any{"k": "v"}))
	if err != nil {
		t.Fatal(err)
	}
	// WFT close 边界作为 reset 截断点。
	completeOpenWorkflowTask(t, mgr, gen1)

	// 并发：reset(gen1) 与 signal(runID 空, 按 workflowID 解析 current) 同时跑。
	resetDone := make(chan struct{})
	var gen2 string
	var resetErr error
	go func() {
		gen2, resetErr = svc.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
			RunID:     gen1,
			Reason:    "concurrent-reset",
			ResetType: execution.ResetTypeEnums.LastWorkflowTask,
		})
		close(resetDone)
	}()

	sigDone := make(chan error, 1)
	go func() {
		_, e := svc.SignalWorkflow(ctx, protocol.DefaultNamespace, "wf-reset-conc", "", "concurrent-sig", "", nil)
		sigDone <- e
	}()

	// 全程不得死锁（-timeout 短能暴露重入死锁）。
	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not complete (deadlock? reentrant keyed lock?)")
	}
	if resetErr != nil {
		t.Fatalf("concurrent reset failed: %v", resetErr)
	}

	var sigErr error
	select {
	case sigErr = <-sigDone:
	case <-time.After(5 * time.Second):
		t.Fatal("signal did not complete (deadlock?)")
	}

	// keyed lock 串行化 signal 的「解析 current → 落事件」与 reset 的「GetEvents→fork→terminate→CAS」，
	// 故无漂移、无交错。两条合法路径（keyed lock 二选一，绝无第三种瞬态）：
	//   A) signal 在 reset 抢锁前完成 → 落到旧 current gen1。reset 随后 GetEvents 读到该 signal，
	//      reapplyEvents 忠实搬到 gen2（设计语义：reset 把 base 历史里的 signal 重放到新代）。
	//      ⇒ gen1=1, gen2∈{0,1}（取决于 signal 是否在 reset 读 events 之前落，二者皆 reset 内部确定，不竞态）。
	//   B) signal 在 reset 完成后抢锁 → 解析到新 current gen2，主写落 gen2。⇒ gen1=0, gen2=1。
	// 关键反漂移不变量：gen1（被 reset 终止的旧 run）绝不会在「终止之后」收到 signal——若 signal 落在
	// gen1，必发生在 reset 终止它之前（路径 A）。每个 run 至多 1 个 signal（无重复/无撕裂写）。
	gen1Sigs := countSignaledEvents(mustEvents(t, mgr, gen1))
	gen2Sigs := countSignaledEvents(mustEvents(t, mgr, gen2))

	cur, err := mgr.GetCurrentRunID("", "wf-reset-conc")
	if err != nil {
		t.Fatalf("GetCurrentRunID: %v", err)
	}
	if cur != gen2 {
		t.Fatalf("after reset, current must be gen2(%s), got %s", gen2, cur)
	}

	if gen1Sigs > 1 || gen2Sigs > 1 {
		t.Fatalf("torn/duplicate signal write (gen1=%d gen2=%d), keyed lock failed to serialize", gen1Sigs, gen2Sigs)
	}
	if sigErr == nil {
		// signal 成功：必命中两条合法路径之一——A) 落 gen1（可被 reapply 到 gen2）；B) 仅落 gen2。
		pathA := gen1Sigs == 1
		pathB := gen1Sigs == 0 && gen2Sigs == 1
		if !pathA && !pathB {
			t.Fatalf("signal drifted off current (gen1=%d gen2=%d): must hit pre-reset gen1 or post-reset gen2", gen1Sigs, gen2Sigs)
		}
	} else {
		// signal 失败也允许（如 reset 后抢锁解析到刚切代的瞬态错误）——但绝不能在错误的 run 上落事件。
		if gen1Sigs+gen2Sigs != 0 {
			t.Fatalf("signal errored but still wrote an event (gen1=%d gen2=%d): %v", gen1Sigs, gen2Sigs, sigErr)
		}
	}
}
