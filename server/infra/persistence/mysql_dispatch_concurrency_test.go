package persistence

import (
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// TestAcquireTaskDispatchConcurrentNoDeadlock 覆盖缺陷 3。
//
// 原实现是"SELECT ... FOR UPDATE 探测 → 没有则 INSERT"。FOR UPDATE 打在**不存在的行**上
// 会取 supremum 上的 X 型间隙锁，紧接着的 INSERT 需要同一间隙的 insert intention 锁，
// 二者互斥 → 多事务互等 → Error 1213 Deadlock found。
// 实测 16 路并发抢同一 task：ok=2 fail=14。
// 且 AcquireTaskDispatch 无重试包装，retryOnDuplicatedKey 只认 ErrDuplicatedKey，抓不到 1213。
//
// 现改为单条 INSERT ... ON DUPLICATE KEY UPDATE 条件抢占，"先锁空行再插"的模式被消除。
//
// 本用例断言两件事（缺一不可）：
//  1. **零死锁**——任何 1213/1205 都直接判错，而不是重试掩盖；
//  2. **语义正确**——恰好一个 winner，其余全部 acquired=false 且不报错。
//     只测"不死锁"是不够的：把函数改成无脑 upsert 也不会死锁，但会让所有人都拿到租约，
//     去重保证就被悄悄弱化了。
func TestAcquireTaskDispatchConcurrentNoDeadlock(t *testing.T) {
	store := requireMySQL(t)

	const workers = 16
	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-dispatch-race", ScheduledEventID: 99,
	}

	var wg sync.WaitGroup
	leases := make([]dispatch.Lease, workers)
	oks := make([]bool, workers)
	errs := make([]error, workers)

	start := make(chan struct{})
	for i := range workers {
		wg.Go(func() {
			<-start // 尽量让 16 个 goroutine 同时冲进去，最大化间隙锁交错概率
			leases[i], oks[i], errs[i] = store.Dispatches.AcquireTaskDispatch(task, time.Minute)
		})
	}
	close(start)
	wg.Wait()

	winners := 0
	var winnerLease string
	for i, err := range errs {
		if isDeadlockErr(err) {
			t.Errorf("worker %d 撞上 InnoDB 死锁/锁等待超时（缺陷 3 回归）: %v", i, err)
			continue
		}
		if err != nil {
			t.Errorf("worker %d AcquireTaskDispatch 报错: %v", i, err)
			continue
		}
		if oks[i] {
			winners++
			winnerLease = leases[i].LeaseID
			if leases[i].LeaseID == "" {
				t.Errorf("worker %d 抢到了租约但 LeaseID 为空", i)
			}
		}
	}

	if winners != 1 {
		t.Fatalf("winner 数 = %d, want 恰好 1——去重/幂等保证被破坏", winners)
	}

	// 落库状态必须与那个唯一 winner 一致。
	state, found, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil || !found {
		t.Fatalf("GetTaskDispatch: found=%v err=%v", found, err)
	}
	if state.LeaseID != winnerLease {
		t.Fatalf("落库 lease_id = %q, want winner 的 %q", state.LeaseID, winnerLease)
	}
	if state.Status != dispatch.StatusLeased {
		t.Fatalf("落库 status = %q, want %q", state.Status, dispatch.StatusLeased)
	}
	// 只有第一次抢占成功，attempts 不应被 16 路并发累加。
	if state.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1——并发抢占不应重复累加", state.Attempts)
	}
}

// TestAcquireTaskDispatchPreservesRejectionSemantics 逐条锁定 upsert 改写后
// **三个拒绝分支**的语义未被弱化。这是"不能只测不死锁"的正面覆盖：
//  1. 已 Dispatched → 拒绝；
//  2. Leased 且租约未过期 → 拒绝；
//  3. Leased 但租约已过期 → 允许续租，且 attempts 递增。
func TestAcquireTaskDispatchPreservesRejectionSemantics(t *testing.T) {
	store := requireMySQL(t)

	t.Run("dispatched 后拒绝", func(t *testing.T) {
		task := protocol.Task{
			Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
			RunID: "run-reject-dispatched", ScheduledEventID: 1,
		}
		lease, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
		if err != nil || !ok {
			t.Fatalf("首次抢占: ok=%v err=%v", ok, err)
		}
		if err := store.Dispatches.MarkTaskDispatchDispatched(task, lease.LeaseID); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute); err != nil || ok {
			t.Fatalf("已 dispatched 仍被抢占: ok=%v err=%v", ok, err)
		}
		state, _, _ := store.Dispatches.GetTaskDispatch(task)
		if state.Status != dispatch.StatusDispatched {
			t.Fatalf("status 被拒绝路径改写成了 %q, want 保持 dispatched", state.Status)
		}
		if state.LeaseID != lease.LeaseID {
			t.Fatalf("拒绝路径不应改写 lease_id: got %q want %q", state.LeaseID, lease.LeaseID)
		}
	})

	t.Run("租约未过期时拒绝", func(t *testing.T) {
		task := protocol.Task{
			Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
			RunID: "run-reject-leased", ScheduledEventID: 2,
		}
		lease, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Hour)
		if err != nil || !ok {
			t.Fatalf("首次抢占: ok=%v err=%v", ok, err)
		}
		if _, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Hour); err != nil || ok {
			t.Fatalf("租约未过期仍被抢占: ok=%v err=%v", ok, err)
		}
		state, _, _ := store.Dispatches.GetTaskDispatch(task)
		if state.LeaseID != lease.LeaseID {
			t.Fatalf("拒绝路径改写了 lease_id: got %q want %q", state.LeaseID, lease.LeaseID)
		}
		if state.Attempts != 1 {
			t.Fatalf("拒绝路径不应递增 attempts: got %d want 1", state.Attempts)
		}
	})

	t.Run("租约过期后允许续租", func(t *testing.T) {
		task := protocol.Task{
			Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
			RunID: "run-renew-expired", ScheduledEventID: 3,
		}
		// 负 TTL 让租约立刻处于已过期状态，无需 sleep。
		first, ok, err := store.Dispatches.AcquireTaskDispatch(task, -time.Second)
		if err != nil || !ok {
			t.Fatalf("首次抢占: ok=%v err=%v", ok, err)
		}
		second, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
		if err != nil || !ok {
			t.Fatalf("租约已过期却未能续租: ok=%v err=%v", ok, err)
		}
		if second.LeaseID == first.LeaseID {
			t.Fatal("续租应产生新的 LeaseID")
		}
		state, _, _ := store.Dispatches.GetTaskDispatch(task)
		if state.Attempts != 2 {
			t.Fatalf("续租后 attempts = %d, want 2", state.Attempts)
		}
		if state.LeaseID != second.LeaseID {
			t.Fatalf("落库 lease_id = %q, want %q", state.LeaseID, second.LeaseID)
		}
	})
}
