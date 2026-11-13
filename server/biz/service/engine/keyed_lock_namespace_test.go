package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// 本文件钉死 keyedMu 的锁 key 语义：per-(namespace, workflowID)，而非裸 workflowID。
//
// 两条断言缺一不可：
//   - 跨 namespace 不阻塞：这是本次修复的目的（多租户下同名 workflowID 不再意外串行）。
//   - 同 namespace 仍互斥：防止「改过头」——把 namespace 拼进 key 时若不慎让每次调用产生不同 key，
//     锁就等于没了，且不会有任何编译错误或跨 namespace 测试失败。
//
// 直接对 KeyedLock + workflowLockKey 做单元测试，不启动整个 Server（锁语义与 Server 无关）。

// TestWorkflowLockKey_DifferentNamespacesDoNotBlock 钉死：不同 namespace 下的同名 workflowID
// 能同时持锁（不再互相阻塞）。
func TestWorkflowLockKey_DifferentNamespacesDoNotBlock(t *testing.T) {
	ctx := context.Background()
	k := NewKeyedLock()

	releaseA, err := k.Lock(ctx, workflowLockKey("tenant-a", "order-1"))
	if err != nil {
		t.Fatalf("lock tenant-a: %v", err)
	}

	// 持有 tenant-a/order-1 期间，tenant-b/order-1 必须能立刻拿到锁。
	// 用带超时的 ctx：若两者错误地共用一把锁，这里会等到超时并返回 ctx.Err()。
	acquireCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	releaseB, err := k.Lock(acquireCtx, workflowLockKey("tenant-b", "order-1"))
	if err != nil {
		releaseA()
		t.Fatalf("different namespaces blocked each other (lock key missing namespace): %v", err)
	}

	releaseB()
	releaseA()

	if got := k.EntryCount(); got != 0 {
		t.Fatalf("entries leaked after release: got %d, want 0", got)
	}
}

// TestWorkflowLockKey_SameNamespaceStillMutuallyExclusive 钉死：同 namespace 同 workflowID
// 仍然互斥——防止把 namespace 拼进 key 的过程中把互斥性改没了。
func TestWorkflowLockKey_SameNamespaceStillMutuallyExclusive(t *testing.T) {
	ctx := context.Background()
	k := NewKeyedLock()
	key := workflowLockKey("tenant-a", "order-1")

	releaseFirst, err := k.Lock(ctx, key)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		release, e := k.Lock(ctx, key)
		if e != nil {
			return
		}
		close(acquired)
		release()
	}()

	// 第一把锁未释放期间，第二次 Lock 必须阻塞（给足调度时间确认它没偷跑）。
	select {
	case <-acquired:
		releaseFirst()
		t.Fatal("same (namespace, workflowID) acquired concurrently: mutual exclusion lost")
	case <-time.After(50 * time.Millisecond):
		// 符合预期：被挡住。
	}

	releaseFirst()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second lock did not acquire after release (deadlock?)")
	}
}

// TestWorkflowLockKey_NormalizesEmptyNamespace 钉死：空 namespace 经 NormalizeNamespace 归一到
// DefaultNamespace，与显式 "default" 落到同一把锁——否则 "" 与 "default" 会变成两把锁，
// 把本次修复变成新的竞态。
func TestWorkflowLockKey_NormalizesEmptyNamespace(t *testing.T) {
	if got, want := workflowLockKey("", "order-1"), workflowLockKey(protocol.DefaultNamespace, "order-1"); got != want {
		t.Fatalf("empty namespace not normalized: got %q, want %q", got, want)
	}

	ctx := context.Background()
	k := NewKeyedLock()

	release, err := k.Lock(ctx, workflowLockKey("", "order-1"))
	if err != nil {
		t.Fatalf("lock empty namespace: %v", err)
	}
	defer release()

	// 显式 "default" 必须被同一把锁挡住。
	acquireCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if r, e := k.Lock(acquireCtx, workflowLockKey(protocol.DefaultNamespace, "order-1")); e == nil {
		r()
		t.Fatal(`"" and "default" mapped to different locks: normalization missing from lock key`)
	}
}
