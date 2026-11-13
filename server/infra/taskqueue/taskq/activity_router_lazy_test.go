package taskq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// fakeCloser 记录 Close 是否被调用，用于断言 GC / router.Close 回收了 lazy 条目。
type fakeCloser struct {
	closed atomic.Bool
}

func (c *fakeCloser) Close() { c.closed.Store(true) }

// TestLazyActivityRouterDialsUnconfiguredQueueEndToEnd 对齐 Temporal 隐式动态语义：
// poll 一个未在 TASKQ_ACTIVITY_QUEUES_JSON 配置的 route，router 应 lazy-dial 出一个
// queue，enqueue→dequeue→ack 全链路成功，且物理名走 activity: 前缀约定。
func TestLazyActivityRouterDialsUnconfiguredQueueEndToEnd(t *testing.T) {
	client := &routingClientFake{}
	var dialed []string
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, physical string) (*Queue, interface{ Close() }, error) {
		dialed = append(dialed, physical)
		return NewQueue(client), &fakeCloser{}, nil
	})
	defer router.Close()

	ctx := context.Background()
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-1", TaskQueue: "brand-new-queue"}
	if err := router.Enqueue(ctx, "brand-new-queue", task); err != nil {
		t.Fatalf("enqueue unconfigured queue: %v", err)
	}
	lease, err := router.Dequeue(activityWorkerPollContext("tenant-a", "worker-a", ""), "brand-new-queue")
	if err != nil {
		t.Fatalf("dequeue unconfigured queue: %v", err)
	}
	if lease.Task.RunID != "run-1" || lease.Task.TaskQueue != "brand-new-queue" {
		t.Fatalf("unexpected lease: %+v", lease.Task)
	}
	if err := router.Ack(ctx, lease); err != nil {
		t.Fatalf("ack: %v", err)
	}
	wantPhysical := activityPhysicalQueuePrefix + dispatch.ActivityTaskQueueKey("tenant-a", "brand-new-queue")
	if len(dialed) != 1 || dialed[0] != wantPhysical {
		t.Fatalf("dialed = %v, want single %q", dialed, wantPhysical)
	}
}

// TestLazyActivityRouterHonorsPhysicalOverride 验证显式配置的 route 走配置的物理名，
// 不加 activity: 前缀。
func TestLazyActivityRouterHonorsPhysicalOverride(t *testing.T) {
	client := &routingClientFake{}
	var dialed string
	overrides := map[string]string{dispatch.ActivityTaskQueueKey("default", "payments"): "activity-legacy-payments"}
	router := newLazyActivityQueueRouter(overrides, func(_ context.Context, physical string) (*Queue, interface{ Close() }, error) {
		dialed = physical
		return NewQueue(client), &fakeCloser{}, nil
	})
	defer router.Close()

	if err := router.Enqueue(context.Background(), "payments", protocol.Task{Type: protocol.TaskTypeActivity, RunID: "r", TaskQueue: "payments"}); err != nil {
		t.Fatal(err)
	}
	if dialed != "activity-legacy-payments" {
		t.Fatalf("dialed physical = %q, want legacy override name", dialed)
	}
}

// TestLazyActivityRouterCachesDialedQueue 验证第二次访问命中缓存，不重复 dial。
func TestLazyActivityRouterCachesDialedQueue(t *testing.T) {
	client := &routingClientFake{}
	var dialCount int
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		dialCount++
		return NewQueue(client), &fakeCloser{}, nil
	})
	defer router.Close()

	ctx := context.Background()
	for range 3 {
		_, release, err := router.queue(ctx, "default", "q")
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if dialCount != 1 {
		t.Fatalf("dial count = %d, want 1 (cached)", dialCount)
	}
}

// TestLazyActivityRouterDialFailurePropagates 验证 dial 真失败返回真实错误，而不是
// 旧的 "... is not configured" 白名单错误。
func TestLazyActivityRouterDialFailurePropagates(t *testing.T) {
	sentinel := errors.New("frontend boom")
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		return nil, nil, sentinel
	})
	defer router.Close()

	_, _, err := router.queue(context.Background(), "default", "q")
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want wrapping %v", err, sentinel)
	}
}

// TestLazyActivityRouterGCReclaimsIdleFrontend 验证空闲 GC 确实回收了空闲 frontend：
// 用未来时间戳驱动 collectIdle，超过 TTL 的 lazy 条目被 Close 并移出缓存。
func TestLazyActivityRouterGCReclaimsIdleFrontend(t *testing.T) {
	closer := &fakeCloser{}
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		return NewQueue(&routingClientFake{}), closer, nil
	})
	defer router.Close()

	ctx := context.Background()
	// 用完即归还：引用计数归零后条目才具备被回收的资格（在途持有者不回收，另见
	// TestLazyActivityRouterGCSkipsEntryHeldByCaller）。
	_, release, err := router.queue(ctx, "default", "idle-q")
	if err != nil {
		t.Fatal(err)
	}
	release()

	// 未到 TTL：不回收。
	router.collectIdle(time.Now())
	if closer.closed.Load() {
		t.Fatal("frontend reclaimed before idle TTL elapsed")
	}
	router.mu.Lock()
	entries := len(router.entries)
	router.mu.Unlock()
	if entries != 1 {
		t.Fatalf("entries = %d before TTL, want 1", entries)
	}

	// 超过 TTL：回收并 Close。
	router.collectIdle(time.Now().Add(activityFrontendIdleTTL + time.Second))
	if !closer.closed.Load() {
		t.Fatal("idle frontend was not Closed by GC")
	}
	router.mu.Lock()
	entries = len(router.entries)
	router.mu.Unlock()
	if entries != 0 {
		t.Fatalf("entries = %d after GC, want 0 (cache cleared)", entries)
	}
}

// TestLazyActivityRouterGCSkipsRecentlyActive 验证并发安全语义的关键不变量：刚被访问
// 刷新过 lastActive 的条目不会被同一轮 GC 回收。
func TestLazyActivityRouterGCSkipsRecentlyActive(t *testing.T) {
	closer := &fakeCloser{}
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		return NewQueue(&routingClientFake{}), closer, nil
	})
	defer router.Close()

	ctx := context.Background()
	_, release, err := router.queue(ctx, "default", "hot-q")
	if err != nil {
		t.Fatal(err)
	}
	release()
	// 命中刷新 lastActive 到 now；即便 GC 用「now 之后但不足 TTL」的时刻扫描也不回收。
	// 归还后再扫描，确保这里测的是 lastActive 时间语义，而非引用计数把条目钉住。
	_, release, err = router.queue(ctx, "default", "hot-q")
	if err != nil {
		t.Fatal(err)
	}
	release()
	router.collectIdle(time.Now().Add(activityFrontendIdleTTL - time.Second))
	if closer.closed.Load() {
		t.Fatal("recently-active frontend was reclaimed")
	}
}

// TestLazyActivityRouterGCSkipsEntryHeldByCaller 咬住 use-after-close 窗口：调用方已从
// queue() 拿到 *Queue、尚未使用时，GC 恰好扫到这条已过 TTL 的条目。此时绝不能 Close——
// 否则调用方接下来会打在一个已关闭的 frontend 上。
//
// 修复前（queue() 不计引用、collectIdle 摘除即 Close）此测试 FAIL；修复后 PASS。
func TestLazyActivityRouterGCSkipsEntryHeldByCaller(t *testing.T) {
	closer := &fakeCloser{}
	client := &routingClientFake{}
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		return NewQueue(client), closer, nil
	})
	defer router.Close()

	ctx := context.Background()
	// 调用方拿到 queue 指针（模拟 Enqueue/Ack 里 queue() 已返回、真正使用还没发生的那一刻）。
	queue, release, err := router.queue(ctx, "default", "held-q")
	if err != nil {
		t.Fatal(err)
	}

	// GC 此刻扫描：该条目 lastActive 已过 TTL，但有在途持有者。
	router.collectIdle(time.Now().Add(2 * activityFrontendIdleTTL))

	if closer.closed.Load() {
		t.Fatal("GC closed the frontend while a caller still held the *Queue (use-after-close window)")
	}
	// 持有者现在真正使用它：必须成功，说明打在一个活着的 frontend 上。
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "default", RunID: "run-held", TaskQueue: "held-q"}
	if err := queue.enqueueWithRoutingKey(ctx, task, activityRoutingKey("default", "held-q")); err != nil {
		t.Fatalf("enqueue on caller-held queue: %v", err)
	}

	// 归还后条目重新具备回收资格，下一轮 GC 正常收割（GC 未因引用计数而永久漏收）。
	release()
	router.collectIdle(time.Now().Add(2 * activityFrontendIdleTTL))
	if !closer.closed.Load() {
		t.Fatal("idle frontend was not reclaimed after the caller released it")
	}
}

// TestLazyActivityRouterCloseDefersFrontendHeldByCaller 验证同一不变量在关停路径上也成立：
// Close() 不能关掉在途调用方持有的 frontend，实际 Close 推迟到最后一个持有者归还时。
func TestLazyActivityRouterCloseDefersFrontendHeldByCaller(t *testing.T) {
	closer := &fakeCloser{}
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		return NewQueue(&routingClientFake{}), closer, nil
	})

	_, release, err := router.queue(context.Background(), "default", "held-q")
	if err != nil {
		t.Fatal(err)
	}

	router.Close()
	if closer.closed.Load() {
		t.Fatal("Close() closed a frontend still held by a caller")
	}

	release()
	if !closer.closed.Load() {
		t.Fatal("frontend not closed after the last holder released it")
	}
}

// TestLazyActivityRouterCloseReclaimsAll 验证 Close 停 GC 并 Close 所有 lazy frontend。
func TestLazyActivityRouterCloseReclaimsAll(t *testing.T) {
	var closers []*fakeCloser
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		c := &fakeCloser{}
		closers = append(closers, c)
		return NewQueue(&routingClientFake{}), c, nil
	})

	ctx := context.Background()
	for _, name := range []string{"a", "b", "c"} {
		_, release, err := router.queue(ctx, "default", name)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	router.Close()
	for i, c := range closers {
		if !c.closed.Load() {
			t.Fatalf("closer %d not closed after router.Close", i)
		}
	}
	router.mu.Lock()
	entries := len(router.entries)
	router.mu.Unlock()
	if entries != 0 {
		t.Fatalf("entries = %d after Close, want 0", entries)
	}
	// 可重复调用不 panic。
	router.Close()
}

// Error Case: Close() 之后 miss 的 route 返回错误，Router 不再拨号。
func TestLazyActivityRouterRefusesDialAfterClose(t *testing.T) {
	var dialCount atomic.Int32
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		dialCount.Add(1)
		return NewQueue(&routingClientFake{}), &fakeCloser{}, nil
	})
	router.Close()

	err := router.Enqueue(context.Background(), "late-queue", protocol.Task{Type: protocol.TaskTypeActivity, RunID: "r", TaskQueue: "late-queue"})
	if !errors.Is(err, errActivityRouterClosed) {
		t.Fatalf("enqueue after close: err = %v, want errActivityRouterClosed", err)
	}
	if dialCount.Load() != 0 {
		t.Fatalf("router dialed %d frontends after close", dialCount.Load())
	}
}

// Edge Case: 拨号进行中 Router 被 Close，拨出的 frontend 被当场关闭，不进入缓存。
func TestLazyActivityRouterClosesFrontendDialedDuringClose(t *testing.T) {
	dialing := make(chan struct{})
	proceed := make(chan struct{})
	closer := &fakeCloser{}
	router := newLazyActivityQueueRouter(nil, func(_ context.Context, _ string) (*Queue, interface{ Close() }, error) {
		close(dialing)
		<-proceed
		return NewQueue(&routingClientFake{}), closer, nil
	})

	enqueueErr := make(chan error, 1)
	go func() {
		enqueueErr <- router.Enqueue(context.Background(), "racing-queue", protocol.Task{Type: protocol.TaskTypeActivity, RunID: "r", TaskQueue: "racing-queue"})
	}()
	<-dialing
	router.Close()
	close(proceed)

	if err := <-enqueueErr; !errors.Is(err, errActivityRouterClosed) {
		t.Fatalf("enqueue racing with close: err = %v, want errActivityRouterClosed", err)
	}
	if !closer.closed.Load() {
		t.Fatal("frontend dialed during close was left open")
	}
	router.mu.Lock()
	cached := len(router.entries)
	router.mu.Unlock()
	if cached != 0 {
		t.Fatalf("router cached %d entries after close", cached)
	}
}
