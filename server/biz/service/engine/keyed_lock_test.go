package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================
// KeyedLock 测试
// ============================================
// 测试策略:
// - Happy Path: 同 key 串行（第二个 Lock 阻塞到第一个 release）
// - Happy Path: 不同 key 并发（互不阻塞）
// - 并发压力: N goroutine 抢同 key，共享计数最终正确（串行化保证）
// - Error Case: ctx 已取消/超时 → 返回 ctx.Err()，refCount 正确归还（不泄漏）
// - Edge Case: refCount 清理——Lock+release 后 map 中 key 被删（EntryCount 归零）
// - Edge Case: release 幂等——多次调用不 panic、不重复还令牌
// ============================================

// Happy Path: 同一个 key 必须串行——第二个 Lock 在第一个 release 之前阻塞。
func TestKeyedLock_SameKeySerial(t *testing.T) {
	k := NewKeyedLock()
	ctx := context.Background()

	release1, err := k.Lock(ctx, "wf-1")
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}

	started := make(chan struct{})
	acquired := make(chan struct{})
	done := make(chan struct{}) // goroutine 整体结束（含 release2 完成），用于无竞态地查 EntryCount
	go func() {
		defer close(done)
		close(started) // 先同步：保证主 goroutine 等到本 goroutine 真正发起 Lock 调用，排除"未调度"假象
		release2, err := k.Lock(ctx, "wf-1")
		if err != nil {
			t.Errorf("second lock failed: %v", err)
			return
		}
		close(acquired)
		release2()
	}()

	// 等 goroutine 启动并即将进入 Lock，再断言"第一个还持锁时第二个拿不到"。
	<-started
	select {
	case <-acquired:
		t.Fatal("second Lock acquired while first still held")
	case <-time.After(200 * time.Millisecond):
	}

	release1() // 释放后第二个应迅速拿到。
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second Lock did not acquire after release")
	}

	// 等 goroutine 完全结束（release2 已归还令牌、回收 entry），再查 EntryCount，避免竞态。
	<-done
	if got := k.EntryCount(); got != 0 {
		t.Fatalf("entry leak: EntryCount=%d, want 0", got)
	}
}

// Happy Path: 不同 key 互不阻塞，可同时持有。
func TestKeyedLock_DifferentKeysConcurrent(t *testing.T) {
	k := NewKeyedLock()
	ctx := context.Background()

	releaseA, err := k.Lock(ctx, "wf-A")
	if err != nil {
		t.Fatalf("lock A failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		releaseB, err := k.Lock(ctx, "wf-B") // 不应被 A 阻塞
		if err != nil {
			t.Errorf("lock B failed: %v", err)
			return
		}
		releaseB()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lock B blocked by unrelated key A")
	}

	releaseA()
	if got := k.EntryCount(); got != 0 {
		t.Fatalf("entry leak: EntryCount=%d, want 0", got)
	}
}

// 并发压力: 100 goroutine 抢同一 key，各自 Lock→改共享计数→release。
// 串行化保证下最终计数精确为 N。配合 -race 验证无数据竞争。
func TestKeyedLock_ConcurrentStressSameKey(t *testing.T) {
	k := NewKeyedLock()
	ctx := context.Background()

	const n = 100
	var counter int // 故意用裸 int：靠锁串行化保护，-race 会暴露任何破绽
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			release, err := k.Lock(ctx, "hot-key")
			if err != nil {
				t.Errorf("lock failed: %v", err)
				return
			}
			counter++ // 临界区：仅持锁者可改
			release()
		}()
	}
	wg.Wait()

	if counter != n {
		t.Fatalf("counter=%d, want %d (serialization broken)", counter, n)
	}
	if got := k.EntryCount(); got != 0 {
		t.Fatalf("entry leak after stress: EntryCount=%d, want 0", got)
	}
}

// Error Case: ctx 已取消时 Lock 立即返回 ctx.Err()，且 refCount 正确归还——
// 后续同 key 的 Lock 仍能成功（证明取消分支没泄漏 refCount）。
func TestKeyedLock_CtxCancelledReturnsErrAndCleansUp(t *testing.T) {
	k := NewKeyedLock()

	// 先占住锁，使后来者只能阻塞在 ch 上。
	holder, err := k.Lock(context.Background(), "wf-1")
	if err != nil {
		t.Fatalf("holder lock failed: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	release, err := k.Lock(cancelCtx, "wf-1")
	if err == nil {
		release()
		t.Fatal("expected ctx error, got nil")
	}
	if !errorsIsCanceled(err) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// holder 仍持锁 → 仅它一个 refCount，取消者已归还。
	if got := k.EntryCount(); got != 1 {
		t.Fatalf("EntryCount=%d after cancel, want 1 (cancel must not leak refCount)", got)
	}

	holder() // 释放后 entry 应被回收。
	if got := k.EntryCount(); got != 0 {
		t.Fatalf("EntryCount=%d after holder release, want 0", got)
	}

	// 同 key 再次 Lock 应成功（无残留信号量错乱）。
	again, err := k.Lock(context.Background(), "wf-1")
	if err != nil {
		t.Fatalf("re-lock after cancel failed: %v", err)
	}
	again()
}

// Error Case: ctx 超时（非立即取消）下，等锁者超时返回，refCount 归还，持锁者照常。
func TestKeyedLock_CtxTimeoutWhileWaiting(t *testing.T) {
	k := NewKeyedLock()

	holder, err := k.Lock(context.Background(), "wf-1")
	if err != nil {
		t.Fatalf("holder lock failed: %v", err)
	}
	defer holder()

	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	release, err := k.Lock(waitCtx, "wf-1")
	if err == nil {
		release()
		t.Fatal("expected deadline error, got nil")
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("returned too early (%v), should have waited for timeout", elapsed)
	}

	// 只有 holder 还持锁。
	if got := k.EntryCount(); got != 1 {
		t.Fatalf("EntryCount=%d after timeout, want 1", got)
	}
}

// Edge Case: refCount 清理——一次 Lock+release 后 entry 必须从 map 删除，无泄漏。
func TestKeyedLock_EntryRemovedAfterRelease(t *testing.T) {
	k := NewKeyedLock()
	ctx := context.Background()

	release, err := k.Lock(ctx, "wf-1")
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	if got := k.EntryCount(); got != 1 {
		t.Fatalf("EntryCount=%d while held, want 1", got)
	}

	release()
	if got := k.EntryCount(); got != 0 {
		t.Fatalf("EntryCount=%d after release, want 0 (refCount cleanup failed)", got)
	}
}

// Edge Case: release 幂等——多次调用不 panic、不重复还令牌（否则后继会拿到"凭空"的令牌）。
func TestKeyedLock_ReleaseIdempotent(t *testing.T) {
	k := NewKeyedLock()
	ctx := context.Background()

	release, err := k.Lock(ctx, "wf-1")
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	// 多次释放必须安全。
	release()
	release()
	release()

	if got := k.EntryCount(); got != 0 {
		t.Fatalf("EntryCount=%d after repeated release, want 0", got)
	}

	// 若 release 重复还了令牌，ch 容量为 1 会令第二个 Lock 异常；这里验证仍是严格串行。
	var holding atomic.Bool
	r1, err := k.Lock(ctx, "wf-1")
	if err != nil {
		t.Fatalf("re-lock failed: %v", err)
	}
	holding.Store(true)

	started := make(chan struct{})
	got := make(chan struct{})
	go func() {
		close(started) // 先同步：保证主 goroutine 等到本 goroutine 真正发起 Lock 调用，排除"未调度"假象
		r2, err := k.Lock(ctx, "wf-1")
		if err != nil {
			t.Errorf("second re-lock failed: %v", err)
			return
		}
		if holding.Load() {
			t.Error("acquired while previous holder active: token leaked by repeated release")
		}
		r2()
		close(got)
	}()

	// 等 goroutine 启动并即将进入 Lock，再断言"令牌未被重复释放、第二个仍被阻塞"。
	<-started
	select {
	case <-got:
		t.Fatal("second Lock acquired immediately: repeated release leaked a token")
	case <-time.After(200 * time.Millisecond):
	}

	holding.Store(false)
	r1()
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("second Lock never acquired")
	}
}

func errorsIsCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}

// EntryCount 返回当前存活的 entry 数量，测试用它验证"无泄漏"（refCount 归零后 key 被删）。
func (k *KeyedLock) EntryCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.entries)
}
