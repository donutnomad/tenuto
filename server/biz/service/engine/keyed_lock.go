package engine

import (
	"context"
	"sync"
)

// keyedLockEntry 是单个 key 的锁状态。
//
// ch 是容量为 1 的信号量（等价 Temporal PrioritySemaphore(1)）：持有令牌者独占该 key。
// refCount 是引用计数护栏（对标 Temporal cache 的 refCount）：统计"当前关心该 key 的人数"
// （正在等锁的 + 正在持锁的）。它的唯一职责是决定何时可以安全地从 map 删除 entry。
type keyedLockEntry struct {
	ch       chan struct{} // 容量1信号量（等价 PrioritySemaphore(1)）
	refCount int           // 护栏：>0 绝不删（防删早了崩）；每 ++ 必有对应 --（防泄漏）
}

// KeyedLock 是进程内的 per-key 互斥锁（对标 Temporal 的 per-(workflowID,runID) 锁，
// 但本引擎无 sharding，故用最简的 map + chan 信号量 + 引用计数，不上 LRU/shard/finalizer）。
//
// 设计目标：同一 key 的操作串行化，不同 key 互不阻塞，且 entry 在无人使用时及时回收（零泄漏）。
type KeyedLock struct {
	mu      sync.Mutex
	entries map[string]*keyedLockEntry
}

func NewKeyedLock() *KeyedLock {
	return &KeyedLock{entries: make(map[string]*keyedLockEntry)}
}

// Lock 获取 key 的锁。ctx 取消则放弃并返回 ctx.Err()。
// 成功时返回幂等的 release 闭包，持锁期间用 defer 调用即可。
//
// 正确性护栏（对标 Temporal，逐条说明为什么不会删早/不会泄漏）：
//   - refCount 的 ++/-- 与 delete 都在同一把 k.mu 下完成 → 任何人只要还在"等锁或持锁"，
//     其 refCount++ 就尚未配对 --，refCount≥1，entry 绝不会被 delete（根治"删早了崩"，
//     即拿着一个已被别人从 map 删掉、又被新 Lock 重建的陌生 entry 上做信号量操作）。
//   - 每一次 refCount++（进入"关心"状态）都有唯一对应的 refCount--：抢到令牌后由 release 走 --；
//     ctx 取消放弃则由本函数立即走 releaseEntry --。两条出口覆盖全部分支 → 不泄漏。
//   - release 用 sync.Once 包裹，幂等 → reset 等"双 defer release"场景安全，且 panic 时
//     defer release 兜底归还令牌、回收 entry。
//
// ⚠️ 签名预留 priority 概念（暂忽略，将来填）：当前内部单优先级，调用方用 Lock(ctx, key) 即可。
//
// 契约：抢到令牌即返回成功，不在抢到令牌后二次检查 ctx——取消的优先级判断交给调用方 fn。
// （与 Temporal priority_semaphore 不同：它 Acquire 不返回 release 闭包，故须抢到后自查 ctx 并自 Release；
//
//	本实现把释放责任交给返回的 release 闭包 + 调用方 defer，select 双就绪选中 <-e.ch 时令牌已被取走、
//	由 release 闭包归还，令牌守恒不丢，无需二次检查。）若 ctx 已取消但碰巧抢到锁，fn 内用同一 ctx
//
// 的 RunInTx/Get 会自然失败返回，defer release 兜底回收，不卡死不泄漏。
func (k *KeyedLock) Lock(ctx context.Context, key string) (release func(), err error) {
	k.mu.Lock()
	e, ok := k.entries[key]
	if !ok {
		e = &keyedLockEntry{ch: make(chan struct{}, 1)}
		e.ch <- struct{}{} // 预填令牌：首个进入者可直接抢到
		k.entries[key] = e
	}
	e.refCount++ // ← 进入"关心"状态：此刻起本 entry 不可删（对标 cache refCount++）
	k.mu.Unlock()

	select {
	case <-e.ch: // 抢到令牌，独占该 key
	case <-ctx.Done(): // 放弃 → 必须归还 refCount（对标 cache 抢锁失败时 c.Release）
		k.releaseEntry(key, e, false)
		return nil, ctx.Err()
	}

	var once sync.Once
	// release 幂等（对标 makeReleaseFunc 的 CAS 防重复释放）。
	return func() { once.Do(func() { k.releaseEntry(key, e, true) }) }, nil
}

// releaseEntry 归还引用并在 refCount 归零时回收 entry。
//
// holdingLock 区分两种调用来源：
//   - true：来自 release（曾抢到令牌），需先把令牌还回 ch 以唤醒后继等待者。
//   - false：来自 Lock 内 ctx 取消分支（从未抢到令牌），不可还令牌（否则凭空多造一个令牌）。
//
// 关键：refCount-- 和可能的 delete 都在 k.mu 下原子完成，与 Lock 里的 refCount++/读 map 串行，
// 因此"某人 refCount++ 后、抢锁前"窗口内 entry 必然存活，不会被并发的归零删除。
func (k *KeyedLock) releaseEntry(key string, e *keyedLockEntry, holdingLock bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if holdingLock {
		e.ch <- struct{}{} // 归还令牌，唤醒后继等待者
	}
	e.refCount--
	if e.refCount == 0 {
		delete(k.entries, key) // ← 仅 refCount==0（无人等也无人持）才删（对标 tryEvict 仅删 refCount==0）
	}
}
