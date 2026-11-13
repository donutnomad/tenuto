package coroutine

import (
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// waitGoroutines 轮询等待 goroutine 数回落到 baseline，避免 cancel→Goexit 回收的调度延迟造成假阳性。
// Reclaim 的 <-doneCh join 保证 fn 已退出，但 Go 运行时销毁 goroutine 有微小延迟。
func waitGoroutines(baseline int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= baseline || time.Now().After(deadline) {
			return n
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// runAndReclaim 推进到全部协程稳定 blocked，再回收全部协程。
func runAndReclaim(s *Scheduler) {
	s.RunBatch()
	s.Reclaim()
}

// runWithTimeout 在超时内跑 fn，超时视为死锁并 Fatal。
func runWithTimeout(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("RunBatch+Reclaim 未在超时内返回，疑似死锁")
	}
}

// Happy Path（Temporal keptBlocked 收敛语义）：协程用「检查-yield 循环」（照 internal_workflow.go:810-837
// channelImpl.Receive）等一个永不满足的条件——每轮被 RunBatch+Reclaim 无条件 resume（照 :1289-1294，Temporal 不
// 物理排除已 block 协程），从 yield 点续跑回循环顶重查，条件仍不满足 → 立刻又 yield 且不调 unblocked() →
// keptBlocked 保持 true（照 :1069）→ RunBatch+Reclaim 判定稳定 block 收敛（照 :1308/:1319）。
//
// 断言从旧的「reached==1」（锁旧 park-物理出局语义）改为：循环体内计数每轮都被跑到（协程确实被每轮 resume，
// = Temporal 语义），但循环【之后】的行永不执行（条件永假、无进展 → 稳定 block）。前者证明「每轮 resume」，
// 后者证明「keptBlocked 收敛使无进展协程停在 yield」——正是移植后要表达的收敛不变式。
func TestScheduler_RunBatch_SingleParkedCoroutine_Returns(t *testing.T) {
	s := NewScheduler()
	loopBody := int32(0)
	afterLoop := int32(0)
	condMet := false // 永不满足的条件：检查-yield 循环永远走 yield 分支
	s.Spawn("main", func(yield func()) {
		for {
			atomic.AddInt32(&loopBody, 1) // 每轮被 resume 续跑到这里（Temporal 每轮 call）
			if condMet {
				break // 条件满足才越过——本例永不
			}
			yield() // 无进展（未调 unblocked）→ keptBlocked 保持 true → 收敛
		}
		atomic.AddInt32(&afterLoop, 1) // 循环之后：条件永假 → 稳定 block → 永不执行
	})

	runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

	// 稳定 block 的协程停在 yield 冻结栈，循环之后的行永不执行（keptBlocked 收敛不变式）。
	if got := atomic.LoadInt32(&afterLoop); got != 0 {
		t.Fatalf("条件永假、无进展的检查-yield 循环，循环之后的行不应执行（afterLoop 应为 0），got %d", got)
	}
	// 协程确实被每轮 resume 续跑（Temporal 语义，非旧 park-物理出局）：循环体至少被跑到 2 次
	// （轮1 首次执行到 yield；轮2 被 resume 续跑回循环顶重查，keptBlocked 保持 true 后收敛）。
	if got := atomic.LoadInt32(&loopBody); got < 2 {
		t.Fatalf("协程应被每轮 resume 续跑回循环顶重查（loopBody 应 >= 2），got %d", got)
	}
}

// Happy Path: park 协程被 reclaim 后 goroutine 回收，无泄漏；-race 干净。
func TestScheduler_RunBatch_ReclaimsParkedGoroutine_NoLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	s := NewScheduler()
	s.Spawn("main", func(yield func()) {
		yield()
		yield() // 多个 park 点，验证回收从任一 park 点都能 unwind
	})
	runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

	if n := waitGoroutines(baseline, 2*time.Second); n > baseline {
		t.Fatalf("park 协程未回收：baseline=%d, after=%d（泄漏）", baseline, n)
	}
}

// Edge Case: 回收路径（cancel→Goexit）会 unwind 栈并跑协程内注册的 Go defer。
func TestScheduler_RunBatch_CancelRunsGoDefer(t *testing.T) {
	s := NewScheduler()
	deferRan := int32(0)
	s.Spawn("main", func(yield func()) {
		defer atomic.AddInt32(&deferRan, 1) // cancel→Goexit unwind 时应执行
		yield()
	})
	runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

	if got := atomic.LoadInt32(&deferRan); got != 1 {
		t.Fatalf("回收 Goexit 应跑协程内 Go defer（deferRan 应为 1），got %d", got)
	}
}

// Edge Case（修正1/2 场景1）: 多个 parked 协程被 reclaim，全部回收、无泄漏、-race 干净。
func TestScheduler_RunBatch_MultipleParkedReclaimed_NoLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	s := NewScheduler()
	deferCount := int32(0)
	for range 5 {
		s.Spawn("lane", func(yield func()) {
			defer atomic.AddInt32(&deferCount, 1)
			yield() // 全部 park，RunBatch+Reclaim 收敛后 Reclaim 逐个回收
		})
	}
	runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

	if got := atomic.LoadInt32(&deferCount); got != 5 {
		t.Fatalf("5 个 park 协程应全部回收并跑 defer，got %d", got)
	}
	if n := waitGoroutines(baseline, 2*time.Second); n > baseline {
		t.Fatalf("park 协程未全部回收：baseline=%d, after=%d", baseline, n)
	}
}

// Edge Case（修正2 场景2）: 从未被 resume 的协程被 reclaim——初始挂起点监听 cancelCh，
// close(cancelCh) 能唤醒它 Goexit，<-doneCh 不死锁、不泄漏。
//
// 直接调 Reclaim（不经 RunBatch+Reclaim），构造"Spawn 后从未 resume"的协程，
// 验证初始挂起点（scheduler.go 的 select{resumeCh|cancelCh}）能被 cancel 唤醒回收。
func TestScheduler_ReclaimNeverResumedCoroutine_NoDeadlock(t *testing.T) {
	baseline := runtime.NumGoroutine()

	s := NewScheduler()
	fnStarted := int32(0)
	for range 3 {
		s.Spawn("never-resumed", func(yield func()) {
			atomic.AddInt32(&fnStarted, 1) // 若被 cancel 从初始点唤醒，fn 不应开始跑
		})
	}
	// 不 resume 任何协程，直接回收——它们都停在初始挂起点。
	runWithTimeout(t, 2*time.Second, s.Reclaim)

	if got := atomic.LoadInt32(&fnStarted); got != 0 {
		t.Fatalf("从未 resume 的协程 fn 不应开始跑（fnStarted 应为 0），got %d", got)
	}
	if n := waitGoroutines(baseline, 2*time.Second); n > baseline {
		t.Fatalf("从未 resume 的协程未回收：baseline=%d, after=%d", baseline, n)
	}
}

// Determinism（Temporal keptBlocked 收敛语义）：RunBatch+Reclaim 多次运行的可观测行为一致。
// 用检查-yield 循环（条件永假）：协程每轮被 resume 续跑回循环顶重查、无进展则再 yield（keptBlocked 保持
// true 收敛）。断言从旧「trace=[start]」（锁旧 park-物理出局）改为：循环之后的 "escaped" 永不出现（稳定
// block 不越过 yield），且循环体每轮跑到 "check" 稳定累计 >= 2（每次 iter 计数一致 = 确定性）。
func TestScheduler_RunBatch_Determinism(t *testing.T) {
	var reference int
	for iter := range 100 {
		s := NewScheduler()
		var trace []string
		condMet := false // 永不满足
		s.Spawn("main", func(yield func()) {
			for {
				trace = append(trace, "check") // 每轮被 resume 续跑到这里
				if condMet {
					break
				}
				yield() // 无进展 → keptBlocked 保持 → 收敛
			}
			trace = append(trace, "escaped") // 稳定 block → 永不执行
		})
		runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

		// "escaped" 永不出现：条件永假、无进展的协程停在 yield 冻结栈。
		if slices.Contains(trace, "escaped") {
			t.Fatalf("iter %d: 稳定 block 不应越过 yield（trace 不应含 escaped），got %v", iter, trace)
		}
		// 协程被每轮 resume 续跑（Temporal 语义）：至少 2 次 "check"。
		if len(trace) < 2 {
			t.Fatalf("iter %d: 协程应被每轮 resume 续跑回循环顶（trace 应 >= 2 个 check），got %v", iter, trace)
		}
		// 确定性：每次 iter 的 trace 长度一致（收敛轮数稳定）。
		if iter == 0 {
			reference = len(trace)
			continue
		}
		if len(trace) != reference {
			t.Fatalf("iter %d: trace 长度不确定（收敛轮数漂移）：期望 %d，got %d（%v）", iter, reference, len(trace), trace)
		}
	}
}

// Happy Path（阶段1 eager 机制）: 某协程被 resume 时通过 SpawnEager 起一个新 eager 协程，
// 断言该 eager 协程【当轮】就被 resume 执行——验证 RunBatch+Reclaim 迭代体末尾的 slices.Insert 生效。
//
// 若不加 slices.Insert（红），eager 协程会滞留在 newEagerCoroutines，永不进入 coroutines，
// 既不会被本轮 i++ 遍历到，也不在 Reclaim 的回收范围内——eagerRan 恒为 0。
func TestScheduler_EagerCoroutineScheduledSameRound(t *testing.T) {
	s := NewScheduler()
	var trace []string

	s.Spawn("main", func(yield func()) {
		trace = append(trace, "main-start")
		// 迭代中动态起 eager 协程：期望它当轮（main park 之后）就被调度。
		s.SpawnEager("eager", func(yield func()) {
			trace = append(trace, "eager-ran")
		})
		trace = append(trace, "main-before-park")
		yield() // main park 出局；slices.Insert 把 eager 插到 main 之后当轮调度
	})

	runWithTimeout(t, 2*time.Second, func() { runAndReclaim(s) })

	// eager 协程必须在 main 之后、同一轮被执行到（trace 含 eager-ran）。
	if !slices.Contains(trace, "eager-ran") {
		t.Fatalf("eager 协程当轮未被调度（slices.Insert 未生效）：trace=%v", trace)
	}
	// 确定性顺序断言：main 先跑并 park，随后 eager 当轮补上。
	expected := []string{"main-start", "main-before-park", "eager-ran"}
	if len(trace) != len(expected) {
		t.Fatalf("trace 长度不符：expected %v, got %v", expected, trace)
	}
	for i := range expected {
		if trace[i] != expected[i] {
			t.Fatalf("at %d: expected %q got %q (full %v)", i, expected[i], trace[i], trace)
		}
	}
}
