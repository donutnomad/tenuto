package coroutine

import (
	"runtime"
	"slices"
)

// Coroutine 协作式协程
type Coroutine struct {
	id       uint64
	name     string
	fn       func(yield func())
	yield    func() // 本协程的 yield 句柄（newState 内建），供 Scheduler.CurrentYield 按 current 动态解析
	yieldCh  chan struct{}
	resumeCh chan struct{}
	// cancelCh: 收敛回收信号。close 后，所有挂起点（初始挂起点 + yield 点）走 runtime.Goexit() 回收。
	cancelCh chan struct{}
	// doneCh: join 用。goroutine 真正退出前 close，Reclaim 靠 <-doneCh 确认协程已回收。
	doneCh chan struct{}
	// keptBlocked 为 true 表示协程自上次被唤醒后没有取得进展（醒来立刻又 yield）。yield 返回时置 true，
	// 挂起原语在真正取得进展时调 unblocked() 清 false。RunBatch 的收敛判据读它。
	// 读写全在协作式单线程内（resumeCh/yieldCh 串行建立 happens-before），普通 bool 即可。
	keptBlocked bool
	// done: 协程 goroutine 在 done-defer 写；调度器仅在 <-yieldCh / <-doneCh 建立
	// happens-before 之后读（Reclaim 内绝不裸读，见 RunBatch/Reclaim）。
	done bool
}

// unblocked 置 keptBlocked=false，表示协程自上次被唤醒后取得了进展（挂起原语命中条件返回，而非再次 yield）。
func (c *Coroutine) unblocked() { c.keptBlocked = false }

// Scheduler 协作式协程调度器，确保确定性执行顺序
type Scheduler struct {
	coroutines []*Coroutine
	// sequence 是已建协程计数，每次 newState 自增。
	sequence int
	// newEagerCoroutines 暂存迭代中新起的 eager 协程。
	// RunBatch 迭代体末尾用 slices.Insert 把它们插到当前协程之后，使其当轮就被后续 i++ 遍历到。
	newEagerCoroutines []*Coroutine
	// current 是 RunBatch 当前正在 resume 的协程。协作式单线程下 resume 与 <-yieldCh 之间只有它在跑，
	// 故它就是「当前执行线」的协程——挂起原语（executor 侧 Get/Wait 命中）经 Unblocked() 定位到它清 keptBlocked。
	// 仅 RunBatch 写，Unblocked() 读。
	current *Coroutine
}

// CurrentYield 返回当前正在执行协程的 yield 句柄（RunBatch 每次 resume 前设 s.current）。
// 顶层挂起原语（executor.park）用它冻结【当前跑的那条协程】的栈——按 s.current 动态解析，天然随
// 交错 park/resume 正确跟踪，杜绝共享单字段串味。current 为 nil（调度循环之外）时返回 nil，由调用方兜底。
func (s *Scheduler) CurrentYield() func() {
	if s.current != nil {
		return s.current.yield
	}
	return nil
}

// CurrentRoutineID returns the deterministic scheduler-local identity of the
// coroutine currently being resumed. The main coroutine is 0 and spawned
// lanes receive increasing IDs in creation order.
func (s *Scheduler) CurrentRoutineID() uint64 {
	if s.current != nil {
		return s.current.id
	}
	return 0
}

// Unblocked 由挂起原语在取得进展、命中条件返回时调用（channel 收到值、future ready 等），
// 清当前执行协程的 keptBlocked。
// current 为 nil（调度循环之外）时为空操作。
func (s *Scheduler) Unblocked() {
	if s.current != nil {
		s.current.unblocked()
	}
}

// NewScheduler 创建调度器
func NewScheduler() *Scheduler {
	return &Scheduler{}
}

// Spawn 注册一个协程函数入普通 coroutines 队列，启动 goroutine 但立刻挂起等待调度。
func (s *Scheduler) Spawn(name string, fn func(yield func())) {
	s.newState(name, fn, false)
}

// SpawnEager 注册一个 eager 协程入 newEagerCoroutines 暂存区。
// 迭代中新起的 eager 协程会被 RunBatch 的 slices.Insert 插到当前协程之后，当轮即被调度。
func (s *Scheduler) SpawnEager(name string, fn func(yield func())) {
	s.newState(name, fn, true)
}

// newState 建 Coroutine、sequence 自增，按 eager 入 newEagerCoroutines 或 coroutines，
// 启动 goroutine 但立刻挂起等待调度。
func (s *Scheduler) newState(name string, fn func(yield func()), eager bool) {
	c := &Coroutine{
		id:       uint64(s.sequence),
		name:     name,
		fn:       fn,
		yieldCh:  make(chan struct{}),
		resumeCh: make(chan struct{}),
		cancelCh: make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	// yield 句柄建一次，存 c.yield（供 CurrentYield 按 s.current 动态解析，消除 executor 侧共享单字段
	// 在交错 park/resume 时串味的隐患）并传给 fn（测试直接用 yield 参数）。
	c.yield = func() {
		// yield: 通知调度器 → 等待唤醒或回收
		c.yieldCh <- struct{}{}
		select {
		case <-c.resumeCh:
			// 被 resume 醒来后置 keptBlocked=true。
			// 「醒来即视为无进展」，直到协程跑到挂起原语命中条件调 unblocked() 清 false。若醒来后
			// 立刻又 yield（检查-yield 循环里条件仍不满足），keptBlocked 保持 true → 该协程算稳定 block。
			// 写在调度器 <-yieldCh 建立 HB 之前的协程侧，调度器收敛判据读它有 happens-before。
			c.keptBlocked = true
		case <-c.cancelCh:
			runtime.Goexit()
		}
	}
	s.sequence++
	if eager {
		s.newEagerCoroutines = append(s.newEagerCoroutines, c)
	} else {
		s.coroutines = append(s.coroutines, c)
	}

	go func() {
		// done-defer 必须在任何挂起点（含初始点）之前注册：初始点的 runtime.Goexit() 若在 defer
		// 注册前执行，会跳过 done-defer → close(doneCh) 不跑 → Reclaim 的 <-doneCh 死锁。
		defer func() {
			// 先写 done，再发 yieldCh，使调度器 <-yieldCh 之后读 done 有 happens-before。
			c.done = true
			// 回收路径（cancelCh 已 close）不发 yieldCh（无接收者，发送会死锁）；只 close(doneCh) 供 join。
			// 非回收路径（正常 return / 挂起点 Goexit 但未被 cancel）发 yieldCh 通知轮转。
			select {
			case <-c.cancelCh:
			default:
				c.yieldCh <- struct{}{}
			}
			close(c.doneCh)
		}()
		// 初始挂起点：也监听 cancelCh，使"从未被 resume 就被 reclaim"的协程能被唤醒回收。
		select {
		case <-c.resumeCh:
		case <-c.cancelCh:
			// fn 尚未开始跑 → 走 done-defer 回收。
			runtime.Goexit()
		}
		fn(c.yield)
	}()
}

// RunBatch 轮转推进所有协程，直到全部稳定 blocked（allBlocked 收敛）。收敛后不回收：协程栈跨批存活，
// 每批 History 事件应用完后再调一次 RunBatch 唤醒推进。
func (s *Scheduler) RunBatch() {
	for {
		// 假设全部 block，任一协程本轮取得进展（done 或 keptBlocked=false）则拉低。
		allBlocked := true
		// 记录本轮起始 sequence：迭代中新起了协程（sequence 增长）则本轮不收敛。
		lastSequence := s.sequence
		// 下标遍历：迭代体末尾可能 slices.Insert 扩容 coroutines，
		// range 快照下标会漏掉当轮插入的 eager 协程，故必须用 len() 实时求值。
		for i := 0; i < len(s.coroutines); i++ {
			c := s.coroutines[i]
			if c.done {
				continue
			}
			// 无条件 resume 每个非 done 协程，包括已 yield（park）的协程。
			// current 供挂起原语经 Unblocked() 定位当前执行协程清 keptBlocked。
			s.current = c
			c.resumeCh <- struct{}{}
			<-c.yieldCh // 建立 happens-before：其后读 c.done / c.keptBlocked 无 race
			s.current = nil
			if !c.done {
				// 协程醒来无进展（keptBlocked 保持 true）才算稳定 block；
				// 真取得进展（Get/Wait 命中调 Unblocked() 清 false）则拉低 allBlocked → 本轮继续轮。
				allBlocked = allBlocked && c.keptBlocked
			} else {
				// 协程 done（跑完 / Goexit）算取得进展 → 本轮不收敛，给醒来的等待者机会。
				allBlocked = false
			}
			// 当前协程被 resume 时新起了 eager 协程：
			// 插到 i+1 位置，使其当轮就被后续 i++ 遍历到。
			if len(s.newEagerCoroutines) > 0 {
				s.coroutines = slices.Insert(s.coroutines, i+1, s.newEagerCoroutines...)
				s.newEagerCoroutines = nil
				allBlocked = false
			}
		}
		// 本轮新起了协程（sequence 增长）则不收敛。
		allBlocked = allBlocked && lastSequence == s.sequence
		if allBlocked {
			break
		}
	}
}

// Reclaim 在全部批推进完后调用一次，无条件对每个协程 close(cancelCh) 一次、<-doneCh 一次，回收所有存活 goroutine。
// 全程不读 racy 的 done/parked：
//   - close(cancelCh) 对仍挂起的协程 → 唤醒走 <-cancelCh → runtime.Goexit()；对已自然 done 的协程 →
//     close 一个无接收者的未关闭 channel，合法不 panic。cancelCh 每协程唯一、仅此处 close → 无 double-close。
//   - <-doneCh 对已 done 协程 → doneCh 已被 done-defer close，读已关闭 channel 立即返回；对被 cancel 唤醒
//     的协程 → 它 Goexit→done-defer→close(doneCh)，接到。回收路径 done-defer 恒走 cancel 分支不发 yieldCh，
//     故无"向无接收者发送"、无死锁。
//
// ⚠️ 必须【串行】回收（close 一个 → join 一个 → 再下一个），不能先 close 全部再 join 全部：
// park 之后 lane 冻结在 InvokeClosure 栈中间（真 yield 而非 Goexit），close(cancelCh) 唤醒它走 Goexit
// 会【展开】那条冻结的 InvokeClosure 栈、跑其 defer。若一次 close 全部，多条 lane 的 Goexit 展开会并发
// 触碰共享的 WLang Executor 内部状态（InvokeClosure 非并发安全）→ data race。串行回收使每条 lane 的展开
// 跑完再唤醒下一条，维持「一次只一条协程动 wlExec」的协作式单线程不变式（与调度器轮转期一致）。
func (s *Scheduler) Reclaim() {
	for _, c := range s.coroutines {
		close(c.cancelCh)
		<-c.doneCh
	}
}

// RoutineCount returns the number of scheduler-owned coroutine stacks.
func (s *Scheduler) RoutineCount() int {
	return len(s.coroutines) + len(s.newEagerCoroutines)
}
