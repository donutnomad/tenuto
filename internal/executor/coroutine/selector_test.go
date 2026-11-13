package coroutine

import "testing"

// ============================================
// Selector 纯试探算法测试（照 Temporal selectorImpl，internal_workflow.go:140-145, 1416-1556）
// ============================================
// 测试策略（不涉 executor / yield / park，纯数据结构 + 声明序试探）：
// - Happy Path: 3 receive case，只第 2 个 channel 有值 → TrySelect 返回 readyIndex=1
// - 声明序:     多个 case 同时 ready → 返回声明序第一个（readyIndex 最小）
// - 竞争护栏:   注册后多个 channel 都触发 callback → 只第一个消费值，其余 callback 返回 false
// - default:    AddDefault 后无 case ready → TrySelect 返回 readyIndex=-1（host 走 default）
// - cleanups:   无 case ready → callback 进 blocked 队列 → 跑 cleanups 后队列清空（无残留）
// - recValue:   receive case 命中时 pre-store recValue，host 回调内 Receive 能取到值
// ============================================

// TestSelector_ReceiveHappyPath_SecondReady
// Happy Path（对应 Temporal Select receive 分支 :1427-1476，:1461 `if ok || !more`）：
// 3 个 receive case，只有第 2 个 channel 预存了值（buffered）。声明序遍历：
// case0 无值 → 登记 blockedReceive；case1 有值 → 当场命中，readyIndex=1，返回，不再看 case2。
func TestSelector_ReceiveHappyPath_SecondReady(t *testing.T) {
	c0 := NewChannel()
	c1 := NewBufferedChannel(1)
	c2 := NewChannel()
	c1.SendAsync("v1") // 只有 c1 预存值

	sel := NewSelector("s")
	sel.AddReceive(c0) // index 0
	sel.AddReceive(c1) // index 1
	sel.AddReceive(c2) // index 2

	readyIndex, cleanups := sel.TrySelect()

	if readyIndex != 1 {
		t.Fatalf("only c1 has value → want readyIndex=1, got %d", readyIndex)
	}
	// recValue pre-store（照 :1470）：c1 命中，值 pre-store 进 recValue，供 host 回调内 Receive 取。
	if c1.recValue == nil || *c1.recValue != "v1" {
		t.Fatalf("receive case hit must pre-store recValue=v1, got %v", c1.recValue)
	}
	// case0 未命中 → 其 callback 进 c0.blockedReceives；cleanups 应含移除它的清理函数。
	if len(cleanups) != 1 {
		t.Fatalf("case0 registered blockedReceive → want 1 cleanup, got %d", len(cleanups))
	}
	if len(c0.blockedReceives) != 1 {
		t.Fatalf("case0 callback should be in c0.blockedReceives, got %d", len(c0.blockedReceives))
	}
	// case2 在 case1 命中后 return，从未试探 → c2 无登记。
	if len(c2.blockedReceives) != 0 {
		t.Fatalf("case2 must not be probed (case1 returned first), got %d", len(c2.blockedReceives))
	}
}

// TestSelector_DeclarationOrder_FirstReadyWins
// 声明序（对应 Temporal `for _, pair := range s.cases` :1427，声明序遍历取第一个 ready）：
// 两个 receive case 的 channel 都预存了值（都能当场命中）。必须返回声明序第一个（readyIndex=0），
// 且第二个 channel 的值不被消费（recValue 未被设，因为 case1 命中即 return，从未试探 case2 之外的更早 return）。
func TestSelector_DeclarationOrder_FirstReadyWins(t *testing.T) {
	cA := NewBufferedChannel(1)
	cB := NewBufferedChannel(1)
	cA.SendAsync("A")
	cB.SendAsync("B")

	sel := NewSelector("s")
	sel.AddReceive(cA) // index 0
	sel.AddReceive(cB) // index 1

	readyIndex, _ := sel.TrySelect()

	if readyIndex != 0 {
		t.Fatalf("both ready → declaration order picks first: want 0, got %d", readyIndex)
	}
	// cA 命中，值 pre-store 进 recValue。
	if cA.recValue == nil || *cA.recValue != "A" {
		t.Fatalf("cA (first ready) must be selected, recValue=A, got %v", cA.recValue)
	}
	// cB 从未被试探（cA 命中即 return）→ 其 buffer 里的 B 原封不动，recValue 未设。
	if cB.recValue != nil {
		t.Fatalf("cB must not be touched (cA won), got recValue=%v", cB.recValue)
	}
	if cB.Len() != 1 {
		t.Fatalf("cB value must remain unconsumed, Len want 1, got %d", cB.Len())
	}
}

// TestSelector_RaceGuard_OnlyFirstConsumes
// 竞争护栏（对应 Temporal callback fn 内 `if readyBranch != nil { return false }` :1434/:1487）：
// 两个 receive case，注册时都无值（都进 blocked 队列）。随后手动模拟两个 channel【都】被 send 触发
// callback（模拟并发到达）。竞争护栏保证：只有第一个触发的 callback 消费值（设 readyIndex + recValue），
// 第二个 callback 看到 readyIndex 已占，返回 false → 其值不被消费，留在自己的 channel。
func TestSelector_RaceGuard_OnlyFirstConsumes(t *testing.T) {
	cX := NewChannel()
	cY := NewChannel()

	sel := NewSelector("s")
	sel.AddReceive(cX) // index 0
	sel.AddReceive(cY) // index 1

	// 首次试探：两个 channel 都无值 → 都登记 blockedReceive，readyIndex=-1。
	readyIndex, cleanups := sel.TrySelect()
	if readyIndex != -1 {
		t.Fatalf("no channel has value → want readyIndex=-1, got %d", readyIndex)
	}
	if len(cX.blockedReceives) != 1 || len(cY.blockedReceives) != 1 {
		t.Fatalf("both cases must register blockedReceive, got cX=%d cY=%d",
			len(cX.blockedReceives), len(cY.blockedReceives))
	}

	// 模拟两个 channel 都来了 send：sendAsyncImpl 会触发各自 blockedReceive 的 callback。
	// cX 先 send → 其 callback 抢占 readyIndex=0，消费值（ok=true）。
	okX := cX.SendAsync("x")
	if !okX {
		t.Fatal("send to cX must be consumed by the registered blocked receive (first, wins guard)")
	}
	// cY 后 send → 其 callback 看到 readyIndex 已占（=0），返回 false → 值未被消费。
	// SendAsync 无缓冲、无其他 receiver → 返回 false（值留存，登记不了因 pair=nil）。
	okY := cY.SendAsync("y")
	if okY {
		t.Fatal("race guard broken: second channel's value was consumed (readyIndex already taken)")
	}

	// 最终只 cX 被选中，recValue=x；cY 未被污染。
	if cX.recValue == nil || *cX.recValue != "x" {
		t.Fatalf("cX (first) must consume value into recValue=x, got %v", cX.recValue)
	}
	if cY.recValue != nil {
		t.Fatalf("cY (second, guarded) must not consume value, got recValue=%v", cY.recValue)
	}

	// 跑 cleanups 清理残留 callback（cX 的已在触发时出队；cY 的仍在 blocked 队列）。
	for _, cl := range cleanups {
		cl()
	}
	if len(cX.blockedReceives) != 0 || len(cY.blockedReceives) != 0 {
		t.Fatalf("after cleanups, blocked queues must be empty, got cX=%d cY=%d",
			len(cX.blockedReceives), len(cY.blockedReceives))
	}
}

// TestSelector_Default_NoCaseReady_ReturnsMinusOne
// default（对应 Temporal `if s.defaultFunc != nil` :1543-1547，本层只报「无 ready」）：
// 无任何 channel 有值 + AddDefault → TrySelect 返回 readyIndex=-1（host 层据 HasDefault 走 default）。
func TestSelector_Default_NoCaseReady_ReturnsMinusOne(t *testing.T) {
	c := NewChannel()

	sel := NewSelector("s")
	sel.AddReceive(c) // index 0，无值
	sel.AddDefault()

	if !sel.HasDefault() {
		t.Fatal("AddDefault must set HasDefault=true")
	}

	readyIndex, cleanups := sel.TrySelect()
	if readyIndex != -1 {
		t.Fatalf("no case ready → want readyIndex=-1 (host goes default), got %d", readyIndex)
	}
	// 即便有 default，未命中的 receive case 仍登记 blockedReceive + cleanup（照 :1478）——
	// host 走 default 前会跑 cleanups 清掉（照 defer cleanups :1421）。
	if len(cleanups) != 1 {
		t.Fatalf("unready receive case still registers cleanup, want 1, got %d", len(cleanups))
	}
	for _, cl := range cleanups {
		cl()
	}
	if len(c.blockedReceives) != 0 {
		t.Fatalf("after default-path cleanups, blockedReceives must be empty, got %d", len(c.blockedReceives))
	}
}

// TestSelector_Cleanups_DrainBlockedQueues
// cleanups（对应 Temporal cleanups :1420-1425/:1478-1480/:1507-1509）：
// 混合 receive + send case，全部未命中 → 各自 callback 进 channel 的 blockedReceives/blockedSends。
// 返回的 cleanups 跑完后，两个队列都必须清空（无 closure 残留泄漏）。
func TestSelector_Cleanups_DrainBlockedQueues(t *testing.T) {
	cr := NewChannel() // receive case：无值 → 登记 blockedReceive
	cs := NewChannel() // send case：unbuffered 无 receiver → 登记 blockedSend

	sel := NewSelector("s")
	sel.AddReceive(cr)      // index 0
	sel.AddSend(cs, "data") // index 1

	readyIndex, cleanups := sel.TrySelect()
	if readyIndex != -1 {
		t.Fatalf("no case ready → want readyIndex=-1, got %d", readyIndex)
	}
	// receive case 进 cr.blockedReceives；send case 进 cs.blockedSends。
	if len(cr.blockedReceives) != 1 {
		t.Fatalf("receive case must register blockedReceive, got %d", len(cr.blockedReceives))
	}
	if len(cs.blockedSends) != 1 {
		t.Fatalf("send case must register blockedSend, got %d", len(cs.blockedSends))
	}
	if len(cleanups) != 2 {
		t.Fatalf("two unready cases → want 2 cleanups, got %d", len(cleanups))
	}

	for _, cl := range cleanups {
		cl()
	}
	// 跑完 cleanups：两队列必须清空，无残留。
	if len(cr.blockedReceives) != 0 {
		t.Fatalf("after cleanups, cr.blockedReceives must be empty, got %d", len(cr.blockedReceives))
	}
	if len(cs.blockedSends) != 0 {
		t.Fatalf("after cleanups, cs.blockedSends must be empty, got %d", len(cs.blockedSends))
	}
}

// TestSelector_SendCaseReady
// Happy Path（对应 Temporal Select send 分支 :1481-1504，:1497 `if ok`）：
// send case 的 channel 有等待的 receiver（预登记 blockedReceive）→ sendAsyncImpl 当场把值交付，
// send case 命中，readyIndex 指向该 send case。
func TestSelector_SendCaseReady(t *testing.T) {
	c := NewChannel()

	// 预登记一个 blockedReceive，使后续 send 能当场交付。
	delivered := false
	c.receiveAsyncImpl(&receiveCallback{
		fn: func(v any, more bool) bool {
			delivered = true
			return true
		},
	})

	sel := NewSelector("s")
	sel.AddReceive(NewChannel()) // index 0：无值 → 不 ready
	sel.AddSend(c, "payload")    // index 1：有等待 receiver → 当场交付

	readyIndex, _ := sel.TrySelect()
	if readyIndex != 1 {
		t.Fatalf("send case with waiting receiver → want readyIndex=1, got %d", readyIndex)
	}
	if !delivered {
		t.Fatal("send case hit must deliver value to the waiting receiver")
	}
}

// TestSelector_PostRegisterTrigger_ReadyIndexObservable
// 🔴 跨层可观察（P0，对标 Temporal Select 的 readyBranch 在 callback 与 yield 循环间共享 :1419/:1434/:1549）：
// TrySelect 首次试探无值 → 注册 callback 进 blockedReceives、readyIndex=-1。此后 channel 被 send 触发已登记的
// callback → callback 设 s.readyIndex。host（5b）park 醒来经 ReadyIndex() 必须能观察到 readyIndex=0。
// 若 readyIndex 是 TrySelect 局部变量（非 Selector 字段），host 只拿到返回值拷贝 -1，永远看不到后续触发 → Select 永卡。
func TestSelector_PostRegisterTrigger_ReadyIndexObservable(t *testing.T) {
	ch := NewChannel() // unbuffered
	sel := NewSelector("s")
	sel.AddReceive(ch)

	readyIndex, cleanups := sel.TrySelect()
	if readyIndex != -1 {
		t.Fatalf("首次试探无值应 readyIndex=-1, got %d", readyIndex)
	}
	if len(ch.blockedReceives) != 1 {
		t.Fatalf("未命中应登记 callback 进 blockedReceives, got %d", len(ch.blockedReceives))
	}

	// 后续 sender 触发已登记的 callback（模拟 5b host park 期间兄弟 lane send）。
	ch.sendAsyncImpl("v", nil)

	// host park 醒来经 ReadyIndex() 重查——必须观察到 case 0 ready（这是 5b Select 循环命中的依据）。
	if got := sel.ReadyIndex(); got != 0 {
		t.Fatalf("post-register 触发后 host 应经 ReadyIndex() 观察到 readyIndex=0, got %d（若为 -1 说明跨层共享标志失效）", got)
	}

	for _, c := range cleanups {
		c()
	}
}

// TestSelector_CloseWakesReceiveWithMoreFalse：park 期间 channel 被关闭时，唤醒的 receive case
// 报告 more=false，且不预存值（回调里的 Receive 看到的是关闭，而不是一个 nil 值）。
func TestSelector_CloseWakesReceiveWithMoreFalse(t *testing.T) {
	ch := NewChannel()
	sel := NewSelector("s")
	sel.AddReceive(ch)
	if readyIndex, _ := sel.TrySelect(); readyIndex != -1 {
		t.Fatalf("首次试探无值应 readyIndex=-1, got %d", readyIndex)
	}
	ch.Close()
	if got := sel.ReadyIndex(); got != 0 {
		t.Fatalf("关闭应唤醒 case 0, got %d", got)
	}
	if sel.ReadyMore() {
		t.Fatal("关闭唤醒的 receive case 必须报告 more=false")
	}
	if ch.recValue != nil {
		t.Fatalf("关闭唤醒不应预存值, got %v", *ch.recValue)
	}
}

// TestSelector_ValueWakesReceiveWithMoreTrue：park 期间收到值时 more=true 且值已预存。
func TestSelector_ValueWakesReceiveWithMoreTrue(t *testing.T) {
	ch := NewChannel()
	sel := NewSelector("s")
	sel.AddReceive(ch)
	sel.TrySelect()
	ch.sendAsyncImpl("v", nil)
	if !sel.ReadyMore() || ch.recValue == nil || *ch.recValue != "v" {
		t.Fatalf("值唤醒应 more=true 且预存 v, more=%v recValue=%v", sel.ReadyMore(), ch.recValue)
	}
}
