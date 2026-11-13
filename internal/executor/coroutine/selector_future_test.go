package coroutine

import "testing"

// ============================================
// Selector future case 测试（照 Temporal Select future 分支 :1510-1541）
// ============================================
// 测试策略（纯试探算法，不涉 host/yield）：
// - Happy Path: AddFuture(已 Set) → TrySelect 当场命中（T4）
// - Edge Case:  AddFuture(未 Set) → -1，cleanups 移除 future channel 的 callback（T5）
// - Happy Path: 两个已 ready future 逐个消费——MarkConsumed+Reset 后第二次命中下一个（T6，照 :1519）
// - Edge Case:  future 与 channel 混合声明序，第一个 ready 获胜（T7，照 :1427 声明序）
// - Edge Case:  closed channel receive case 命中后 MarkConsumed，重复 Select 不再命中（T7b，照 :1472）
// ============================================

// Happy Path: AddFuture(已 Set) → TrySelect 返回该 case index。
func TestSelector_FutureCase_ReadyAtTrySelect_Hits(t *testing.T) {
	f := NewFuture()
	f.Set("done", nil)
	sel := NewSelector("s")
	sel.AddFuture(f) // index 0
	readyIndex, _ := sel.TrySelect()
	if readyIndex != 0 {
		t.Fatalf("已 Set 的 future case 应当场命中, got %d", readyIndex)
	}
}

// Edge Case: AddFuture(未 Set) → TrySelect=-1；callback 已登记进 future channel；
// 跑 cleanups 后 blockedReceives 归零（防泄漏锚，照 :1538-1540）。
func TestSelector_FutureCase_NotReady_CleanupsRemoveCallback(t *testing.T) {
	f := NewFuture()
	sel := NewSelector("s")
	sel.AddFuture(f)
	readyIndex, cleanups := sel.TrySelect()
	if readyIndex != -1 {
		t.Fatalf("未 Set 的 future 不应命中, got %d", readyIndex)
	}
	if n := f.channel.BlockedReceiveCount(); n != 1 {
		t.Fatalf("callback 应登记进 future channel, count=%d want 1", n)
	}
	for _, c := range cleanups {
		c()
	}
	if n := f.channel.BlockedReceiveCount(); n != 0 {
		t.Fatalf("cleanups 后应归零, count=%d", n)
	}
}

// Happy Path（Temporal n-future 逐个消费模式，照 :1519 futureFunc=nil + :1419 readyBranch 新局部变量）：
// 两个已 ready future；第一次 TrySelect 命中 0 → MarkConsumed(0)+Reset() → 第二次命中 1。
func TestSelector_FutureConsumed_NextSelectSkips(t *testing.T) {
	f0, f1 := NewFuture(), NewFuture()
	f0.Set("a", nil)
	f1.Set("b", nil)
	sel := NewSelector("s")
	sel.AddFuture(f0) // index 0
	sel.AddFuture(f1) // index 1

	ri, _ := sel.TrySelect()
	if ri != 0 {
		t.Fatalf("第一次应命中声明序第一个 ready future(0), got %d", ri)
	}
	sel.MarkConsumed(0)
	sel.Reset()
	ri2, _ := sel.TrySelect()
	if ri2 != 1 {
		t.Fatalf("consumed 的 case0 应被跳过、命中 1, got %d", ri2)
	}
	// 两个都消费后：第三次应无 case 可命中。
	sel.MarkConsumed(1)
	sel.Reset()
	ri3, _ := sel.TrySelect()
	if ri3 != -1 {
		t.Fatalf("全部 consumed 后应 -1, got %d", ri3)
	}
}

// Edge Case: future 与 channel 混合，声明序第一个 ready 获胜（照 :1427 声明序遍历）。
func TestSelector_MixedFutureChannel_DeclarationOrder(t *testing.T) {
	// 形态1：future(ready) 声明在 channel(有值) 之前 → 命中 future。
	f := NewFuture()
	f.Set("done", nil)
	ch := NewBufferedChannel(1)
	ch.SendAsync("v")
	sel := NewSelector("s")
	sel.AddFuture(f)   // index 0
	sel.AddReceive(ch) // index 1
	ri, _ := sel.TrySelect()
	if ri != 0 {
		t.Fatalf("future 声明在前应命中 0, got %d", ri)
	}

	// 形态2：反序 → 命中 channel。
	f2 := NewFuture()
	f2.Set("done", nil)
	ch2 := NewBufferedChannel(1)
	ch2.SendAsync("v")
	sel2 := NewSelector("s2")
	sel2.AddReceive(ch2) // index 0
	sel2.AddFuture(f2)   // index 1
	ri2, _ := sel2.TrySelect()
	if ri2 != 0 {
		t.Fatalf("channel 声明在前应命中 0, got %d", ri2)
	}
}

// Edge Case（v2 修正，照 Temporal :1472 receiveFunc=nil）: closed channel 的 receive case 命中
// （!more）后 MarkConsumed → Reset 后第二次 TrySelect 跳过它、命中另一个 ready case。
func TestSelector_ClosedReceiveConsumed_NextSelectSkips(t *testing.T) {
	chClosed := NewChannel()
	chClosed.Close()
	f := NewFuture()
	f.Set("done", nil)
	sel := NewSelector("s")
	sel.AddReceive(chClosed) // index 0：closed → 每次试探都 ready（receiveAsyncImpl :911 恒 !more）
	sel.AddFuture(f)         // index 1
	ri, _ := sel.TrySelect()
	if ri != 0 {
		t.Fatalf("closed channel case 首次应命中 0, got %d", ri)
	}
	sel.MarkConsumed(0)
	sel.Reset()
	ri2, _ := sel.TrySelect()
	if ri2 != 1 {
		t.Fatalf("consumed 的 closed-receive 应被跳过、命中 future(1), got %d", ri2)
	}
}
