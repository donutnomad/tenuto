package coroutine

import "testing"

// ============================================
// Channel 纯 async 交付原语测试（照 Temporal channelImpl，internal_workflow.go:784-1010）
// ============================================
// 测试策略（不涉 executor / yield / park，纯数据结构 + 交付算法）：
// - Happy Path: unbuffered send→receive 直接交付（blockedReceive 先登记，send 触发 callback）
// - Happy Path: buffered channel 多次 send 入 buffer，receive 按 FIFO 取
// - Edge Case: 手动设 recValue，receiveAsyncImpl 优先取它（prefetch）
// - Edge Case: buffer 满时 send 登记 blockedSend，receive 取 buffer[0] 后把 blockedSend 移入 buffer
// - Edge Case: Close 唤醒 blockedReceives；close 后 receive 返回 (nil,false,false)；send on closed panic
// ============================================

// TestChannel_Unbuffered_SendReceiveDelivery
// Happy Path（对应 Temporal receiveAsyncImpl :922-924 登记 callback + sendAsyncImpl :983-991 触发）：
// 非缓冲 channel 上，先 receive 登记 blockedReceive（无值 → more=true），
// 后 send 经 sendAsyncImpl 逐个触发 blockedReceives 的 fn，直接把值交付给等待的 receiver。
func TestChannel_Unbuffered_SendReceiveDelivery(t *testing.T) {
	c := NewChannel()

	var got any
	var gotMore bool
	delivered := false
	cb := &receiveCallback{
		fn: func(v any, more bool) bool {
			got = v
			gotMore = more
			delivered = true
			return true
		},
	}

	// receive 先来：无值可取 → 登记 blockedReceive，返回 (nil,false,true)
	v, ok, more := c.receiveAsyncImpl(cb)
	if v != nil || ok || !more {
		t.Fatalf("receiveAsyncImpl before send: want (nil,false,true), got (%v,%v,%v)", v, ok, more)
	}
	if len(c.blockedReceives) != 1 {
		t.Fatalf("want 1 blockedReceive registered, got %d", len(c.blockedReceives))
	}

	// send 来：sendAsyncImpl 触发 blockedReceive 的 callback，直接交付
	if ok := c.sendAsyncImpl("hello", nil); !ok {
		t.Fatal("sendAsyncImpl to waiting receiver: want ok=true")
	}
	if !delivered {
		t.Fatal("callback should have been invoked (send did not wake blocked receive)")
	}
	if got != "hello" || !gotMore {
		t.Fatalf("delivered value: want (hello,true), got (%v,%v)", got, gotMore)
	}
	if len(c.blockedReceives) != 0 {
		t.Fatalf("blockedReceives should be drained, got %d", len(c.blockedReceives))
	}
}

// TestChannel_Buffered_FIFO
// Happy Path（对应 Temporal sendAsyncImpl 入 buffer :992-995 + receiveAsyncImpl 取 buffer[0] :893-909）：
// 缓冲 channel 依次 send 入 buffer，receive 按先进先出顺序取出。
func TestChannel_Buffered_FIFO(t *testing.T) {
	c := NewBufferedChannel(3)

	for _, v := range []int{1, 2, 3} {
		if ok := c.SendAsync(v); !ok {
			t.Fatalf("SendAsync(%d) into buffer: want ok=true", v)
		}
	}
	if got := c.Len(); got != 3 {
		t.Fatalf("Len after 3 sends: want 3, got %d", got)
	}

	for _, want := range []int{1, 2, 3} {
		v, ok, more := c.ReceiveAsync()
		if !ok || !more {
			t.Fatalf("ReceiveAsync: want (ok=true,more=true), got (%v,%v)", ok, more)
		}
		if v != want {
			t.Fatalf("FIFO order broken: want %d, got %v", want, v)
		}
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after draining: want 0, got %d", got)
	}
}

// TestChannel_Prefetch_RecValue
// Edge Case（对应 Temporal receiveAsyncImpl 优先取 recValue :888-892）：
// 手动设置 recValue（模拟预取），receiveAsyncImpl 必须优先返回它，而非 buffer 中的值。
func TestChannel_Prefetch_RecValue(t *testing.T) {
	c := NewBufferedChannel(2)
	// buffer 里放一个占位值，用于验证 recValue 优先级高于 buffer
	c.SendAsync("from-buffer")

	pre := any("prefetched")
	c.recValue = &pre

	v, ok, more := c.receiveAsyncImpl(nil)
	if !ok || !more {
		t.Fatalf("receiveAsyncImpl with recValue: want (ok=true,more=true), got (%v,%v)", ok, more)
	}
	if v != "prefetched" {
		t.Fatalf("recValue must take priority over buffer: want prefetched, got %v", v)
	}
	if c.recValue != nil {
		t.Fatal("recValue must be cleared after being consumed")
	}
	// 下一次取才轮到 buffer 里的值
	v2, _, _ := c.receiveAsyncImpl(nil)
	if v2 != "from-buffer" {
		t.Fatalf("after recValue consumed, buffer value expected: want from-buffer, got %v", v2)
	}
}

// TestChannel_BlockedSend_MovedIntoBuffer
// Edge Case（对应 Temporal receiveAsyncImpl :899-907 把一个 blockedSend 移入 buffer）：
// buffer 满时，send 登记为 blockedSend（返回 false）；receive 取走 buffer[0] 后，
// 把一个 blockedSend 的值移入 buffer 末尾，其 fn 被触发。
func TestChannel_BlockedSend_MovedIntoBuffer(t *testing.T) {
	c := NewBufferedChannel(1)

	// 填满 buffer
	if ok := c.SendAsync("A"); !ok {
		t.Fatal("first send fills buffer: want ok=true")
	}

	// buffer 已满，带 pair 的 send 登记为 blockedSend，返回 false
	consumed := false
	pair := &sendCallback{
		value: "B",
		fn: func() bool {
			consumed = true
			return true
		},
	}
	if ok := c.sendAsyncImpl("B", pair); ok {
		t.Fatal("send when buffer full: want ok=false (blocked)")
	}
	if len(c.blockedSends) != 1 {
		t.Fatalf("want 1 blockedSend, got %d", len(c.blockedSends))
	}

	// receive 取走 buffer[0]=A，随后把 blockedSend "B" 移入 buffer
	v, ok, more := c.receiveAsyncImpl(nil)
	if !ok || !more || v != "A" {
		t.Fatalf("receive buffer[0]: want (A,true,true), got (%v,%v,%v)", v, ok, more)
	}
	if !consumed {
		t.Fatal("blockedSend fn should have been invoked when moved into buffer")
	}
	if len(c.blockedSends) != 0 {
		t.Fatalf("blockedSends should be drained, got %d", len(c.blockedSends))
	}
	// B 现在在 buffer 里，下一次 receive 取到它
	v2, ok2, _ := c.receiveAsyncImpl(nil)
	if !ok2 || v2 != "B" {
		t.Fatalf("moved-in value: want B, got %v (ok=%v)", v2, ok2)
	}
}

// TestChannel_Close_WakesBlockedReceives
// Edge Case（对应 Temporal Close :1002-1010）：
// Close 对 blockedReceives 副本逐个 fn(nil,false)，唤醒它们（more=false）。
func TestChannel_Close_WakesBlockedReceives(t *testing.T) {
	c := NewChannel()

	woken := false
	var moreAtWake bool = true
	cb := &receiveCallback{
		fn: func(v any, more bool) bool {
			woken = true
			moreAtWake = more
			return true
		},
	}
	c.receiveAsyncImpl(cb) // 登记 blockedReceive

	c.Close()

	if !woken {
		t.Fatal("Close must invoke blocked receive callback")
	}
	if moreAtWake {
		t.Fatal("Close must wake blocked receive with more=false")
	}
	if !c.closed {
		t.Fatal("channel should be marked closed")
	}
}

// TestChannel_Receive_OnClosedEmpty
// Edge Case（对应 Temporal receiveAsyncImpl closed 分支 :911-912）：
// 已关闭且无值的 channel，receiveAsyncImpl 返回 (nil,false,false)。
func TestChannel_Receive_OnClosedEmpty(t *testing.T) {
	c := NewChannel()
	c.Close()

	v, ok, more := c.receiveAsyncImpl(nil)
	if v != nil || ok || more {
		t.Fatalf("receive on closed&empty: want (nil,false,false), got (%v,%v,%v)", v, ok, more)
	}
}

// TestChannel_Send_OnClosedPanics
// Edge Case（对应 Temporal sendAsyncImpl closed 分支 :980-982）：
// 向已关闭 channel 发送必须 panic("Closed channel")。
func TestChannel_Send_OnClosedPanics(t *testing.T) {
	c := NewChannel()
	c.Close()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("send on closed channel must panic")
		}
		if s, _ := r.(string); s != "Closed channel" {
			t.Fatalf("panic message: want \"Closed channel\", got %v", r)
		}
	}()
	c.SendAsync("x")
}

// TestChannel_Len_CountsRecValueAndBlockedSends
// Edge Case（对应 Temporal Len :877-883）：
// Len = len(buffer) + len(blockedSends)，若有 recValue 再 +1。
func TestChannel_Len_CountsRecValueAndBlockedSends(t *testing.T) {
	c := NewBufferedChannel(1)
	c.SendAsync("A") // buffer: [A]

	// 再 send 一个，buffer 满 → 登记 blockedSend
	pair := &sendCallback{value: "B", fn: func() bool { return true }}
	c.sendAsyncImpl("B", pair)

	// 此时 buffer=1, blockedSends=1, recValue=nil → Len=2
	if got := c.Len(); got != 2 {
		t.Fatalf("Len with buffer+blockedSends: want 2, got %d", got)
	}

	// 加上 recValue → Len=3
	pre := any("pre")
	c.recValue = &pre
	if got := c.Len(); got != 3 {
		t.Fatalf("Len including recValue: want 3, got %d", got)
	}
}

// TestChannel_RemoveCallbacks
// Edge Case（对应 Temporal removeReceiveCallback :928-935 / removeSendCallback :937-944）：
// 从 blocked 队列中按指针移除指定 callback。
func TestChannel_RemoveCallbacks(t *testing.T) {
	c := NewChannel()

	rcb := &receiveCallback{fn: func(any, bool) bool { return true }}
	c.receiveAsyncImpl(rcb)
	if len(c.blockedReceives) != 1 {
		t.Fatalf("want 1 blockedReceive, got %d", len(c.blockedReceives))
	}
	c.removeReceiveCallback(rcb)
	if len(c.blockedReceives) != 0 {
		t.Fatalf("removeReceiveCallback failed, got %d", len(c.blockedReceives))
	}

	cb := NewBufferedChannel(0)
	scb := &sendCallback{value: "x", fn: func() bool { return true }}
	cb.sendAsyncImpl("x", scb) // buffer 满(size=0)且无 receiver → 登记 blockedSend
	if len(cb.blockedSends) != 1 {
		t.Fatalf("want 1 blockedSend, got %d", len(cb.blockedSends))
	}
	cb.removeSendCallback(scb)
	if len(cb.blockedSends) != 0 {
		t.Fatalf("removeSendCallback failed, got %d", len(cb.blockedSends))
	}
}

// TestChannel_ExportedCallbackPrimitives
// Happy Path（导出封装层，供 engine host 异包构造 callback + 无 yield async 交付）：
// NewReceiveCallback 登记 blockedReceive；NewSendCallback + SendWithCallback 触发 receiver 的
// onDelivered 回调（send 唤醒 blocked receive 的核心，照 Temporal sendAsyncImpl :983-991）。
func TestChannel_ExportedCallbackPrimitives(t *testing.T) {
	c := NewChannel()

	var gotV any
	var gotMore bool
	delivered := false
	rcb := NewReceiveCallback(func(v any, more bool) {
		gotV = v
		gotMore = more
		delivered = true
	})
	// receive 先来：无值 → 登记 blockedReceive，返回 (nil,false,true)
	v, ok, more := c.ReceiveWithCallback(rcb)
	if v != nil || ok || !more {
		t.Fatalf("ReceiveWithCallback before send: want (nil,false,true), got (%v,%v,%v)", v, ok, more)
	}

	// send 来：SendWithCallback 触发 receiver 的 onDelivered，consumed=true
	consumed := false
	scb := NewSendCallback("payload", func() { consumed = true })
	if ok := c.SendWithCallback("payload", scb); !ok {
		t.Fatal("SendWithCallback to waiting receiver: want consumed=true")
	}
	if !delivered {
		t.Fatal("onDelivered should fire (send did not wake blocked receive)")
	}
	if gotV != "payload" || !gotMore {
		t.Fatalf("delivered: want (payload,true), got (%v,%v)", gotV, gotMore)
	}
	_ = consumed // send 直接交付给 receiver（未走 blockedSend 路径），consumed 由 receiver 侧完成
}

// TestChannel_Exported_ClosedAndSendWithCallbackBlocked
// Edge Case（导出封装层）：unbuffered channel 无 receiver 时 SendWithCallback 登记 blockedSend
// 返回 false（consumed=false）；Closed() 报告关闭状态（供 host Send 循环内查 :967-970）。
func TestChannel_Exported_ClosedAndSendWithCallbackBlocked(t *testing.T) {
	c := NewChannel()
	if c.Closed() {
		t.Fatal("new channel: Closed() want false")
	}
	scb := NewSendCallback("x", func() {})
	if ok := c.SendWithCallback("x", scb); ok {
		t.Fatal("SendWithCallback with no receiver, buffer full: want consumed=false (blocked)")
	}
	c.Close()
	if !c.Closed() {
		t.Fatal("after Close: Closed() want true")
	}
}
