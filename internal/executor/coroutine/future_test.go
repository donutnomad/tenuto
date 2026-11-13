package coroutine

import "testing"

// ============================================
// Future（照 Temporal futureImpl :54-60 移植）单测
// ============================================
// 测试策略:
// - Happy Path: Set 后 GetAsync 当场 ready（T1）
// - Happy Path: 未 Set 时 GetAsync 登记 callback，Remove 归零，后续 Set 不 panic（T2）
// - Error Case: 二次 Set panic "already set"（T3，照 :423-425）
// ============================================

// Happy Path: Set 后 GetAsync → (v, true, err)；IsReady=true。
func TestFuture_SetThenGetAsync_Ready(t *testing.T) {
	f := NewFuture()
	if f.IsReady() {
		t.Fatalf("新建 future 不应 ready")
	}
	f.Set(map[string]any{"k": "v"}, nil)
	if !f.IsReady() {
		t.Fatalf("Set 后应 ready")
	}
	cb := &receiveCallback{fn: func(v any, more bool) bool { return true }}
	v, ok, err := f.GetAsync(cb)
	if !ok {
		t.Fatalf("已 Set 的 future GetAsync 应 ok=true")
	}
	if err != nil {
		t.Fatalf("成功 Set 的 future err 应为 nil, got %v", err)
	}
	m, _ := v.(map[string]any)
	if m["k"] != "v" {
		t.Fatalf("GetAsync 应返回 Set 的值, got %v", v)
	}
	val, gerr := f.GetValueAndError()
	if gerr != nil || val.(map[string]any)["k"] != "v" {
		t.Fatalf("GetValueAndError 应返回 Set 的三元组, got %v %v", val, gerr)
	}
}

// Happy Path: 未 Set 时 GetAsync ok=false 且 callback 登记进 future channel 的 blockedReceives；
// RemoveReceiveCallback 后归零；随后 Set（Close 触发空 callback 表）不 panic。
func TestFuture_GetAsync_NotReady_RegistersCallback(t *testing.T) {
	f := NewFuture()
	cb := &receiveCallback{fn: func(v any, more bool) bool { return true }}
	_, ok, _ := f.GetAsync(cb)
	if ok {
		t.Fatalf("未 Set 的 future GetAsync 应 ok=false")
	}
	if n := f.channel.BlockedReceiveCount(); n != 1 {
		t.Fatalf("callback 应登记进 blockedReceives, count=%d want 1", n)
	}
	f.RemoveReceiveCallback(cb)
	if n := f.channel.BlockedReceiveCount(); n != 0 {
		t.Fatalf("Remove 后 blockedReceives 应归零, count=%d", n)
	}
	f.Set(nil, nil) // Close 遍历空 callback 表，不应 panic
	if !f.IsReady() {
		t.Fatalf("Set 后应 ready")
	}
}

// Error Case: 二次 Set panic（照 Temporal :423-425 "already set"）。
func TestFuture_SetTwice_Panics(t *testing.T) {
	f := NewFuture()
	f.Set("first", nil)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("二次 Set 应 panic")
		}
	}()
	f.Set("second", nil)
}
