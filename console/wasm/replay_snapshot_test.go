package main

import (
	"encoding/json"
	"testing"

	"github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
)

// ============================================
// snapshotValue 特判 + 深拷贝测试 (M4)
// ============================================
// 测试策略:
// - snapshotValue 必须在 json.Marshal 兜底之前特判不可序列化的值:
//   routineHandle(含 chan/sync.Once) / closure(func<>) / OutArg。
//   否则 json.Marshal 会 panic 或产生垃圾。
// - 返回的占位符必须可被 json.Marshal(时间旅行快照要能序列化到前端)。
// - 嵌套容器(map/[]any)里的 handle/closure 也要被占位,且深拷贝(各步快照独立)。
// ============================================

// mustMarshal 断言 v 可被 json.Marshal(不 panic、不报错)。
func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("snapshot result not marshalable: %v", err)
	}
	return string(b)
}

// asTypeTag 断言 v 是 map 且 __type 字段等于 want。
func asTypeTag(t *testing.T, v any, want string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected placeholder map, got %T (%v)", v, v)
	}
	if m["__type"] != want {
		t.Fatalf("expected __type=%q, got %v", want, m["__type"])
	}
	return m
}

// TestSnapshotValue_RoutineHandle
// Error Case(marshal 会炸): routineHandle 含 chan/sync.Once,直接 marshal 会失败。
// 预期: 返回 {"__type":"routineHandle"} 占位符,可被 marshal。
func TestSnapshotValue_RoutineHandle(t *testing.T) {
	v := types.NewValue(runtime.RoutineHandleType, &runtime.RoutineHandle{})
	got := snapshotValue(v)
	asTypeTag(t, got, "routineHandle")
	mustMarshal(t, got)
}

// TestSnapshotValue_Closure
// Edge Case: closure 类型名以 "func<" 前缀,函数值无法 marshal。
// 预期: 返回 {"__type":"closure","sig":<typeName>}。
func TestSnapshotValue_Closure(t *testing.T) {
	sig := "func<(int64)->int64>"
	v := types.NewValue(sig, func(int64) int64 { return 0 })
	got := snapshotValue(v)
	m := asTypeTag(t, got, "closure")
	if m["sig"] != sig {
		t.Fatalf("expected sig=%q, got %v", sig, m["sig"])
	}
	mustMarshal(t, got)
}

// TestSnapshotValue_OutArg
// Edge Case: OutArg 含 Commit 函数字段,无法 marshal。
// 预期: 返回 {"__type":"outArg",...} 占位符,可 marshal。
func TestSnapshotValue_OutArg(t *testing.T) {
	out := types.OutArg{Name: "reserve", TypeName: "map<string,any>"}
	v := types.NewValue("outArg", out)
	got := snapshotValue(v)
	asTypeTag(t, got, "outArg")
	mustMarshal(t, got)
}

// TestSnapshotValue_NestedContainerWithHandle
// Edge Case: map/[]any 里嵌 handle/closure 也要走 snapshotValue,不能直接 marshal。
// 预期: 容器被递归遍历,内层 handle 被占位,整体可 marshal。
func TestSnapshotValue_NestedContainerWithHandle(t *testing.T) {
	handle := types.NewValue(runtime.RoutineHandleType, &runtime.RoutineHandle{})
	inner := map[string]any{
		"h":     handle,
		"plain": "ok",
		"list":  []any{handle, "x"},
	}
	v := types.NewValue("map<string,any>", inner)
	got := snapshotValue(v)

	// 顶层容器序列化成功。
	mustMarshal(t, got)

	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", got)
	}
	asTypeTag(t, m["h"], "routineHandle")
	if m["plain"] != "ok" {
		t.Fatalf("plain value corrupted: %v", m["plain"])
	}
	list, ok := m["list"].([]any)
	if !ok || len(list) != 2 {
		t.Fatalf("expected list of 2, got %v", m["list"])
	}
	asTypeTag(t, list[0], "routineHandle")
}

// TestSnapshotValue_DeepCopyIndependence
// 关键正确性(时间旅行前提): 同一变量在不同步的快照必须互相独立,
// 修改后一步的底层 map 不能污染前一步已生成的快照。
func TestSnapshotValue_DeepCopyIndependence(t *testing.T) {
	shared := map[string]any{"count": float64(1), "nested": map[string]any{"x": "a"}}
	v := types.NewValue("map<string,any>", shared)

	snap1 := snapshotValue(v)

	// 模拟后续步骤原地修改底层 map。
	shared["count"] = float64(2)
	shared["nested"].(map[string]any)["x"] = "b"

	snap2 := snapshotValue(v)

	m1 := snap1.(map[string]any)
	m2 := snap2.(map[string]any)
	if m1["count"] != float64(1) {
		t.Fatalf("snap1 polluted: count=%v (want 1)", m1["count"])
	}
	if m2["count"] != float64(2) {
		t.Fatalf("snap2 stale: count=%v (want 2)", m2["count"])
	}
	n1 := m1["nested"].(map[string]any)
	n2 := m2["nested"].(map[string]any)
	if n1["x"] != "a" {
		t.Fatalf("nested snap1 polluted: x=%v (want a)", n1["x"])
	}
	if n2["x"] != "b" {
		t.Fatalf("nested snap2 stale: x=%v (want b)", n2["x"])
	}
}
