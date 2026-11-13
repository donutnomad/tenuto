package interpreter

import (
	"testing"
)

// ============================================
// coroCtx（lanePath 随 workflow.Context 携带）针对性守恒测试
// ============================================
// 背景：
// 子步 3b 把共享单字段 curLane 改为随 workflow.Context 逐协程携带（对标 Temporal 每协程独立 ctx，
// NewCoroutine :346）。本组测试直接在 laneKey 层验证「lanePath 来源是 ctx 参数、不是 api 共享状态」，
// 从而证明「ctx 携带比单字段更安全」——为 3D 给 lane 加 park/resume 铺网。
//
// 决定性理由：3D 一旦让 lane 在 ActivityFuture.Get 未 ready 时 park（冻结栈），共享单字段 curLane 会在
// 交错 park/resume 时串味（A park→B 入口改字段→A resume 读到 B 的字段），生成错键 /1#a2 而非 /0#a2，
// 不 panic 的静默错键。改 ctx 携带后 lanePath 随栈上 ctx 冻结/恢复，交错不串。
// ============================================

// laneCtx 构造携带指定 lanePath 的 workflow.Context（root 传 ""）。
func laneCtx(lanePath string) workflowContext {
	return workflowContext{lanePath: lanePath}
}

// Happy Path: laneKey 的 lanePath 部分完全取自 ctx，不读 api 任何共享字段。
// root ctx→ "#a1"；/0 ctx→ "/0#a1"；/1 ctx→ "/1#a1"（三个 ctx 各自独立命名空间）。
func TestLaneCtx_LaneKeyReadsPathFromCtx(t *testing.T) {
	api := &runtimeWorkflowAPI{}

	if got := api.laneKey(laneCtx(""), "a"); got != "#a1" {
		t.Fatalf("root ctx first activity: want #a1, got %q", got)
	}
	if got := api.laneKey(laneCtx("/0"), "a"); got != "/0#a1" {
		t.Fatalf("/0 ctx first activity: want /0#a1, got %q", got)
	}
	if got := api.laneKey(laneCtx("/1"), "a"); got != "/1#a1" {
		t.Fatalf("/1 ctx first activity: want /1#a1, got %q", got)
	}
}

// 🔴 核心守恒: 两条 lane 交错调 laneKey（A→B→A→B，模拟 3D 的 park/resume 切换），
// A 恒携带 /0、B 恒携带 /1——即使共享 api 不再有单字段 curLane。
//
// 这是「ctx 携带比单字段更安全」的直接证据：若仍是共享单字段 curLane，B 入口设 curLane=/1 后，
// A resume 时读到的 curLane 是 /1，A 的第二个 activity 会错生成 /1#a2（串味）。改 ctx 携带后，
// A 的第二次调用传入的仍是 A 自己的 /0 ctx，稳定得 /0#a2。计数器共享在 api 且按 lanePath\x00kind
// 分桶，故 A 的 a 计数（1→2）与 B 的 a 计数（1→2）互不干扰。
func TestLaneCtx_InterleavedLanesDoNotCrossContaminate(t *testing.T) {
	api := &runtimeWorkflowAPI{}

	ctxA := laneCtx("/0") // lane A 的协程作用域 ctx（随栈携带，模拟 park/resume 后恢复）
	ctxB := laneCtx("/1") // lane B 的协程作用域 ctx

	// 交错序：A 第一步 → B 第一步 → A 第二步 → B 第二步。
	// 模拟 3D：A park 让位 B，B park 让位 A resume……ctx 随各自栈走，互不覆盖。
	a1 := api.laneKey(ctxA, "a")
	b1 := api.laneKey(ctxB, "a")
	a2 := api.laneKey(ctxA, "a")
	b2 := api.laneKey(ctxB, "a")

	if a1 != "/0#a1" {
		t.Fatalf("lane A step1: want /0#a1, got %q", a1)
	}
	if b1 != "/1#a1" {
		t.Fatalf("lane B step1: want /1#a1, got %q", b1)
	}
	// 关键断言：A resume 后第二个 activity 仍是 /0#a2（不被 B 的 /1 串味）。
	if a2 != "/0#a2" {
		t.Fatalf("lane A step2 after interleave: want /0#a2 (ctx-carried, no cross-contamination), got %q", a2)
	}
	if b2 != "/1#a2" {
		t.Fatalf("lane B step2 after interleave: want /1#a2, got %q", b2)
	}
}

// Edge Case: 同一 lanePath 下不同 kind 各自独立命名空间（按 lanePath\x00kind 分桶），
// 交错也不互挤——lane /0 的 activity 计数与 timer/child/random 计数分开，与旧五游标语义一致。
func TestLaneCtx_KindBucketsIndependentPerLane(t *testing.T) {
	api := &runtimeWorkflowAPI{}
	ctx := laneCtx("/0")

	if got := api.laneKey(ctx, "a"); got != "/0#a1" {
		t.Fatalf("want /0#a1, got %q", got)
	}
	if got := api.laneKey(ctx, "t"); got != "/0#t1" { // timer 命名空间独立，不受前面 activity 影响
		t.Fatalf("want /0#t1, got %q", got)
	}
	if got := api.laneKey(ctx, "a"); got != "/0#a2" { // activity 继续自增
		t.Fatalf("want /0#a2, got %q", got)
	}
	if got := api.laneKey(ctx, "c"); got != "/0#c1" {
		t.Fatalf("want /0#c1, got %q", got)
	}
}

// Happy Path: 嵌套 Go 的 lanePath 派生——从 /0 的 ctx 再 withLanePath 得 /0/0（对标 Temporal
// 子协程从父协程 ctx 派生）。守 Go host 的 childLane = parentLane(ctx)+"/"+i 计算在嵌套下正确。
func TestLaneCtx_NestedDerivation(t *testing.T) {
	root := workflowContext{}
	if got := root.lanePath; got != "" {
		t.Fatalf("root lanePath: want empty, got %q", got)
	}

	// 顶层 Go 派生第 0 条 lane：/0。
	lane0 := root.withLanePath(root.lanePath + "/0")
	if got := lane0.lanePath; got != "/0" {
		t.Fatalf("lane0 lanePath: want /0, got %q", got)
	}

	// 嵌套 Go 在 lane0 内派生第 0 条子 lane：/0/0。
	nested := lane0.withLanePath(lane0.lanePath + "/0")
	if got := nested.lanePath; got != "/0/0" {
		t.Fatalf("nested lanePath: want /0/0, got %q", got)
	}

	// 用嵌套 ctx 生成键：/0/0#a1（守嵌套 per-lane 键公式）。
	api := &runtimeWorkflowAPI{}
	if got := api.laneKey(nested, "a"); got != "/0/0#a1" {
		t.Fatalf("nested laneKey: want /0/0#a1, got %q", got)
	}
}
