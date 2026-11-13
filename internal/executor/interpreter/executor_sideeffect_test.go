package interpreter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// ============================================================================
// SideEffect 闭包对齐 Temporal
// ============================================================================
// SideEffect 从「传值 marker」升级为「传闭包」：first-run 执行闭包记 marker、replay 跳过读 marker。
// id 自动分配（laneKey "se"），record 时机从 Get 前移到 SideEffect()。Get 支持任意返回类型（assignAny
// JSON round-trip 归一 first-run 原生值与 replay 的 DB-round-trip 值到 *T）。
//
// 测试策略：
// - SE1 Happy Path:   闭包 first-run 执行 → marker 记录 + Get 取到值。
// - SE2 Edge:         replay 跳过闭包（闭包不执行）读 marker，值从历史。
// - SE3 Error:        replay 缺 marker → panicNonDeterministic。
// - SE4 Happy Path（核心·双路一致性锚）: 四类目标 first-run Get 值 == replay Get 值。
// - SE5 Edge:         多次 SideEffect 各自 laneKey（#se1/#se2 不串）+ replay 一致。
// - SE7 Edge:         replay 比对/shadow 不误报。
// - 大整数死角:       >2^53 int64 first-run vs replay 边界固化。
// ============================================================================

func compileWLangSource(t *testing.T, src []byte, workflowID, funcName, filename string) *program.WorkflowConfig {
	t.Helper()
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{
		WorkflowID: workflowID,
		FuncName:   funcName,
		Filename:   filename,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := result.Workflow
	return nodes
}

// dbRoundTrip 模拟 gormstore marshalMap/unmarshalMap 的 marker Value DB round-trip（JSON 编解码），
// 令原生 Go 值退化为 JSON 泛型（int64→float64、[]string→[]any、map→map[string]any）——replay 路径的坑。
func dbRoundTrip(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	blob, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("dbRoundTrip marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("dbRoundTrip unmarshal: %v", err)
	}
	return out
}

// SE1 Happy Path: 首个 WFT（isReplay=false）闭包 first-run 执行 → 记 marker + 同轮 Get 取到值继续执行。
// 观测手段：SideEffect 后紧跟一个 ExecuteActivity，其输入引用 SideEffect 返回值。若闭包执行且值同轮流回，
// 则本轮同时产 marker 命令 + 用该值的调度命令（marker id 自动为 #se1）。
func TestSideEffect_SE1_ClosureFirstRunExecutesRecordsAndGets(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func SideEffectFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "sg"}
	}).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	err = workflow.ExecuteActivity(ctx, activities.Provision, cfg).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-sideeffect", "SideEffectFlow", "sideeffect_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 同轮既产 SideEffect marker，又产用该值的 activity 调度 → 闭包执行 + 值同轮流回 body。
	if len(commands) != 2 {
		t.Fatalf("expected 2 commands (marker + schedule) in a single WFT, got %+v", commands)
	}
	if commands[0].Type != historypkg.CmdRecordMarker || commands[0].MarkerName != "SideEffect" || commands[0].MarkerID != "#se1" {
		t.Fatalf("unexpected first command (want SideEffect marker id=#se1): %+v", commands[0])
	}
	// marker 值包在 {"value": ...} 里（对齐 RandomUUID 包装）。
	val, ok := commands[0].Input["value"].(map[string]any)
	if !ok || val["region"] != "sg" {
		t.Fatalf("unexpected side effect marker input (want value.region=sg): %+v", commands[0].Input)
	}
	if commands[1].Type != historypkg.CmdScheduleActivity || commands[1].ActivityType != "activities.Provision" {
		t.Fatalf("unexpected second command (want Provision schedule): %+v", commands[1])
	}
	if commands[1].Input["region"] != "sg" {
		t.Fatalf("activity input should carry same-round SideEffect closure value, got %+v", commands[1].Input)
	}
}

// SE2 Edge: 历史含 SideEffect marker（batch1 replay）→ 闭包不执行、读回历史值 → batch2(live) 用该值调
// activity。观测手段：闭包 return "computed-live"（与历史 sg 不同）。SideEffect 在 batch1（replay，marker
// 命中）被跳过闭包；随后 WaitSignal park 结束 batch1；batch2 signal 放行 → Report activity live 调度（进
// 返回 commands），其输入 = Get 读回值。断言输入 = 历史 sg（非 computed-live）→ 证明闭包未执行、值来自历史。
func TestSideEffect_SE2_ReplaySkipsClosureReadsMarker(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func SideEffectReplayFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "computed-live"}
	}).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	var sig map[string]any
	if err := workflow.WaitSignal(ctx, "go").Get(ctx, &sig); err != nil {
		return err
	}
	err = workflow.ExecuteActivity(ctx, activities.Report, cfg).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-sideeffect-replay", "SideEffectReplayFlow", "sideeffect_replay_flow.go")

	// batch1(replay): Started + SideEffect marker(region=sg) + WFTC → SideEffect 命中 marker、闭包不执行、
	// WaitSignal park。batch2(live): signal → Report activity live 调度（用 Get 读回值）。
	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	marker := historypkg.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{
		MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"region": "sg"}},
	}}
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	signal := historypkg.Event{ID: 4, Timestamp: time.Now(), WorkflowSignaled: &historypkg.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"k": "v"}}}
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, marker, taskCompleted, signal}).Execute()
	if err != nil {
		t.Fatalf("replay with cached SideEffect marker must not error: %v", err)
	}
	var reportCmd *historypkg.Command
	for i := range commands {
		if commands[i].Type == historypkg.CmdScheduleActivity && commands[i].ActivityType == "activities.Report" {
			reportCmd = &commands[i]
		}
	}
	if reportCmd == nil {
		t.Fatalf("expected Report activity schedule (live command in batch2), got %+v", commands)
	}
	// activity 输入 = Get 读回值。闭包未执行 → 值来自历史 sg（非 computed-live）。
	if reportCmd.Input["region"] != "sg" {
		t.Fatalf("closure must NOT execute during replay: activity input should be history value sg, got %+v", reportCmd.Input)
	}
}

// SE3 Error（变异守护）: replay（history 含 WFTC → isReplay=true）缺 SideEffect marker →
// panicNonDeterministic → Execute 返回 error。守护 isReplay 分支 load-bearing。
func TestSideEffect_SE3_ReplayMissingMarkerPanicsNonDeterministic(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func SideEffectGuardFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "sg"}
	}).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-sideeffect-guard", "SideEffectGuardFlow", "sideeffect_guard_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted}).Execute()
	if err == nil {
		t.Fatal("expected non-determinism error when replay lacks SideEffect marker, got nil")
	}
	if !strings.Contains(err.Error(), "no cached marker for SideEffect") {
		t.Fatalf("unexpected error (want non-determinism guard): %v", err)
	}
}

// ── SE4 双路一致性锚 helper：直接驱动 host SideEffect + Get，不经 sdkgen（隔离类型转换机制）──

// se4FirstRun 用 first-run 语义（isReplay=false）执行一个返回 retVal 的闭包，产 marker，然后 Get 到 out。
// 返回产出的 marker Value（供 replay 路径复用）。这里用宿主直调而非 WLang 源码，聚焦 assignAny 类型归一。
func se4RunClosure(t *testing.T, retVal any, out any) map[string]any {
	t.Helper()
	e := &WorkflowExecutor{markers: map[string]map[string]any{}, shadowedMarkers: map[string]bool{}}
	id := "#se1"
	// first-run 记 marker（模拟 SideEffect() host 首见分支：out.Go() 存进 marker）。
	e.markers[markerKey("SideEffect", id)] = map[string]any{"value": retVal}
	f := SideEffectFuture{executor: e, id: id}
	if err := f.Get(workflowContext{}, out); err != nil {
		t.Fatalf("first-run Get: %v", err)
	}
	return e.markers[markerKey("SideEffect", id)]
}

// se4Replay 用 replay 语义读回一个（经 DB round-trip 的）marker Value 到 out。
func se4Replay(t *testing.T, markerVal map[string]any, out any) {
	t.Helper()
	e := &WorkflowExecutor{markers: map[string]map[string]any{}, shadowedMarkers: map[string]bool{}, isReplay: true}
	id := "#se1"
	e.markers[markerKey("SideEffect", id)] = markerVal
	f := SideEffectFuture{executor: e, id: id}
	if err := f.Get(workflowContext{}, out); err != nil {
		t.Fatalf("replay Get: %v", err)
	}
}

type se4Nested struct {
	Inner string   `json:"inner"`
	Tags  []string `json:"tags"`
}

type se4Struct struct {
	Name   string    `json:"name"`
	Count  int64     `json:"count"`
	Nested se4Nested `json:"nested"`
}

// SE4 Happy Path: 四类目标 first-run Get 值 == replay Get 值。
// replay 路径经 marker DB round-trip（int64→float64 等）+ assignAny JSON round-trip，才是坑真正生效处。
// 变异自查：若 Get 改成 reflect.Set 直赋（不走 JSON），int64/[]string/struct 的 replay 路径会转红。
func TestSideEffect_SE4_FirstRunEqualsReplayAcrossTypes(t *testing.T) {
	// ── 类型 1：*int64（专锚 float64 漂移）──
	t.Run("int64", func(t *testing.T) {
		var first int64
		markerVal := se4RunClosure(t, int64(42), &first)
		dbVal := dbRoundTrip(t, markerVal) // 模拟 DB：value 变 float64(42)
		if _, isFloat := dbVal["value"].(float64); !isFloat {
			t.Fatalf("precondition: DB round-trip should degrade int64 to float64, got %T", dbVal["value"])
		}
		var replay int64
		se4Replay(t, dbVal, &replay)
		if first != int64(42) {
			t.Fatalf("first-run int64 want 42, got %d", first)
		}
		if first != replay {
			t.Fatalf("SE4 int64 双路不一致：first=%d replay=%d（float64 漂移未被 JSON round-trip 修掉）", first, replay)
		}
	})

	// ── 类型 2：*se4Struct（嵌套）──
	t.Run("nested-struct", func(t *testing.T) {
		ret := map[string]any{
			"name":  "wf",
			"count": int64(7),
			"nested": map[string]any{
				"inner": "deep",
				"tags":  []string{"a", "b"},
			},
		}
		var first se4Struct
		markerVal := se4RunClosure(t, ret, &first)
		dbVal := dbRoundTrip(t, markerVal)
		var replay se4Struct
		se4Replay(t, dbVal, &replay)
		want := se4Struct{Name: "wf", Count: 7, Nested: se4Nested{Inner: "deep", Tags: []string{"a", "b"}}}
		if !reflect.DeepEqual(first, want) {
			t.Fatalf("first-run struct mismatch: got %+v want %+v", first, want)
		}
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("SE4 nested-struct 双路不一致：first=%+v replay=%+v", first, replay)
		}
	})

	// ── 类型 3：*[]string ──
	t.Run("slice-string", func(t *testing.T) {
		var first []string
		markerVal := se4RunClosure(t, []string{"x", "y", "z"}, &first)
		dbVal := dbRoundTrip(t, markerVal) // value 变 []any
		var replay []string
		se4Replay(t, dbVal, &replay)
		if !reflect.DeepEqual(first, []string{"x", "y", "z"}) {
			t.Fatalf("first-run slice mismatch: %+v", first)
		}
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("SE4 []string 双路不一致：first=%+v replay=%+v", first, replay)
		}
	})

	// ── 类型 4：map[string]any（out 为 *map[string]any）──
	t.Run("map-any", func(t *testing.T) {
		ret := map[string]any{"k1": "v1", "n": int64(3)}
		var first map[string]any
		markerVal := se4RunClosure(t, ret, &first)
		dbVal := dbRoundTrip(t, markerVal)
		var replay map[string]any
		se4Replay(t, dbVal, &replay)
		// map[string]any 两路都是 JSON 泛型（string / float64），只要两路彼此相等即可（用户目标本就是泛型）。
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("SE4 map[string]any 双路不一致：first=%+v replay=%+v", first, replay)
		}
		if first["k1"] != "v1" {
			t.Fatalf("map value lost: %+v", first)
		}
	})
}

// 大整数死角: >2^53 int64 经 DB JSON round-trip 精度丢失，first-run 原生值 ≠ replay
// 解码值——JSON round-trip 也救不回（精度已在 DB 层丢）。本测试固化这个已知边界（断言限制存在）。
func TestSideEffect_BigInteger_PrecisionDeadZone(t *testing.T) {
	const bigInt = int64(9007199254740993) // 2^53 + 1，float64 无法精确表示
	var first int64
	markerVal := se4RunClosure(t, bigInt, &first)
	if first != bigInt {
		t.Fatalf("first-run should hold native int64 exactly, got %d", first)
	}
	dbVal := dbRoundTrip(t, markerVal) // DB JSON round-trip：精度在此丢失
	var replay int64
	se4Replay(t, dbVal, &replay)
	// 固化死角：大整数经 DB round-trip 后 first-run != replay（限制存在）。
	if first == replay {
		t.Fatalf("BigInteger dead-zone unexpectedly consistent (first=%d replay=%d): "+
			"若精度不再丢失，更新 SideEffect 文档并放开大整数限制", first, replay)
	}
	t.Logf("BigInteger dead-zone confirmed: first=%d replay=%d (precision lost via DB JSON round-trip; use string for big ints)", first, replay)
}

// SE5 Edge: 同 lane 两次 SideEffect → id #se1/#se2 各自 marker，不串；replay 各自读回一致。
func TestSideEffect_SE5_MultipleSideEffectsDistinctLaneKeys(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TwoSideEffectFlow(ctx workflow.Context, input map[string]any) error {
	var a map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"v": 1}
	}).Get(ctx, &a); err != nil {
		return err
	}
	var b map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"v": 2}
	}).Get(ctx, &b); err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-two-sideeffect", "TwoSideEffectFlow", "two_sideeffect_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	var seMarkers []historypkg.Command
	for _, c := range commands {
		if c.Type == historypkg.CmdRecordMarker && c.MarkerName == "SideEffect" {
			seMarkers = append(seMarkers, c)
		}
	}
	if len(seMarkers) != 2 {
		t.Fatalf("expected 2 distinct SideEffect markers, got %d in %+v", len(seMarkers), commands)
	}
	if seMarkers[0].MarkerID != "#se1" || seMarkers[1].MarkerID != "#se2" {
		t.Fatalf("expected marker ids #se1/#se2 (laneKey auto), got %q/%q", seMarkers[0].MarkerID, seMarkers[1].MarkerID)
	}
	v0, _ := seMarkers[0].Input["value"].(map[string]any)
	v1, _ := seMarkers[1].Input["value"].(map[string]any)
	if int64FromAny(v0["v"]) != 1 || int64FromAny(v1["v"]) != 2 {
		t.Fatalf("markers crossed: se1=%+v se2=%+v", v0, v1)
	}

	// replay：两个 marker 都在历史 → 闭包不执行、各自读回，Execute 完成不报错。
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	m1 := historypkg.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"v": 1}}}}
	m2 := historypkg.Event{ID: 3, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se2", Value: map[string]any{"value": map[string]any{"v": 2}}}}
	if _, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, m1, m2}).Execute(); err != nil {
		t.Fatalf("replay with both markers must not error: %v", err)
	}
}

// SE7 Edge: 多批历史带 SideEffect marker → matchReplayWithHistory 不报 missing/extra（shadow 配对成立）。
// 通过一个跨批工作流（WaitSignal 分批 + SideEffect）验证 replay 比对不误报。
func TestSideEffect_SE7_ReplayMatchNoFalsePositive(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func ReplayMatchSideEffectFlow(ctx workflow.Context, input map[string]any) error {
	var sig map[string]any
	if err := workflow.WaitSignal(ctx, "go").Get(ctx, &sig); err != nil {
		return err
	}
	var cfg map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "sg"}
	}).Get(ctx, &cfg); err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-sideeffect-match", "ReplayMatchSideEffectFlow", "sideeffect_match_flow.go")

	// 批1: Started + WFTC；批2(live): signal → SideEffect 首见记 marker。先跑 live 产 marker。
	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	signal := historypkg.Event{ID: 3, Timestamp: time.Now(), WorkflowSignaled: &historypkg.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"k": "v"}}}
	// 现在完整 replay：批1 replay + 批2 有历史 SideEffect marker → 比对不应误报。
	marker := historypkg.Event{ID: 4, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"region": "sg"}}}}
	wftc2 := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, signal, marker, wftc2}).Execute()
	if err != nil {
		t.Fatalf("replay match with SideEffect marker must not report drift: %v", err)
	}
}

// Happy Path: R类原语首见于【live 批】（末批）→
// isReplay=false → 合法记新 marker，不触发非确定性护栏。历史 [Started, WFTC₁][signal] 双批：批1 无 signal →
// main park 于 WaitSignal.Get（SideEffect 调用点未到达）；批2（live）signal 放行 → SideEffect 首见 → 记新
// marker（照 Temporal :986-993 的 live 分支）。SideEffect 已迁移为闭包形态。
func TestSideEffectFirstSeenInLiveBatch_RecordsNewMarker(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func LiveBatchSideEffectFlow(ctx workflow.Context, input map[string]any) error {
	var sig map[string]any
	if err := workflow.WaitSignal(ctx, "go").Get(ctx, &sig); err != nil {
		return err
	}
	var cfg map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"region": "sg"}
	}).Get(ctx, &cfg); err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-sideeffect-live-batch", "LiveBatchSideEffectFlow", "sideeffect_live_batch_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	signal := historypkg.Event{ID: 3, Timestamp: time.Now(), WorkflowSignaled: &historypkg.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"k": "v"}}}
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, signal}).Execute()
	if err != nil {
		t.Fatalf("SideEffect first seen in live batch must record new marker, not panic: %v", err)
	}
	found := false
	for _, c := range commands {
		if c.Type == historypkg.CmdRecordMarker && c.MarkerName == "SideEffect" && c.MarkerID == "#se1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected new SideEffect marker command in live batch, got %+v", commands)
	}
}

// ============================================================================
// GetVersion 测试（与 SideEffect 闭包无关，但用 SideEffect 闭包作观测载体固化 version 值）
// ============================================================================

// Happy Path: replay 缺 GetVersion marker 且支持范围包含 -1 时，返回 DefaultVersion。
func TestGetVersionReplayMissingMarkerReturnsDefaultVersion(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionGuardFlow(ctx workflow.Context, input map[string]any) error {
	version := workflow.GetVersion(ctx, "change-a", -1, 3)
	var s map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"version": version}
	}).Get(ctx, &s)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-guard", "VersionGuardFlow", "version_guard_flow.go")

	// history 含 WFTC（isReplay=true）+ SideEffect marker（id=#se1，laneKey 自动）→ SideEffect 不触发护栏；
	// 【无】GetVersion marker → GetVersion 走 DefaultVersion=-1，不 panic。
	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	verMarker := historypkg.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "#se1", Value: map[string]any{"value": map[string]any{"version": DefaultVersion}}}}
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, verMarker}).Execute()
	if err != nil {
		t.Fatalf("GetVersion replay missing marker must not panic (DefaultVersion path), got err: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("expected workflow completion (GetVersion→DefaultVersion, SideEffect marker hit), got %+v", commands)
	}
}

// 第 9 项：作者用 workflow.DefaultVersion 写支持范围和分支判断；没有 marker 的历史走 DefaultVersion 分支。
func TestGetVersionWithSDKDefaultVersionConstant(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Out struct {
	Branch  string
	Version int
}

func VersionConstFlow(ctx workflow.Context, input map[string]any) (Out, error) {
	version := workflow.GetVersion(ctx, "change-a", workflow.DefaultVersion, 3)
	if version == workflow.DefaultVersion {
		return Out{Branch: "legacy", Version: version}, nil
	}
	return Out{Branch: "current", Version: version}, nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-const", "VersionConstFlow", "version_const_flow.go")
	completed := func(events []historypkg.Event) map[string]any {
		t.Helper()
		commands, err := NewWorkflowExecutor(nodes, events).Execute()
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		for _, command := range commands {
			if command.Type == historypkg.CmdCompleteWorkflow {
				return command.Output
			}
		}
		t.Fatalf("workflow did not complete: %+v", commands)
		return nil
	}
	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	if got := completed([]historypkg.Event{started}); got["Branch"] != "current" || fmt.Sprint(got["Version"]) != "3" {
		t.Fatalf("new run output = %#v, want current/3", got)
	}
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	if got := completed([]historypkg.Event{started, taskCompleted}); got["Branch"] != "legacy" || fmt.Sprint(got["Version"]) != "-1" {
		t.Fatalf("marker-less history output = %#v, want legacy/-1", got)
	}
}

// Error Case: 旧历史没有 marker 且支持范围排除 DefaultVersion 时，重放报告非确定性错误。
func TestGetVersionReplayMissingMarkerRejectsUnsupportedDefault(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionRejectFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "change-a", 1, 3)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-reject-default", "VersionRejectFlow", "version_reject_flow.go")
	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted}).Execute()
	if err == nil {
		t.Fatal("expected non-determinism error when DefaultVersion is below minSupported")
	}
	if !strings.Contains(err.Error(), "change-a") || !strings.Contains(err.Error(), "oldest supported version is 1") {
		t.Fatalf("expected version range error for change-a, got: %v", err)
	}
}

const getVersionTwiceSource = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TwiceFlow(ctx workflow.Context, input map[string]any) error {
	first := workflow.GetVersion(ctx, "change-a", -1, 1)
	var one map[string]any
	if err := workflow.WaitSignal(ctx, "one").Get(ctx, &one); err != nil {
		return err
	}
	second := workflow.GetVersion(ctx, "change-a", -1, 1)
	var two map[string]any
	if err := workflow.WaitSignal(ctx, "two").Get(ctx, &two); err != nil {
		return err
	}
	var s map[string]any
	return workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"first": first, "second": second}
	}).Get(ctx, &s)
}
`

func assertGetVersionTwiceValues(t *testing.T, commands []historypkg.Command, first, second int64) {
	t.Helper()
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 1 || commands[0].MarkerName != "SideEffect" {
		t.Fatalf("expected only the observing SideEffect marker, got %+v", commands)
	}
	value, _ := commands[0].Input["value"].(map[string]any)
	if int64FromAny(value["first"]) != first || int64FromAny(value["second"]) != second {
		t.Fatalf("GetVersion values = %+v, want first=%d second=%d", value, first, second)
	}
}

// Edge Case: 照 Temporal changeVersions，replay 首见的 DefaultVersion 对同一 Run 的后续 live 调用生效。
func TestGetVersionReplayDefaultAppliesToLaterLiveCall(t *testing.T) {
	nodes := compileWLangSource(t, []byte(getVersionTwiceSource), "sdk-version-twice", "TwiceFlow", "version_twice_flow.go")
	history := []historypkg.Event{
		historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
		historypkg.NewWorkflowTaskCompletedEvent(0, ""),
		historypkg.NewWorkflowSignaledEvent("one", nil),
		historypkg.NewWorkflowSignaledEvent("two", nil),
	}
	commands, err := NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatal(err)
	}
	assertGetVersionTwiceValues(t, commands, DefaultVersion, DefaultVersion)
}

// Edge Case: 修复前写入的 History 含“先 -1、后 marker”序列，重放时 marker 优先，结果保持稳定。
func TestGetVersionHistoryMarkerOverridesReplayDefault(t *testing.T) {
	nodes := compileWLangSource(t, []byte(getVersionTwiceSource), "sdk-version-twice-marker", "TwiceFlow", "version_twice_flow.go")
	history := []historypkg.Event{
		historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
		historypkg.NewWorkflowTaskCompletedEvent(0, ""),
		historypkg.NewWorkflowSignaledEvent("one", nil),
		historypkg.NewWorkflowTaskCompletedEvent(0, ""),
		historypkg.NewMarkerRecordedEvent("GetVersion", "change-a", map[string]any{"version": int64(1)}),
		historypkg.NewWorkflowSignaledEvent("two", nil),
	}
	commands, err := NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatal(err)
	}
	assertGetVersionTwiceValues(t, commands, DefaultVersion, 1)
}

// Happy Path: live 首见写入本次版本，并携带 Run 已确定的全部版本（含 DefaultVersion）。
func TestGetVersionLiveUpsertsAccumulatedChangeVersions(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func AccumulateFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "old-change", -1, 1)
	var sig map[string]any
	if err := workflow.WaitSignal(ctx, "go").Get(ctx, &sig); err != nil {
		return err
	}
	_ = workflow.GetVersion(ctx, "new-change", -1, 2)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-accumulate", "AccumulateFlow", "version_accumulate_flow.go")
	history := []historypkg.Event{
		historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
		historypkg.NewWorkflowTaskCompletedEvent(0, ""),
		historypkg.NewWorkflowSignaledEvent("go", nil),
	}
	commands, err := NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatal(err)
	}
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 2 || commands[0].MarkerID != "new-change" || commands[1].Type != historypkg.CmdUpsertSearchAttributes {
		t.Fatalf("expected version marker then search attribute upsert, got %+v", commands)
	}
	if got := commands[1].Input["TemporalChangeVersion"]; got != `["new-change-2","old-change--1"]` {
		t.Fatalf("unexpected change versions: %v", got)
	}
}

// Edge Case: 属性值达到 2048 字节时跳过写入，marker 继续保证版本语义。
func TestGetVersionSkipsOversizedChangeVersionAttribute(t *testing.T) {
	changeID := strings.Repeat("c", 2048)
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func OversizeFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "` + changeID + `", -1, 1)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-oversize", "OversizeFlow", "version_oversize_flow.go")
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 1 || commands[0].MarkerName != "GetVersion" {
		t.Fatalf("expected only the version marker, got %+v", commands)
	}
}

// Error Case: 照 Temporal，maxSupported < minSupported 经支持范围校验报告非确定性错误。
func TestGetVersionMaxBelowMinIsNonDeterministic(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func InvertedFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "change-a", 5, 3)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-inverted", "InvertedFlow", "version_inverted_flow.go")
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)}).Execute()
	if err == nil || !strings.Contains(err.Error(), "removed support of version 3") {
		t.Fatalf("expected version range error, got: %v", err)
	}
}

// Happy Path: 首个 WFT GetVersion 首见（isReplay=false）→ maxSupported=3 + 记 marker 与版本索引 + 同步返回 3。
func TestGetVersionFirstWFTComputesAndReturnsSameRound(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionFirstFlow(ctx workflow.Context, input map[string]any) error {
	version := workflow.GetVersion(ctx, "change-a", 1, 3)
	var s map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return map[string]any{"version": version}
	}).Get(ctx, &s)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-first", "VersionFirstFlow", "version_first_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []historypkg.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 同轮依次产 GetVersion marker、版本索引和 SideEffect marker（version 值同轮流回 body），main 返回后完成。
	commands = commandsBeforeCompletion(t, commands)
	if len(commands) != 3 {
		t.Fatalf("expected marker, upsert and marker in a single WFT, got %+v", commands)
	}
	if commands[0].MarkerName != "GetVersion" || int64FromAny(commands[0].Input["version"]) != 3 {
		t.Fatalf("unexpected version marker (want version=3): %+v", commands[0])
	}
	if commands[1].Type != historypkg.CmdUpsertSearchAttributes || commands[1].Input["TemporalChangeVersion"] != `["change-a-3"]` {
		t.Fatalf("unexpected change version upsert: %+v", commands[1])
	}
	if commands[2].MarkerName != "SideEffect" || commands[2].MarkerID != "#se1" {
		t.Fatalf("unexpected side effect marker: %+v", commands[2])
	}
	// SideEffect 的输入 version = GetVersion 同轮返回的 3，证明值同轮流回 body。
	val, _ := commands[2].Input["value"].(map[string]any)
	if int64FromAny(val["version"]) != 3 {
		t.Fatalf("SideEffect should carry same-round GetVersion value 3, got %+v", commands[2].Input)
	}
}

// Error Case（变异守护，照 Temporal validateVersion :908-919）: replay 命中已存 version marker 时
// 校验 min/max 边界——用户提高 minSupported 后，老 run 旧 version marker 越界 → panicNonDeterministic。
func TestGetVersionMarkerHitBelowMinPanicsNonDeterministic(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionBoundFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "change-a", 2, 3)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-bound", "VersionBoundFlow", "version_bound_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	verMarker := historypkg.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-a", Value: map[string]any{"version": int64(1)}}}
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, verMarker}).Execute()
	if err == nil {
		t.Fatal("expected non-determinism error when marker version below minSupported, got nil")
	}
	if !strings.Contains(err.Error(), "change-a") {
		t.Fatalf("unexpected error (want version boundary guard for change-a): %v", err)
	}
}

// Error Case（变异守护，照 Temporal validateVersion :914-918）: replay 命中的 version marker 高于
// maxSupported（version=5 > 现 maxSupported=3）→ panicNonDeterministic → error。
func TestGetVersionMarkerHitAboveMaxPanicsNonDeterministic(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func VersionBoundHiFlow(ctx workflow.Context, input map[string]any) error {
	_ = workflow.GetVersion(ctx, "change-a", 1, 3)
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-version-bound-hi", "VersionBoundHiFlow", "version_bound_hi_flow.go")

	started := historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	verMarker := historypkg.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &historypkg.MarkerRecordedEvent{MarkerName: "GetVersion", MarkerID: "change-a", Value: map[string]any{"version": int64(5)}}}
	_, err := NewWorkflowExecutor(nodes, []historypkg.Event{started, taskCompleted, verMarker}).Execute()
	if err == nil {
		t.Fatal("expected non-determinism error when marker version above maxSupported, got nil")
	}
	if !strings.Contains(err.Error(), "change-a") {
		t.Fatalf("unexpected error (want version boundary guard for change-a): %v", err)
	}
}
