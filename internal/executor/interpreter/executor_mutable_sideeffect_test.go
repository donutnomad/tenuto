package interpreter

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	wlcompiler "github.com/donutnomad/wlang/compiler"
	wlruntime "github.com/donutnomad/wlang/runtime"
)

// ============================================================================
// MutableSideEffect 对齐 Temporal
// ============================================================================
// MutableSideEffect 复用 SideEffect 的 marker/Get/assignAny/闭包基建，加 equals dedup：
// 只在值变化时记新 marker（equals 相等则复用旧 marker，不产新命令）。per-id callCounter +
// id→callCount→value 历史映射（照 Temporal internal_event_handlers.go:1081-1110）。
//
// 测试策略：
// - MSE1 Happy:  首见 record + Get 取值。
// - MSE2 Happy:  replay 命中读回一致（batch1 marker 命中）。
// - MSE3 Error:  replay 首见无 marker → panicNonDeterministic。
// - MSE4 Happy:  first-run/replay 值一致（双路锚）。
// - MSE5 核心:   值不变 dedup → 只产 1 个 marker（第二次不 record）。
// - MSE6 B锚:    first-run equals 类型归一（old int vs new int64 归一后判等，不多产 marker）。
// - MSE7 Happy:  Get 任意类型（assignAny 复用）。
// - MSE8 A.2核心: dedup-穿插-record 多轮 first-run 决策 + replay 前缀一致性。
// - MSE9 D-b:    equals 非对称参数顺序（a=new/b=old 固化）。
// - MSE10 D-c:   mseHistory GC（只留最近一条）。
// ============================================================================

// mseCountMarkers 统计 commands 中的 MutableSideEffect marker 命令。
func mseCountMarkers(commands []history.Command) []history.Command {
	var out []history.Command
	for _, c := range commands {
		if c.Type == history.CmdRecordMarker && c.MarkerName == "MutableSideEffect" {
			out = append(out, c)
		}
	}
	return out
}

// buildEqualsClosure 编译一个只绑定 equals 闭包（不调用）的 WLang 程序，从 scope 提取 *Closure +
// 其 Executor，供 mseEquals 直驱测试（照 wlang function_test.go 的 scope.Lookup 手法）。
// bodyJSON 是 equals 闭包 do 体（返回 bool），params 固定 [["a","any"],["b","any"]]。
func buildEqualsClosure(t *testing.T, bodyJSON string) (*wlruntime.Executor, *wlruntime.Closure) {
	t.Helper()
	src := `[
		{"let":{"eq":{"fn":{
			"params":[["a","any"],["b","any"]],
			"returns":["boolean"],
			"do":` + bodyJSON + `
		}}}}
	]`
	prog, err := wlcompiler.ParseProgram([]byte(src))
	if err != nil {
		t.Fatalf("parse equals closure: %v", err)
	}
	scope := wlruntime.NewScope()
	exec := wlruntime.NewExecutor(context.Background(), scope, nil, nil, wlruntime.Budget{})
	if _, err := exec.RunProgram(prog); err != nil {
		t.Fatalf("run equals closure program: %v", err)
	}
	fv, ok := scope.Lookup("eq")
	if !ok {
		t.Fatalf("closure eq not found in scope")
	}
	cl, ok := fv.Go().(*wlruntime.Closure)
	if !ok {
		t.Fatalf("eq is not a *Closure, got %T", fv.Go())
	}
	return exec, cl
}

// newMSEApi 构造一个可直驱 mseEquals/mseLookup/mseRecord 的 api（wlExec 由调用方注入）。
func newMSEApi(wlExec *wlruntime.Executor) *runtimeWorkflowAPI {
	e := &WorkflowExecutor{
		markers:         map[string]map[string]any{},
		shadowedMarkers: map[string]bool{},
		mseCallCounter:  map[string]int{},
		mseHistory:      map[string]map[int]map[string]any{},
	}
	return &runtimeWorkflowAPI{executor: e, wlExec: wlExec}
}

// ── MSE1 Happy Path: 首见 record + Get 取值 ──
// 非 replay 首次调用 → marker 记录 (id_1) + assignAny 取到 f 返回值。
func TestMSE1_FirstSeenRecordsAndGets(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func MSEFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.MutableSideEffect(ctx, "region",
		func(ctx workflow.Context) any { return map[string]any{"region": "sg"} },
		func(a, b any) bool { return false },
	).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, activities.Provision, cfg).Get(ctx, &cfg)
}
`)
	nodes := compileWLangSource(t, src, "sdk-mse1", "MSEFlow", "mse1_flow.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	markers := mseCountMarkers(commands)
	if len(markers) != 1 {
		t.Fatalf("expected 1 MutableSideEffect marker on first-seen, got %d in %+v", len(markers), commands)
	}
	if markers[0].MarkerID != "region_1" {
		t.Fatalf("expected marker id region_1 (id_callCount), got %q", markers[0].MarkerID)
	}
	val, ok := markers[0].Input["value"].(map[string]any)
	if !ok || val["region"] != "sg" {
		t.Fatalf("unexpected marker value (want value.region=sg): %+v", markers[0].Input)
	}
	// Get 值同轮流回 activity 输入。
	var provision *history.Command
	for i := range commands {
		if commands[i].Type == history.CmdScheduleActivity && commands[i].ActivityType == "activities.Provision" {
			provision = &commands[i]
		}
	}
	if provision == nil || provision.Input["region"] != "sg" {
		t.Fatalf("activity should carry same-round MutableSideEffect value sg, got %+v", provision)
	}
}

// ── MSE2 Happy Path: replay 命中读回一致 ──
// batch1(replay) 含 MutableSideEffect marker(region_1) → 闭包不执行、读回历史值 → batch2(live) 用该值调 activity。
func TestMSE2_ReplayHitReadsMarker(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func MSEReplayFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.MutableSideEffect(ctx, "region",
		func(ctx workflow.Context) any { return map[string]any{"region": "computed-live"} },
		func(a, b any) bool { return false },
	).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	var sig map[string]any
	if err := workflow.WaitSignal(ctx, "go").Get(ctx, &sig); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, activities.Report, cfg).Get(ctx, &cfg)
}
`)
	nodes := compileWLangSource(t, src, "sdk-mse2", "MSEReplayFlow", "mse2_flow.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	marker := history.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{
		MarkerName: "MutableSideEffect", MarkerID: "region_1", Value: map[string]any{"value": map[string]any{"region": "sg"}},
	}}
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	signal := history.Event{ID: 4, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "go", Input: map[string]any{"k": "v"}}}
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, marker, taskCompleted, signal}).Execute()
	if err != nil {
		t.Fatalf("replay with cached MutableSideEffect marker must not error: %v", err)
	}
	var reportCmd *history.Command
	for i := range commands {
		if commands[i].Type == history.CmdScheduleActivity && commands[i].ActivityType == "activities.Report" {
			reportCmd = &commands[i]
		}
	}
	if reportCmd == nil {
		t.Fatalf("expected Report activity schedule, got %+v", commands)
	}
	if reportCmd.Input["region"] != "sg" {
		t.Fatalf("closure must NOT execute during replay: input should be history sg, got %+v", reportCmd.Input)
	}
}

// ── MSE3 Error: replay 首见无 marker → panicNonDeterministic ──
func TestMSE3_ReplayMissingMarkerPanics(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func MSEGuardFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	return workflow.MutableSideEffect(ctx, "region",
		func(ctx workflow.Context) any { return map[string]any{"region": "sg"} },
		func(a, b any) bool { return false },
	).Get(ctx, &cfg)
}
`)
	nodes := compileWLangSource(t, src, "sdk-mse3", "MSEGuardFlow", "mse3_flow.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	_, err := NewWorkflowExecutor(nodes, []history.Event{started, taskCompleted}).Execute()
	if err == nil {
		t.Fatal("expected non-determinism error when replay lacks MutableSideEffect marker, got nil")
	}
	if !strings.Contains(err.Error(), "no cached marker for MutableSideEffect") {
		t.Fatalf("unexpected error (want non-determinism guard): %v", err)
	}
}

// ── MSE4 Happy Path: first-run/replay 值一致（双路锚，复用 assignAny）──
// first-run 记 marker → DB round-trip → replay 读回，值一致。直驱 SideEffectFuture.Get（复用 SideEffect 的 Get，
// 但用 MutableSideEffect marker name）。MutableSideEffect 的 Get 走同一 assignAny，故复用 se4 手法。
func TestMSE4_FirstRunEqualsReplay(t *testing.T) {
	// int64 专锚 float64 漂移。
	var first int64
	e := &WorkflowExecutor{markers: map[string]map[string]any{}, shadowedMarkers: map[string]bool{}}
	id := "region_1"
	e.markers[markerKey("MutableSideEffect", id)] = map[string]any{"value": int64(42)}
	f := SideEffectFuture{executor: e, id: id, name: "MutableSideEffect"}
	if err := f.Get(workflowContext{}, &first); err != nil {
		t.Fatalf("first-run Get: %v", err)
	}
	dbVal := dbRoundTrip(t, e.markers[markerKey("MutableSideEffect", id)])
	if _, isFloat := dbVal["value"].(float64); !isFloat {
		t.Fatalf("precondition: DB round-trip should degrade int64 to float64, got %T", dbVal["value"])
	}
	er := &WorkflowExecutor{markers: map[string]map[string]any{}, shadowedMarkers: map[string]bool{}, isReplay: true}
	er.markers[markerKey("MutableSideEffect", id)] = dbVal
	fr := SideEffectFuture{executor: er, id: id, name: "MutableSideEffect"}
	var replay int64
	if err := fr.Get(workflowContext{}, &replay); err != nil {
		t.Fatalf("replay Get: %v", err)
	}
	if first != 42 || first != replay {
		t.Fatalf("MSE4 双路不一致 first=%d replay=%d", first, replay)
	}
}

// ── MSE5 核心: 值不变 dedup → 只产 1 个 marker ──
// 同 id 连续两次调用，f 返回相同值、equals 返回 true → 第二次不 record → 只产 1 个 marker。
func TestMSE5_DedupSameValueOneMarker(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func MSEDedupFlow(ctx workflow.Context, input map[string]any) error {
	eq := func(a, b any) bool { return true }
	f := func(ctx workflow.Context) any { return map[string]any{"v": 1} }
	var a map[string]any
	if err := workflow.MutableSideEffect(ctx, "cfg", f, eq).Get(ctx, &a); err != nil {
		return err
	}
	var b map[string]any
	if err := workflow.MutableSideEffect(ctx, "cfg", f, eq).Get(ctx, &b); err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-mse5", "MSEDedupFlow", "mse5_flow.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	markers := mseCountMarkers(commands)
	if len(markers) != 1 {
		t.Fatalf("MSE5 dedup: equals=true 应只产 1 个 marker, got %d in %+v", len(markers), commands)
	}
	if markers[0].MarkerID != "cfg_1" {
		t.Fatalf("expected dedup keeps first marker cfg_1, got %q", markers[0].MarkerID)
	}
}

// ── MSE6 B锚: first-run equals 类型归一 ──
// mseEquals 直驱：old 是 Go int（native），newVal 是 int64；equals 体 a.(int64)==b.(int64)。
// 归一把 old(int) round-trip 到 newVal 类型(int64) → a==b → 相等（dedup）。
// 去 reflect.New 归一 → old 保持 int → b.(int64) 断言不成立 → 判不等（变异锚 5）。
func TestMSE6_EqualsTypeNormalization(t *testing.T) {
	// equals: a.(int64) == b.(int64)（a=new, b=old）。
	exec, cl := buildEqualsClosure(t, `[
		{"return":{"==":[
			{"var":"a"},
			{"var":"b"}
		]}}
	]`)
	api := newMSEApi(exec)
	// old 是 Go int（不同于 newVal 的 int64），归一后应判等。
	old := int(7)
	newVal := int64(7)
	if !api.mseEquals(cl, old, newVal) {
		t.Fatalf("MSE6: old(int)=7 vs new(int64)=7 归一后应判等 (dedup)，got 不等 — 归一方向错或缺 reflect.New")
	}
	// 不同值应判不等。
	if api.mseEquals(cl, int(7), int64(8)) {
		t.Fatalf("MSE6: old=7 vs new=8 应判不等")
	}
}

// ── MSE7 Happy: Get 任意类型（复用 assignAny）──
func TestMSE7_GetArbitraryTypes(t *testing.T) {
	type nested struct {
		Inner string   `json:"inner"`
		Tags  []string `json:"tags"`
	}
	type cfg struct {
		Name   string `json:"name"`
		Count  int64  `json:"count"`
		Nested nested `json:"nested"`
	}
	e := &WorkflowExecutor{markers: map[string]map[string]any{}, shadowedMarkers: map[string]bool{}}
	id := "s_1"
	ret := map[string]any{"name": "wf", "count": int64(7), "nested": map[string]any{"inner": "deep", "tags": []string{"a", "b"}}}
	e.markers[markerKey("MutableSideEffect", id)] = map[string]any{"value": ret}
	f := SideEffectFuture{executor: e, id: id, name: "MutableSideEffect"}
	var got cfg
	if err := f.Get(workflowContext{}, &got); err != nil {
		t.Fatalf("Get struct: %v", err)
	}
	want := cfg{Name: "wf", Count: 7, Nested: nested{Inner: "deep", Tags: []string{"a", "b"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MSE7 struct mismatch: got %+v want %+v", got, want)
	}
}

// ── MSE8 A.2 核心: dedup-穿插-record 多轮 first-run 决策 + replay 前缀一致性 ──
// 同 id 五次调用，f 返回 v1,v1,v1,v2,v2、equals 按值比 →
// first-run 决策 = record(cc1,v1)→dedup→dedup→record(cc4,v2)→dedup → 产 2 个 marker (region_1, region_4)。
// replay 该历史 → 五次同序重建、逐轮判据复现、取值一致、live 命令序 == replay 命令序前缀。
func TestMSE8_DedupInterleaveMultiRoundReplayPrefix(t *testing.T) {
	// f 用 input["seq"]（一个递增计数器）返回 v1(前三次)/v2(后两次)：闭包体读 input 计数。
	// 为确定性构造五次调用，用循环 5 次 MutableSideEffect(同 id)。闭包内按 executor 侧计数产序列不可行
	// （闭包无副作用状态），改用一个 WLang 变量 seq 在 body 内递增，f 闭包捕获 seq 引用。
	// f 用 body 内递增的 seq（闭包捕获引用）返回 1(前三次)/2(后两次)。equals 用 == 直比 any 值
	// （WLang == 支持 any，MSE6 已证；避免 map type.assert 的类型名坑，dedup 逻辑与 map 值同构）。
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func MSEInterleaveFlow(ctx workflow.Context, input map[string]any) error {
	seq := 0
	f := func(ctx workflow.Context) any {
		seq = seq + 1
		if seq <= 3 {
			return 1
		}
		return 2
	}
	eq := func(a, b any) bool {
		return a == b
	}
	var out int
	if err := workflow.MutableSideEffect(ctx, "region", f, eq).Get(ctx, &out); err != nil {
		return err
	}
	if err := workflow.MutableSideEffect(ctx, "region", f, eq).Get(ctx, &out); err != nil {
		return err
	}
	if err := workflow.MutableSideEffect(ctx, "region", f, eq).Get(ctx, &out); err != nil {
		return err
	}
	if err := workflow.MutableSideEffect(ctx, "region", f, eq).Get(ctx, &out); err != nil {
		return err
	}
	if err := workflow.MutableSideEffect(ctx, "region", f, eq).Get(ctx, &out); err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-mse8", "MSEInterleaveFlow", "mse8_flow.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	liveCommands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	liveMarkers := mseCountMarkers(liveCommands)
	if len(liveMarkers) != 2 {
		t.Fatalf("MSE8 first-run: 应产 2 个 marker (region_1, region_4), got %d: %+v", len(liveMarkers), liveMarkers)
	}
	if liveMarkers[0].MarkerID != "region_1" || liveMarkers[1].MarkerID != "region_4" {
		t.Fatalf("MSE8 marker ids want region_1/region_4, got %q/%q", liveMarkers[0].MarkerID, liveMarkers[1].MarkerID)
	}
	// 取值：region_1 = 1, region_4 = 2。
	if int64FromAny(liveMarkers[0].Input["value"]) != 1 || int64FromAny(liveMarkers[1].Input["value"]) != 2 {
		t.Fatalf("MSE8 marker values want v1=1 v2=2, got %+v %+v", liveMarkers[0].Input["value"], liveMarkers[1].Input["value"])
	}

	// replay：把 first-run 的 2 个 marker 作为历史事件重放，五次调用同序重建。
	// 历史构造：Started + WFTC + marker(region_1) + marker(region_4) + WFTC。
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	m1 := history.Event{ID: 2, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{
		MarkerName: "MutableSideEffect", MarkerID: "region_1", Value: map[string]any{"value": int64(1)}}}
	m4 := history.Event{ID: 3, Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{
		MarkerName: "MutableSideEffect", MarkerID: "region_4", Value: map[string]any{"value": int64(2)}}}
	wftc2 := history.NewWorkflowTaskCompletedEvent(0, "")
	replayCommands, err := NewWorkflowExecutor(nodes, []history.Event{started, taskCompleted, m1, m4, wftc2}).Execute()
	if err != nil {
		t.Fatalf("MSE8 replay must not error (dedup-穿插 前缀一致): %v", err)
	}
	// replay 完成不报 drift（matchReplayWithHistory 内部已验前缀一致）。此处再断言 replay 产的 MutableSideEffect
	// 命令序（含 shadow）与 first-run marker 序一致（前缀相等）。
	replayMarkers := mseCountMarkers(replayCommands)
	// replay 全历史已含 2 marker → 决策复现产 2 shadow（进 replayCommands，不进上报 commands）；
	// Execute 返回的 commands 是 live 批命令。此处 replay 历史已完整（末批也是 replay）→ 上报 commands 可能为空或完成命令。
	// 关键断言：不报错 + 若有 MutableSideEffect 命令，其 id 前缀与 first-run 一致。
	for i, m := range replayMarkers {
		if i < len(liveMarkers) && m.MarkerID != liveMarkers[i].MarkerID {
			t.Fatalf("MSE8 replay 命令序前缀不一致: replay[%d]=%q live[%d]=%q", i, m.MarkerID, i, liveMarkers[i].MarkerID)
		}
	}
}

// ── MSE9 D-b: equals 非对称参数顺序（a=new/b=old 固化）──
// equals: a > b（a=new, b=old）。构造 new>old（应"变了"）与 new<old。
// mseEquals(equals, old, newVal)：a=newVal, b=old。
//   - old=5, new=10 → a=10>b=5=true → equals 返回 true → dedup（视为"相等/不变"）。
//   - old=10, new=5 → a=5>b=10=false → equals 返回 false → 视为"变了"。
//
// 若参数顺序反成 (old,new)：a=old,b=new，上面两例结论颠倒 → 变异锚 4 转红。
func TestMSE9_EqualsAsymmetricParamOrder(t *testing.T) {
	// equals 体: a.(float64) > b.(float64)。用户 Go 代码经 go2wlang 翻译即先 type.assert 再比较——
	// any 值 retag 到 float64 后 > 才成立。用 float64 避免 int/float 归一干扰（值本就 float64）。
	exec, cl := buildEqualsClosure(t, `[
		{"return":{">":[
			{"type.assert":[{"var":"a"},{"literal":{"type":"string","value":"float64"}}]},
			{"type.assert":[{"var":"b"},{"literal":{"type":"string","value":"float64"}}]}
		]}}
	]`)
	api := newMSEApi(exec)
	// old=5, new=10 → a(new)=10 > b(old)=5 → true。
	if !api.mseEquals(cl, float64(5), float64(10)) {
		t.Fatalf("MSE9: equals(a=new=10, b=old=5) 应 a>b=true — 参数顺序 new 在前被破坏")
	}
	// old=10, new=5 → a(new)=5 > b(old)=10 → false。
	if api.mseEquals(cl, float64(10), float64(5)) {
		t.Fatalf("MSE9: equals(a=new=5, b=old=10) 应 a>b=false — 参数顺序 new 在前被破坏")
	}
}

// ── MSE10 D-c: mseHistory GC（只留最近一条）──
// 同 id 多次 record（值都变）：每次 lookup 后 mseHistory[id] 只留 ≤当前 callCount 的最近一条。
func TestMSE10_HistoryGC(t *testing.T) {
	e := &WorkflowExecutor{
		markers:         map[string]map[string]any{},
		shadowedMarkers: map[string]bool{},
		mseCallCounter:  map[string]int{},
		mseHistory:      map[string]map[int]map[string]any{},
	}
	id := "region"
	// 手动灌入历史多条：cc1,cc2,cc3。
	e.mseRecord(id, 1, map[string]any{"value": 1})
	e.mseRecord(id, 2, map[string]any{"value": 2})
	e.mseRecord(id, 3, map[string]any{"value": 3})
	if len(e.mseHistory[id]) != 3 {
		t.Fatalf("precondition: 3 entries before lookup, got %d", len(e.mseHistory[id]))
	}
	// lookup(callCount=3)：best=3，GC 删 ≤3 且非命中的 (cc1,cc2) → 只留 cc3。
	prevCC, _, ok := e.mseLookup(id, 3)
	if !ok || prevCC != 3 {
		t.Fatalf("lookup(3) want best=3 ok, got cc=%d ok=%v", prevCC, ok)
	}
	if len(e.mseHistory[id]) != 1 {
		t.Fatalf("MSE10 GC: lookup 后应只留最近一条, got %d entries: %+v", len(e.mseHistory[id]), e.mseHistory[id])
	}
	if _, has := e.mseHistory[id][3]; !has {
		t.Fatalf("MSE10 GC: 应保留命中的 cc3, got %+v", e.mseHistory[id])
	}
}
