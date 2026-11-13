package interpreter

import (
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// WLang 层非结构化并发 wg.Go + WaitGroup 的 executor 级测试
// ============================================
// 阶段 3D-spawn（照 Temporal 移植）：workflow.Go 从结构化并发块（声明即 join、返回 []map+首错）
// 改为非结构化（起了即返回、不 join、协程逃逸），join 由 WaitGroup 显式做——
//   wg := NewWaitGroup(ctx); wg.Go(ctx, lane0); wg.Go(ctx, lane1); wg.Wait(ctx)
// lane 现在是顶层 e.sched 的 eager 协程（SpawnEager），per-lane 键仍为 lanePath#kind+局部序（守恒）。
//
// 测试策略（守住可在 spawn 编译点验证的命令级语义）：
// - Happy Path : 空历史一次 WFT，两条 lane 各调 activity → 产两条 CmdScheduleActivity（并发调度）
// - 重放确定性 : 多场景 × 多次重放，lane 调度顺序稳定 = 声明序（wg.Go 声明序 = SpawnEager 序）
// - Goexit 隔离: 一条 lane 的 activity 未 ready 触发 Goexit，另一条 lane 仍被调度
// - 多步分支  : lane 内顺序链 X1→X2、跨 WFT 逐步推进、兄弟 lane 不被阻塞、per-lane 键守恒
//
// ✅ 3D-park 已解 BLOCKER-2（照 Temporal keptBlocked/unblocked() + 检查-yield 循环移植）：
//   lane 走真 yield 冻结栈（非 Goexit），activity 未 ready 只挂本 lane；wg.Done 只在 lane 真正 return 时
//   经 defer 触发 → n 精确反映未完成 lane 数。RunTopLevel 每轮无条件 resume 所有非 done 协程（不物理排除
//   已 park 协程），靠 keptBlocked 逻辑收敛：兄弟 lane 快进 wg.Done→n=0→下一轮 main 从 wg.Wait 的检查-yield
//   循环续跑重查 n==0→放行→工作流经 Wait 收尾。原「待 3D-park」的 4 个 t.Skip 正向完成断言已全部解开并全绿。
//   命令级调度断言（哪些 activity 被调度、per-lane 键守恒）与正向完成断言（经 Wait 放行收尾）现皆绿。
// ============================================

// laneActivityFn 构造一条无返回值 lane 闭包（非结构化 wg.Go 的 lane 签名 func(ctx)，returns:[]）：
//
//	func(ctx) {
//	    let out = zero(map<string,any>)
//	    let err = workflow.ExecuteActivity(ctx, activities.<name>, <input>).Get(ctx, out)
//	}
//
// 对标 go2wlang 对内联 lane FuncLit（func(ctx){...}）的翻译产物（见 sdkgen 的 wg.Go 翻译 dump）。
func laneActivityFn(activitySymbol, input string) map[string]any {
	return map[string]any{
		"fn": map[string]any{
			"params":  []any{[]any{"ctx", "workflow.Context"}},
			"returns": []any{},
			"do": []any{
				map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
				map[string]any{"let": map[string]any{"err": map[string]any{"Get": []any{
					map[string]any{"ExecuteActivity": []any{
						map[string]any{"pkg": "workflow"},
						map[string]any{"var": "ctx"},
						map[string]any{"symbol": activitySymbol},
						map[string]any{"literal": map[string]any{"type": "string", "value": input}},
					}},
					map[string]any{"var": "ctx"},
					map[string]any{"out": "out"},
				}}}},
			},
		},
	}
}

// laneMultiStepFn 构造一条【内部顺序依赖】的多步 lane 闭包（无返回值）：
//
//	func(ctx) {
//	    let r1 = zero(map<string,any>)
//	    let r2 = zero(map<string,any>)
//	    let e1 = workflow.ExecuteActivity(ctx, X1, in1).Get(ctx, r1)  // 第一步
//	    let e2 = workflow.ExecuteActivity(ctx, X2, in2).Get(ctx, r2)  // 依赖第一步先完成
//	}
//
// X2 的 ExecuteActivity 在源码上位于 X1.Get 之后 —— 只有 X1.Get 快进（历史已完成）后
// 才会执行到 X2。这是 workflow.Go 的独特价值「多步并发分支」：lane 内是顺序链，lane 间才并行。
// 裸 future 无法表达此语义（顶层 Goexit 掀整轮，无法「只挂起一条 lane 等第一步」）。
func laneMultiStepFn(sym1, in1, sym2, in2 string) map[string]any {
	execGet := func(sym, in, out string) map[string]any {
		return map[string]any{"let": map[string]any{"e_" + out: map[string]any{"Get": []any{
			map[string]any{"ExecuteActivity": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"symbol": sym},
				map[string]any{"literal": map[string]any{"type": "string", "value": in}},
			}},
			map[string]any{"var": "ctx"},
			map[string]any{"out": out},
		}}}}
	}
	return map[string]any{
		"fn": map[string]any{
			"params":  []any{[]any{"ctx", "workflow.Context"}},
			"returns": []any{},
			"do": []any{
				map[string]any{"let": map[string]any{"r1": map[string]any{"zero": "map<string,any>"}}},
				map[string]any{"let": map[string]any{"r2": map[string]any{"zero": "map<string,any>"}}},
				execGet(sym1, in1, "r1"),
				execGet(sym2, in2, "r2"),
			},
		},
	}
}

// parallelGoProgram 构造一个工作流体用「非结构化 wg.Go + WaitGroup join」并发 lane 的 WLang 程序：
//
//	wg := workflow.NewWaitGroup(ctx)
//	wg.Go(ctx, lane0)   // 起了即返回、不 join
//	wg.Go(ctx, lane1)
//	wg.Wait(ctx)        // 显式 join（放行待 3D-park）
//
// wg.Go 声明序 = SpawnEager 序 = per-lane 键分配序（lane0=/0、lane1=/1）。
// activities 参数登记 lane 内引用的 activity 符号（对标 WorkflowConfig.Activities）。
func parallelGoProgram(activities map[string]programpkg.ActivityRef, lanes ...map[string]any) *programpkg.WorkflowConfig {
	program := []any{
		map[string]any{"let": map[string]any{"wg": map[string]any{"NewWaitGroup": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
		}}}},
	}
	for _, lane := range lanes {
		program = append(program, map[string]any{"expr": map[string]any{"Go": []any{
			map[string]any{"var": "wg"}, // 接收者：waitGroup 值 → 分派到 waitGroup.Go
			map[string]any{"var": "ctx"},
			lane,
		}}})
	}
	program = append(program, map[string]any{"expr": map[string]any{"Wait": []any{
		map[string]any{"var": "wg"},
		map[string]any{"var": "ctx"},
	}}})
	return &programpkg.WorkflowConfig{
		ID:         "parallel-wf",
		Activities: activities,
		Program:    programpkg.NewWLangProgram(program),
	}
}

func findScheduleCommands(commands []history.Command) []history.Command {
	var out []history.Command
	for _, c := range commands {
		if c.Type == history.CmdScheduleActivity {
			out = append(out, c)
		}
	}
	return out
}

// 🔴 核心 Happy Path: 工作流体用 wg.Go(laneA)/wg.Go(laneB) 起两条 lane，每条调一个 activity，
// 空历史一次 WFT → 产 [A, B] 两条 CmdScheduleActivity（一次 WFT 并发调度多 activity）。
func TestParallelGo_TwoLanes_OneWFTSchedulesBoth(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	if len(schedules) != 2 {
		t.Fatalf("expected 2 CmdScheduleActivity from one WFT, got %d: %+v", len(schedules), commands)
	}
	// lane 声明顺序 = 调度顺序：A 在 B 前。
	if schedules[0].ActivityType != "activities.A" {
		t.Fatalf("lane 0 should schedule activities.A first, got %q", schedules[0].ActivityType)
	}
	if schedules[1].ActivityType != "activities.B" {
		t.Fatalf("lane 1 should schedule activities.B second, got %q", schedules[1].ActivityType)
	}
	// per-lane 键守恒：lane0 的 A = "/0#a1"、lane1 的 B = "/1#a1"（非结构化 spawnSeq 从 0 起，
	// 与旧结构化 Go 的 lane 下标 i 一致）。
	if schedules[0].ActivityID != "/0#a1" {
		t.Fatalf("lane 0 activity stepID should be /0#a1, got %q", schedules[0].ActivityID)
	}
	if schedules[1].ActivityID != "/1#a1" {
		t.Fatalf("lane 1 activity stepID should be /1#a1, got %q", schedules[1].ActivityID)
	}
}

// 🔴 重放确定性: 多场景 × 200 次重放，产出的 CmdScheduleActivity 顺序稳定 = lane 声明顺序。
func TestParallelGo_ReplayDeterministic(t *testing.T) {
	activities := map[string]programpkg.ActivityRef{
		"activities.A": {Symbol: "activities.A", Type: "activities.A"},
		"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		"activities.C": {Symbol: "activities.C", Type: "activities.C"},
	}
	scenarios := []struct {
		name  string
		lanes []map[string]any
	}{
		{"two_lanes", []map[string]any{laneActivityFn("activities.A", "a"), laneActivityFn("activities.B", "b")}},
		{"three_lanes", []map[string]any{laneActivityFn("activities.A", "a"), laneActivityFn("activities.B", "b"), laneActivityFn("activities.C", "c")}},
		{"reordered", []map[string]any{laneActivityFn("activities.C", "c"), laneActivityFn("activities.A", "a"), laneActivityFn("activities.B", "b")}},
		{"single", []map[string]any{laneActivityFn("activities.B", "b")}},
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	for _, sc := range scenarios {
		nodes := parallelGoProgram(activities, sc.lanes...)
		var reference []string
		for i := 0; i < 200; i++ {
			commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
			if err != nil {
				t.Fatalf("[%s] replay %d: %v", sc.name, i, err)
			}
			var order []string
			for _, c := range findScheduleCommands(commands) {
				order = append(order, c.ActivityType)
			}
			if i == 0 {
				reference = order
				if len(reference) != len(sc.lanes) {
					t.Fatalf("[%s] expected %d schedules, got %v", sc.name, len(sc.lanes), reference)
				}
				continue
			}
			if len(order) != len(reference) {
				t.Fatalf("[%s] replay %d length drift: %v vs %v", sc.name, i, order, reference)
			}
			for j := range order {
				if order[j] != reference[j] {
					t.Fatalf("[%s] replay %d order drift at %d: %v vs %v", sc.name, i, j, order, reference)
				}
			}
		}
	}
}

// 🔴 Goexit 隔离: 一条 lane 的 activity 已在历史里"调度但未完成"→ 该 lane 触发 Goexit，
// 另一条 lane 的 activity（历史里完全没有）仍被调度出去。验证 SpawnEager 的 done-defer 吸收
// 在 WLang 顶层 e.sched 路径成立："一条 lane Goexit 不杀兄弟 lane"。
func TestParallelGo_LaneGoexitIsolation(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// lane 0 的 activities.A 已调度但未完成（stepID = "/0#a1"：lanePath=/0 内第 1 个 activity）。
	scheduledA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, scheduledA}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	// lane 0（A）已调度未完成 → Goexit，不再产 schedule；lane 1（B）仍被调度出去。
	if len(schedules) != 1 {
		t.Fatalf("expected only lane B scheduled (lane A already pending), got %+v", commands)
	}
	if schedules[0].ActivityType != "activities.B" {
		t.Fatalf("expected lane B (activities.B) scheduled, got %q", schedules[0].ActivityType)
	}
}

// 完成顺序无关（正向完成断言，3D-park 解 Skip）：两条 lane 各调 activity，两 activity 都完成后重放，
// 工作流应经 wg.Wait 放行收尾（keptBlocked 收敛：lane 快进 wg.Done→n=0→下轮 main 重查 Wait 放行→CmdCompleteWorkflow）。
func TestParallelGo_ResultsInDeclarationOrder(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// 终态历史：lane0 的 A（/0#a1）与 lane1 的 B（/1#a1）均已调度并完成。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	compA := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"lane": "a"}}}
	compB := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"lane": "b"}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, schedB, compA, compB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 两 lane 均完成 → 各 lane 正常 return → wg.Done×2 → n=0 → main 的 wg.Wait 放行 → 工作流收尾。
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("两 lane activity 全完成后工作流应经 wg.Wait 放行收尾（期望 CmdCompleteWorkflow），got %+v", commands)
	}
	// 收尾轮不应再产任何 activity 调度（历史已闭合）。
	if len(findScheduleCommands(commands)) != 0 {
		t.Fatalf("终态收尾不应再调度 activity，got %+v", commands)
	}
}

// Error Case（语义收窄，3D-park 解 Skip）：非结构化 WaitGroup 不聚合 lane 的 error——lane 内 activity
// 失败时 Get 返错误被 lane 内 `let err` 捕获、lane 仍正常 return（wg.Done），host【绝不】自动产 CmdFailWorkflow。
// 断言：lane0 的 A 失败、lane1 的 B 完成 → 两 lane 均结束 → n=0 → wg.Wait 放行 → 工作流经 Wait 收尾（CmdCompleteWorkflow），
// 且【无 CmdFailWorkflow】（错误不再由 WaitGroup 聚合，语义移交用户 Wait 后检查共享变量）。
func TestParallelGo_LaneError_PropagatesFirst(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// lane0 的 A（/0#a1）失败；lane1 的 B（/1#a1）完成。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	failA := history.Event{ID: 4, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom-A", Failure: &protocol.Failure{Message: "boom-A"}}}
	compB := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"lane": "b"}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, schedB, failA, compB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 语义收窄：WaitGroup 不聚合 error → 无 CmdFailWorkflow。
	for _, c := range commands {
		if c.Type == history.CmdFailWorkflow {
			t.Fatalf("非结构化 WaitGroup 不应聚合 lane error 产 CmdFailWorkflow，got %+v", commands)
		}
	}
	// 两 lane 均结束（lane0 捕获错误后正常 return，lane1 正常）→ n=0 → wg.Wait 放行 → 工作流收尾。
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("lane 失败被捕获、两 lane 均结束后应经 wg.Wait 放行收尾（期望 CmdCompleteWorkflow），got %+v", commands)
	}
}

// Edge Case（语义收窄，3D-park 解 Skip）：声明顺序首错在非结构化下不再由 host 聚合——两条 lane 各自
// 的 activity 都失败（A、B 皆 fail），host【绝不】按声明序挑首错产 CmdFailWorkflow；两 lane 捕获各自错误
// 后正常 return → n=0 → wg.Wait 放行 → 工作流收尾。首错检查语义移交用户代码（Wait 后检查共享变量）。
func TestParallelGo_FirstErrorByLaneOrder(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// lane0 的 A 与 lane1 的 B 都失败。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	failA := history.Event{ID: 4, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom-A", Failure: &protocol.Failure{Message: "boom-A"}}}
	failB := history.Event{ID: 5, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 3, Error: "boom-B", Failure: &protocol.Failure{Message: "boom-B"}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, schedB, failA, failB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 语义收窄：不按声明序挑首错聚合 → 无 CmdFailWorkflow。
	for _, c := range commands {
		if c.Type == history.CmdFailWorkflow {
			t.Fatalf("非结构化 WaitGroup 不应按声明序挑首错产 CmdFailWorkflow，got %+v", commands)
		}
	}
	// 两 lane 各自捕获错误后正常 return → n=0 → wg.Wait 放行 → 工作流收尾。
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("两 lane 皆失败但均被捕获、结束后应经 wg.Wait 放行收尾（期望 CmdCompleteWorkflow），got %+v", commands)
	}
}

// handler 内并发: 命令式 update handler 闭包体内写 wg.Go(laneA)/wg.Go(laneB)。
// drain 调 InvokeClosure 执行 handler → handler 内的 wg.Go host 起顶层 e.sched eager lane →
// 一次 WFT 产多条 CmdScheduleActivity。验证 handler 内非结构化并发的命令级调度成立。
func TestParallelGo_HandlerInternalConcurrency(t *testing.T) {
	// handler 闭包体：
	//   wg := workflow.NewWaitGroup(ctx)
	//   wg.Go(ctx, laneA); wg.Go(ctx, laneB); wg.Wait(ctx)
	//   return {}   // handler 单返回值；本轮 lane 的 activity 尚未 ready
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"let": map[string]any{"wg": map[string]any{"NewWaitGroup": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
				}}}},
				map[string]any{"expr": map[string]any{"Go": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
					laneActivityFn("activities.A", "a"),
				}}},
				map[string]any{"expr": map[string]any{"Go": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
					laneActivityFn("activities.B", "b"),
				}}},
				map[string]any{"expr": map[string]any{"Wait": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
				}}},
				handlerReturn(map[string]any{"map": map[string]any{
					"type":    []any{"string", "any"},
					"entries": []any{},
				}}),
			},
		},
	}
	nodes := &programpkg.WorkflowConfig{
		ID: "handler-parallel",
		Activities: map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		Program: programpkg.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "parallel"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "parallel", "u-1", map[string]any{})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// drain 进入 handler → handler 内 wg.Go 起两条 lane → 一次 WFT 产两条 CmdScheduleActivity。
	schedules := findScheduleCommands(commands)
	if len(schedules) != 2 {
		t.Fatalf("expected 2 schedules from handler-internal concurrency, got %d: %+v", len(schedules), commands)
	}
	if schedules[0].ActivityType != "activities.A" || schedules[1].ActivityType != "activities.B" {
		t.Fatalf("expected lane order A,B, got %q,%q", schedules[0].ActivityType, schedules[1].ActivityType)
	}
	// lane 的 activity 未 ready → lane Goexit → 本轮 update 不 complete（下轮 activity 就绪后再 drain）。
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("update must not complete while lane activities pending, got %+v", commands)
	}
}

// handler 内并发第二轮完成：
// handler 协程 lanePath=update:u-1，其内 wg.Go 两 lane 的 activity 键组合为 update:u-1/0#a1、
// update:u-1/1#a1（专属桶 + lane 下标，不再混 main root 桶）。喂完整历史后两 lane 快进 →
// wg.Wait 放行 → handler return {} → 协程产 CmdCompleteWorkflowUpdate（live 批上报）。
func TestParallelGo_HandlerInternalConcurrency_SecondRoundCompletes(t *testing.T) {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"let": map[string]any{"wg": map[string]any{"NewWaitGroup": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
				}}}},
				map[string]any{"expr": map[string]any{"Go": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
					laneActivityFn("activities.A", "a"),
				}}},
				map[string]any{"expr": map[string]any{"Go": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
					laneActivityFn("activities.B", "b"),
				}}},
				map[string]any{"expr": map[string]any{"Wait": []any{
					map[string]any{"var": "wg"},
					map[string]any{"var": "ctx"},
				}}},
				handlerReturn(map[string]any{"map": map[string]any{
					"type":    []any{"string", "any"},
					"entries": []any{},
				}}),
			},
		},
	}
	nodes := &programpkg.WorkflowConfig{
		ID: "handler-parallel",
		Activities: map[string]programpkg.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		},
		Program: programpkg.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "parallel"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "parallel", "u-1", map[string]any{})
	// 完整历史：handler 内两 lane 的 activity 均已调度并完成——键在 update:u-1 专属桶
	// （handler 协程 lanePath=update:u-1 → lane 组合键 update:u-1/0#a1、update:u-1/1#a1）。
	schedA := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "update:u-1/0#a1", ActivityType: "activities.A"}}
	schedB := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "update:u-1/1#a1", ActivityType: "activities.B"}}
	compA := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"lane": "a"}}}
	compB := history.Event{ID: 6, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"lane": "b"}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, schedA, schedB, compA, compB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 两 lane 快进完成 → handler 内 wg.Wait 放行 → handler return → 协程产 update 完成命令。
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 {
		t.Fatalf("handler 内两 lane 全完成后 wg.Wait 应放行、update 应完成（期望 1 条 CmdCompleteWorkflowUpdate），got %+v", commands)
	}
	if completes[0].UpdateID != "u-1" {
		t.Fatalf("expected update u-1 completed, got %q", completes[0].UpdateID)
	}
	// 收尾轮不再产 activity 调度（历史已闭合）。
	if len(findScheduleCommands(commands)) != 0 {
		t.Fatalf("update 完成收尾不应再调度 activity，got %+v", commands)
	}
}

// ============================================
// 多步并发分支安全网（证明 workflow.Go 的 Scheduler 不可砍）
// ============================================
// 阶段2 决策B 的核心论证：future（activity 级并发）与 Scheduler（协程级并发）是正交层次。
// lane 内多步依赖链（Exec(X1).Get(); Exec(X2).Get()）是 workflow.Go 独有的「多步并发分支」
// 语义 —— 裸 future 无法表达（顶层 Goexit 掀整轮，做不到「只挂起一条 lane 等第一步」）。
// 以下测试锁定该语义的【命令级调度】部分（哪些 activity 被调度、per-lane 键守恒），本 spawn 编译点即绿；
// 不含「工作流经 Wait 收尾完成」的正向断言（那依赖 Wait 放行，待 3D-park）。
// ============================================

// 🔴 核心：lane 内第二个 activity 必须等第一个先完成 —— 空历史一次 WFT 只调度 X1，X2 不产命令。
// 证明 lane 内是【顺序依赖链】而非并发：X1.Get 未 ready → 该 lane Goexit（被 SpawnEager done-defer 吸收），
// 源码上位于其后的 X2.ExecuteActivity 根本没机会执行。这是 Scheduler「只挂本 lane」能力的直接体现。
func TestParallelGo_LaneMultiStep_SecondActivityWaitsForFirst(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.X1": {Symbol: "activities.X1", Type: "activities.X1"},
			"activities.X2": {Symbol: "activities.X2", Type: "activities.X2"},
		},
		laneMultiStepFn("activities.X1", "i1", "activities.X2", "i2"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	// 只有 X1 被调度；X2 在 X1 未完成前不产命令（顺序依赖链）。
	if len(schedules) != 1 {
		t.Fatalf("multi-step lane: only first activity should schedule before it completes, got %d: %+v", len(schedules), commands)
	}
	if schedules[0].ActivityType != "activities.X1" {
		t.Fatalf("expected X1 scheduled first, got %q", schedules[0].ActivityType)
	}
	if schedules[0].ActivityID != "/0#a1" {
		t.Fatalf("expected X1 stepID /0#a1, got %q", schedules[0].ActivityID)
	}
	// 有 pending activity → 不误判完成（hasSchedule=true 阻止误判，与 WaitGroup 的 n 无关）。
	if hasCompleteWorkflow(commands) {
		t.Fatalf("must not complete while X1 pending, got %+v", commands)
	}
}

// 🔴 多步链跨 WFT 逐步推进：X1 完成后重放 → X1.Get 快进拿结果 → X2 才被调度（键 /0#a2）。
// 证明 Scheduler 驱动 lane 在多个 WFT 间沿依赖链前进；per-lane 键 /0#a2 守恒（不挤占兄弟 lane 键）。
func TestParallelGo_LaneMultiStep_ProgressesAcrossWFTs(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.X1": {Symbol: "activities.X1", Type: "activities.X1"},
			"activities.X2": {Symbol: "activities.X2", Type: "activities.X2"},
		},
		laneMultiStepFn("activities.X1", "i1", "activities.X2", "i2"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// X1 已调度并完成（stepID = "/0#a1"：lanePath=/0 内第 1 个 activity）。
	schedX1 := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	compX1 := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"r1": int64(1)}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedX1, compX1}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	// X1 已完成 → X1.Get 快进 → 执行到 X2.ExecuteActivity → 只产 X2 一条调度。
	if len(schedules) != 1 {
		t.Fatalf("after X1 done, lane should advance and schedule X2 only, got %d: %+v", len(schedules), commands)
	}
	if schedules[0].ActivityType != "activities.X2" {
		t.Fatalf("expected X2 scheduled after X1 completes, got %q", schedules[0].ActivityType)
	}
	if schedules[0].ActivityID != "/0#a2" {
		t.Fatalf("expected X2 stepID /0#a2 (per-lane key 守恒), got %q", schedules[0].ActivityID)
	}
}

// 🔴 多步分支与 fan-out lane 混合并行：lane0 = X1→X2 顺序链、lane1 = 单 Y。
// 空历史一次 WFT → lane0 推进到 X1（第一步）、lane1 调 Y，共 [X1, Y] 两条调度；X2 等 X1。
// 证明「多步分支 lane」与「fan-out lane」可共存，兄弟 lane 不被彼此的依赖链阻塞。
func TestParallelGo_MultiStepLane_ParallelWithSingleLane(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.X1": {Symbol: "activities.X1", Type: "activities.X1"},
			"activities.X2": {Symbol: "activities.X2", Type: "activities.X2"},
			"activities.Y":  {Symbol: "activities.Y", Type: "activities.Y"},
		},
		laneMultiStepFn("activities.X1", "i1", "activities.X2", "i2"),
		laneActivityFn("activities.Y", "y"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	// lane0 推进到 X1（X2 等 X1）、lane1 调 Y → 两条调度，声明顺序 X1 在前、Y 在后。
	if len(schedules) != 2 {
		t.Fatalf("expected 2 schedules (X1 from lane0 first step + Y from lane1), got %d: %+v", len(schedules), commands)
	}
	if schedules[0].ActivityType != "activities.X1" || schedules[1].ActivityType != "activities.Y" {
		t.Fatalf("expected [X1, Y] by lane declaration order, got [%q, %q]", schedules[0].ActivityType, schedules[1].ActivityType)
	}
	// per-lane 键守恒：X1 = /0#a1、Y = /1#a1。
	if schedules[0].ActivityID != "/0#a1" || schedules[1].ActivityID != "/1#a1" {
		t.Fatalf("expected keys [/0#a1, /1#a1], got [%q, %q]", schedules[0].ActivityID, schedules[1].ActivityID)
	}
	// X2 未被调度（等 X1）。
	for _, s := range schedules {
		if s.ActivityType == "activities.X2" {
			t.Fatalf("X2 must not schedule before X1 completes, got %+v", commands)
		}
	}
}

// 🔴 P0 回归守护：多步 lane(X1→X2) + fan-out lane(Y) 跨 WFT 推进时，兄弟 lane 的 Y 不得被重复调度。
// per-lane 结构化键把序号按 lanePath+kind 分桶：lane0 的 X1=/0#a1、X2=/0#a2，lane1 的 Y=/1#a1。
// 多步 lane 第二轮推进执行 X2 只消耗 lane0 自己的 /0#a2，不再挤占 Y 的键 → Y 稳定命中历史 /1#a1，
// 不被重复调度（exactly-once 得以保持）。这是 lane 内步骤 ID 稳定性要锁定的行为。
func TestParallelGo_MultiStepLane_SiblingNotRescheduledAcrossWFTs(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]programpkg.ActivityRef{
			"activities.X1": {Symbol: "activities.X1", Type: "activities.X1"},
			"activities.X2": {Symbol: "activities.X2", Type: "activities.X2"},
			"activities.Y":  {Symbol: "activities.Y", Type: "activities.Y"},
		},
		laneMultiStepFn("activities.X1", "i1", "activities.X2", "i2"),
		laneActivityFn("activities.Y", "y"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// WFT1 产出 /0#a1（X1）、/1#a1（Y）；此处喂 X1 已完成、Y 仍在飞（stepID 按 WFT1 真实产出）。
	schedX1 := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1"}}
	schedY := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1"}}
	compX1 := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"r1": int64(1)}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedX1, schedY, compX1}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 期望：只调度 X2（lane0 推进第二步）；Y 已在飞，不得重复调度。
	for _, s := range findScheduleCommands(commands) {
		if s.ActivityType == "activities.Y" {
			t.Fatalf("Y 被重复调度 (stepID=%s)，历史里 Y 已是 /1#a1 在飞 —— exactly-once 被破坏", s.ActivityID)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
