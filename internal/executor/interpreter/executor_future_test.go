package interpreter

import (
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// 真 Future（方案A）executor 级安全网测试
// ============================================
// 测试策略：
// - Happy Path : f1:=Exec(A); f2:=Exec(B); f1.Get; f2.Get —— 空历史一次 WFT 产 [A,B] 两条
//   CmdScheduleActivity（ExecuteActivity 立即调度 = 并发发起）。
// - 独立等待   : A 完成 B 未完成 → f1.Get 返 A 结果、f2.Get Goexit 等（各自按需等待）。
// - 幂等       : 历史含 #a1 scheduled → 重放不重复产 #a1 调度命令。
// - 重放确定性 : 多场景 × 200 次重放，调度命令序列稳定 = 声明顺序。
// - 收敛      : main 仍在 Get 上等待时不完成；fire-and-forget（main 已返回）即完成。
// ============================================

// futureExecCall 构造一条 `let f = workflow.ExecuteActivity(ctx, <symbol>, <input>)` 语句
// （对标 sdkgen 双 future 端到端翻译产物：ExecuteActivity 变量赋值不带 .Get()）。
func futureExecCall(varName, activitySymbol, input string) map[string]any {
	return map[string]any{"let": map[string]any{varName: map[string]any{
		"ExecuteActivity": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
			map[string]any{"symbol": activitySymbol},
			map[string]any{"literal": map[string]any{"type": "string", "value": input}},
		},
	}}}
}

// futureGetCall 构造一条 `let e = f.Get(ctx, out)` 语句（对标翻译产物的 Get 调用）。
func futureGetCall(errVar, futureVar, outVar string) map[string]any {
	return map[string]any{"let": map[string]any{errVar: map[string]any{
		"Get": []any{
			map[string]any{"var": futureVar},
			map[string]any{"var": "ctx"},
			map[string]any{"out": outVar},
		},
	}}}
}

// futureLetZeroMap 构造一条 `let out = zero(map<string,any>)` 语句（对标翻译产物的 var 声明）。
func futureLetZeroMap(varName string) map[string]any {
	return map[string]any{"let": map[string]any{varName: map[string]any{"zero": "map<string,any>"}}}
}

// twoFutureProgram 构造一个平铺（无 workflow.Go）的双 future 工作流体：
//
//	var r1, r2 map[string]any
//	f1 := workflow.ExecuteActivity(ctx, A, "a")
//	f2 := workflow.ExecuteActivity(ctx, B, "b")
//	err1 := f1.Get(ctx, &r1)
//	err2 := f2.Get(ctx, &r2)
//
// 这是 sdkgen 双 future 翻译产物的等价手写形态（已实测 GenerateFromSource 端到端可翻译）。
func twoFutureProgram(activities map[string]programpkg.ActivityRef, program []any) *programpkg.WorkflowConfig {
	return &programpkg.WorkflowConfig{
		ID:         "two-future-wf",
		Activities: activities,
		Program:    programpkg.NewWLangProgram(program),
	}
}

func twoFutureActivities() map[string]programpkg.ActivityRef {
	return map[string]programpkg.ActivityRef{
		"activities.A": {Symbol: "activities.A", Type: "activities.A"},
		"activities.B": {Symbol: "activities.B", Type: "activities.B"},
	}
}

// 🔴 核心 Happy Path: f1:=Exec(A); f2:=Exec(B); f1.Get; f2.Get —— 空历史一次 WFT 产
// [A, B] 两条 CmdScheduleActivity。证明 ExecuteActivity 立即调度（并发发起），不再串行。
func TestTrueFuture_TwoActivities_OneWFTSchedulesBoth(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureLetZeroMap("r2"),
		futureExecCall("f1", "activities.A", "a"),
		futureExecCall("f2", "activities.B", "b"),
		futureGetCall("err1", "f1", "r1"),
		futureGetCall("err2", "f2", "r2"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	if len(schedules) != 2 {
		t.Fatalf("expected 2 CmdScheduleActivity from one WFT (true future concurrency), got %d: %+v", len(schedules), commands)
	}
	// 声明顺序 = 调度顺序：A 在前、B 在后。
	if schedules[0].ActivityType != "activities.A" {
		t.Fatalf("first schedule should be activities.A, got %q", schedules[0].ActivityType)
	}
	if schedules[1].ActivityType != "activities.B" {
		t.Fatalf("second schedule should be activities.B, got %q", schedules[1].ActivityType)
	}
	// stepID 命名规则：per-lane 结构化键，top-level（lanePath=""）内 activity 按 kind 分桶局部计数 → A=#a1、B=#a2。
	if schedules[0].ActivityID != "#a1" || schedules[1].ActivityID != "#a2" {
		t.Fatalf("unexpected stepIDs: %q, %q", schedules[0].ActivityID, schedules[1].ActivityID)
	}
	// 未产 CmdCompleteWorkflow（有 pending schedule）。
	if hasCompleteWorkflow(commands) {
		t.Fatalf("must not complete workflow while activities pending, got %+v", commands)
	}
}

// Happy Path: 独立按需等待。历史里 #a1 已调度未完成、#a2 已调度已完成 →
// replay 后 f1.Get 因 A 未 ready 触发 Goexit（不返结果），f2.Get 拿到 B 的历史结果。
// 证明两个 future 各自独立等待，f2 结果可读不被 f1 阻塞（语义上）。
func TestTrueFuture_IndependentGet(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureLetZeroMap("r2"),
		futureExecCall("f1", "activities.A", "a"),
		futureExecCall("f2", "activities.B", "b"),
		futureGetCall("err1", "f1", "r1"),
		futureGetCall("err2", "f2", "r2"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// #a1 调度未完成；#a2 调度已完成。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2"}}
	compB := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"rb": int64(2)}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, schedB, compB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// A 已调度未完成、B 已调度已完成 → 无新调度命令（幂等），无收尾（A 仍 pending）。
	if got := len(findScheduleCommands(commands)); got != 0 {
		t.Fatalf("expected no new schedules (A pending, B done), got %d: %+v", got, commands)
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("must not complete while A pending, got %+v", commands)
	}
}

// Happy Path: 两 activity 都完成 → 无新调度、工作流收尾（CmdCompleteWorkflow）。
func TestTrueFuture_BothCompleted_Finishes(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureLetZeroMap("r2"),
		futureExecCall("f1", "activities.A", "a"),
		futureExecCall("f2", "activities.B", "b"),
		futureGetCall("err1", "f1", "r1"),
		futureGetCall("err2", "f2", "r2"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}
	schedB := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2"}}
	compA := history.Event{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ra": int64(1)}}}
	compB := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"rb": int64(2)}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, schedB, compA, compB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(findScheduleCommands(commands)); got != 0 {
		t.Fatalf("expected no new schedules once both done, got %d: %+v", got, commands)
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("expected CmdCompleteWorkflow once both done, got %+v", commands)
	}
}

// ============================================
// 不确定点1 验证：Get 未 ready Goexit 要不要设 blocked？
// ============================================

// blocked 验证情形A（同轮 Execute+Get）: f:=Exec(A); f.Get() 首轮，A 未 ready → Goexit。
// 断言本轮命令有 CmdScheduleActivity（hasSchedule=true 避免误判完成）、无 CmdCompleteWorkflow。
func TestTrueFuture_SameWFTExecuteAndGet_NoFalseComplete(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureExecCall("f1", "activities.A", "a"),
		futureGetCall("err1", "f1", "r1"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	if len(schedules) != 1 || schedules[0].ActivityType != "activities.A" {
		t.Fatalf("expected exactly 1 schedule for A (Execute immediate), got %+v", commands)
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("hasSchedule should prevent false complete in same WFT, got %+v", commands)
	}
}

// blocked 验证情形B（跨轮 pending）: 上一轮已调度 A，本轮 replay 到 Get，历史 A 是
// ActivityScheduled 未闭合 → hasPending=true。断言无新调度（幂等）、无 CmdCompleteWorkflow 误判。
func TestTrueFuture_CrossWFTPending_NoFalseComplete(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureExecCall("f1", "activities.A", "a"),
		futureGetCall("err1", "f1", "r1"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// 上一轮已调度 A（历史里 #a1 是 ActivityScheduled 未闭合）。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// 幂等：A 已在历史里调度 → 本轮不重复产命令。
	if got := len(findScheduleCommands(commands)); got != 0 {
		t.Fatalf("idempotent: expected no re-schedule for already-scheduled A, got %d: %+v", got, commands)
	}
	// hasPending（scheduled map 非空）阻止误判完成。
	if hasCompleteWorkflow(commands) {
		t.Fatalf("hasPending should prevent false complete cross WFT, got %+v", commands)
	}
}

// fire-and-forget: Execute(A) 后不 Get，main 直接返回 → 同一批产出 A 的调度命令与 Workflow 完成命令，
// A 的结果不再被消费（与 Temporal 一致：main 返回即结束）。
func TestTrueFuture_FireAndForget_MainReturnCompletes(t *testing.T) {
	// 工作流体只 Execute，不 Get。
	program := []any{
		futureExecCall("f1", "activities.A", "a"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commandsBeforeCompletion(t, commands))
	if len(schedules) != 1 || schedules[0].ActivityType != "activities.A" {
		t.Fatalf("fire-and-forget: expected 1 schedule for A before the completion, got %+v", commands)
	}
}

// fire-and-forget 且 A 已完成 → 主线无 Get，不重复调度 A，工作流完成。
func TestTrueFuture_FireAndForget_CompletedAllowsFinish(t *testing.T) {
	program := []any{
		futureExecCall("f1", "activities.A", "a"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}
	compA := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ra": int64(1)}}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA, compA}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(findScheduleCommands(commands)); got != 0 {
		t.Fatalf("expected no re-schedule when A completed, got %d: %+v", got, commands)
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("fire-and-forget with A completed should finish, got %+v", commands)
	}
}

// ============================================
// 幂等 + 失败
// ============================================

// 幂等: 历史含 #a1 scheduled → 重放不重复产 #a1 调度命令（跨轮幂等）。
func TestTrueFuture_Idempotent_NoDuplicateSchedule(t *testing.T) {
	program := []any{
		futureLetZeroMap("r1"),
		futureLetZeroMap("r2"),
		futureExecCall("f1", "activities.A", "a"),
		futureExecCall("f2", "activities.B", "b"),
		futureGetCall("err1", "f1", "r1"),
		futureGetCall("err2", "f2", "r2"),
	}
	nodes := twoFutureProgram(twoFutureActivities(), program)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	// #a1 已调度未完成、B 尚未调度。
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, schedA}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	schedules := findScheduleCommands(commands)
	// A 已调度 → 不重复；B 尚未调度 → 本轮首次产 B（Execute 立即调度）。
	if len(schedules) != 1 || schedules[0].ActivityType != "activities.B" {
		t.Fatalf("expected only B scheduled (A idempotent), got %+v", commands)
	}
}

// Error Case: 历史里 A_1 已失败 → f.Get 返回历史错误（方案A 的 Get 已失败分支）。
//
// 直接单测 ActivityFuture.Get：验证「已失败 → 返历史错误」这一分支（方案A 不改此分支，
// 保持与现状一致）。不走端到端 WLang 工作流体断言——因为 WLang 路径的 `return err` 不会把
// error 值自动传播为工作流失败（host 返回的 error 到 WLang 层仅是变量，见 Go host 方法注释），
// 那是 WLang 失败传播的既有边界，不属阶段1 真 future 改造范围。
func TestTrueFuture_FailedActivity_GetReturnsHistoricalError(t *testing.T) {
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}
	failA := history.Event{ID: 3, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: "boom", Failure: &protocol.Failure{Message: "boom"}}}
	e := NewWorkflowExecutor(nil, []history.Event{started, schedA, failA})
	// 分批模型下构造不再自动灌表（灌表在 Execute 的 applyBatch）；本测试绕过 Execute 裸调 Get，
	// 需显式灌入事实（等价于旧「构造即全量 replay」的前提）。
	e.applyBatch(e.events)

	f := ActivityFuture{executor: e, call: &activityCall{id: "#a1", scheduledEventID: 2}, activityType: "activities.A"}
	var out map[string]any
	err := f.Get(workflowContext{}, &out)
	if err == nil {
		t.Fatal("expected Get to return historical failure error")
	}
	if !contains(err.Error(), "boom") {
		t.Fatalf("expected error to contain 'boom', got: %v", err)
	}
}

// Happy Path: 直接单测 ActivityFuture.Get 已完成分支 → 返历史结果。
func TestTrueFuture_CompletedActivity_GetReturnsResult(t *testing.T) {
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	schedA := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}}
	compA := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ra": int64(1)}}}
	e := NewWorkflowExecutor(nil, []history.Event{started, schedA, compA})
	// 同上：绕过 Execute 裸调 Get 的单测需显式灌表。
	e.applyBatch(e.events)

	f := ActivityFuture{executor: e, call: &activityCall{id: "#a1", scheduledEventID: 2}, activityType: "activities.A"}
	var out map[string]any
	if err := f.Get(workflowContext{}, &out); err != nil {
		t.Fatalf("expected nil error for completed activity, got: %v", err)
	}
	if got, _ := out["ra"].(int64); got != 1 {
		t.Fatalf("expected out[ra]=1 from historical completion, got %#v", out)
	}
}

// 🔴 重放确定性: 多场景 × 200 次重放，产出的 CmdScheduleActivity 顺序稳定 = 声明顺序。
func TestTrueFuture_ReplayDeterminism(t *testing.T) {
	activities := map[string]programpkg.ActivityRef{
		"activities.A": {Symbol: "activities.A", Type: "activities.A"},
		"activities.B": {Symbol: "activities.B", Type: "activities.B"},
		"activities.C": {Symbol: "activities.C", Type: "activities.C"},
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	// 场景：2 future、3 future、乱序、含已完成/已失败历史。
	scenarios := []struct {
		name   string
		body   []any
		events []history.Event
	}{
		{
			name: "two_futures_empty_history",
			body: []any{
				futureLetZeroMap("r1"), futureLetZeroMap("r2"),
				futureExecCall("f1", "activities.A", "a"),
				futureExecCall("f2", "activities.B", "b"),
				futureGetCall("err1", "f1", "r1"),
				futureGetCall("err2", "f2", "r2"),
			},
			events: []history.Event{started},
		},
		{
			name: "three_futures_empty_history",
			body: []any{
				futureLetZeroMap("r1"), futureLetZeroMap("r2"), futureLetZeroMap("r3"),
				futureExecCall("f1", "activities.A", "a"),
				futureExecCall("f2", "activities.B", "b"),
				futureExecCall("f3", "activities.C", "c"),
				futureGetCall("err1", "f1", "r1"),
				futureGetCall("err2", "f2", "r2"),
				futureGetCall("err3", "f3", "r3"),
			},
			events: []history.Event{started},
		},
		{
			// 乱序声明：C 在前、A 中、B 后。调度顺序 = 声明顺序 C,A,B。
			name: "reordered_declaration",
			body: []any{
				futureLetZeroMap("r1"), futureLetZeroMap("r2"), futureLetZeroMap("r3"),
				futureExecCall("f1", "activities.C", "c"),
				futureExecCall("f2", "activities.A", "a"),
				futureExecCall("f3", "activities.B", "b"),
				futureGetCall("err1", "f1", "r1"),
				futureGetCall("err2", "f2", "r2"),
				futureGetCall("err3", "f3", "r3"),
			},
			events: []history.Event{started},
		},
		{
			// A 已调度未完成、B 尚未调度 → 每轮只产 B 调度（幂等 + 确定）。
			name: "partial_history_A_scheduled",
			body: []any{
				futureLetZeroMap("r1"), futureLetZeroMap("r2"),
				futureExecCall("f1", "activities.A", "a"),
				futureExecCall("f2", "activities.B", "b"),
				futureGetCall("err1", "f1", "r1"),
				futureGetCall("err2", "f2", "r2"),
			},
			events: []history.Event{
				started,
				{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}},
			},
		},
		{
			// A 已完成、B 尚未调度 → 每轮只产 B 调度。
			name: "partial_history_A_completed",
			body: []any{
				futureLetZeroMap("r1"), futureLetZeroMap("r2"),
				futureExecCall("f1", "activities.A", "a"),
				futureExecCall("f2", "activities.B", "b"),
				futureGetCall("err1", "f1", "r1"),
				futureGetCall("err2", "f2", "r2"),
			},
			events: []history.Event{
				started,
				{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}},
				{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"ra": int64(1)}}},
			},
		},
	}

	for _, sc := range scenarios {
		nodes := twoFutureProgram(activities, sc.body)
		var reference []string
		for i := 0; i < 200; i++ {
			commands, err := NewWorkflowExecutor(nodes, sc.events).Execute()
			if err != nil {
				t.Fatalf("[%s] replay %d: %v", sc.name, i, err)
			}
			var order []string
			for _, c := range findScheduleCommands(commands) {
				order = append(order, c.ActivityID)
			}
			if i == 0 {
				reference = order
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
