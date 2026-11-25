package interpreter

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// update handler 协程化测试
// ============================================
// T4/T6/T13 在 wlang_update_coroutine_test.go；T7 由 wlang_replay_match_test.go 的
// TestReplayMatch_UpdateHandler_RunSkipped 改造承载。
// ============================================

// T1 Happy Path: 单批单 update → handler 协程执行、CmdCompleteWorkflowUpdate（live）产出、Output 正确。
func TestUpdateHandlerCoroutine_SingleUpdate_Completes(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(5)})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 || completes[0].UpdateID != "u-1" {
		t.Fatalf("expected 1 complete for u-1, got %+v", commands)
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["total"].(float64); got != 5 {
		t.Fatalf("expected total=5, got %+v", updateOutcomeOutput(t, completes[0]))
	}
}

// lateRegisterProgram 的 body 先停在 activity 上，之后才注册 "late" handler：
//
//	workflow.ExecuteActivity(ctx, activities.Work, {}).Get(ctx, &out)   // main 的第一个阻塞点
//	workflow.SetUpdateHandler(ctx, "late", func(ctx, in) (map<string,any>, error) { return {"late": true}, nil })
func lateRegisterProgram() *program.WorkflowConfig {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				handlerReturn(map[string]any{"map": map[string]any{
					"type": []any{"string", "any"},
					"entries": []any{[]any{
						map[string]any{"literal": map[string]any{"type": "string", "value": "late"}},
						map[string]any{"literal": map[string]any{"type": "boolean", "value": true}},
					}},
				}}),
			},
		},
	}
	nodes := &program.WorkflowConfig{
		Type: "late-register",
		Activities: map[string]program.ActivityRef{
			"activities.Work": {Symbol: "activities.Work", Type: "activities.Work"},
		},
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
			map[string]any{"let": map[string]any{"err": map[string]any{"Get": []any{
				map[string]any{"ExecuteActivity": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"symbol": "activities.Work"},
					map[string]any{"map": map[string]any{"type": []any{"string", "any"}, "entries": []any{}}},
				}},
				map[string]any{"var": "ctx"},
				map[string]any{"out": "out"},
			}}}},
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "late"}},
				handlerFn,
			}}},
		}),
	}
	return nodes
}

// requireUnknownUpdate 断言 commands 里恰有一条针对 updateID 的 UnknownUpdate 失败完成命令。
func requireUnknownUpdate(t *testing.T, commands []history.Command, name, updateID string) {
	t.Helper()
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 || completes[0].UpdateID != updateID || completes[0].Name != name {
		t.Fatalf("expected one complete for %s, got %+v", updateID, commands)
	}
	outcome := completes[0].UpdateOutcome
	if outcome == nil || outcome.Failure == nil || outcome.Success != nil {
		t.Fatalf("unknown update must complete with failure, got %+v", completes[0])
	}
	if outcome.Failure.Type != "UnknownUpdate" || outcome.Failure.Message != `no update handler registered for "`+name+`"` {
		t.Fatalf("failure = %+v, want UnknownUpdate for %q", outcome.Failure, name)
	}
}

// Error Case: body 跑完也没有注册该 name 的 handler → update 以 UnknownUpdate 失败完成，
// 而不是永久停在 Accepted；main 已返回，workflow 在同一批完成。
func TestUpdateHandlerCoroutine_UnknownUpdate_Rejected(t *testing.T) {
	nodes := &program.WorkflowConfig{
		Type: "no-handler",
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"x": map[string]any{"literal": map[string]any{"type": "int64", "value": "1"}}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "orphan", "u-x", map[string]any{})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	requireUnknownUpdate(t, commands, "orphan", "u-x")
	if last := commands[len(commands)-1]; last.Type != history.CmdCompleteWorkflow {
		t.Fatalf("main 已返回，update 完成命令之后应紧跟 workflow 完成命令, got %+v", commands)
	}

	// replay：同一批重放产出同一条 UnknownUpdate 命令，与历史 WorkflowUpdateCompleted 配对，不再上报。
	failed := history.Event{ID: 4, Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "orphan", UpdateID: "u-x", Outcome: findUpdateCompletes(commands)[0].UpdateOutcome,
	}}
	replayed, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftCompletedEvent(3), failed}).Execute()
	if err != nil {
		t.Fatalf("replay of an unknown-update rejection must match history: %v", err)
	}
	if len(findUpdateCompletes(replayed)) != 0 {
		t.Fatalf("replay 批的 UnknownUpdate 命令不得再次上报, got %+v", replayed)
	}
	if !hasCompleteWorkflow(replayed) {
		t.Fatalf("workflow 应在 update 被拒之后正常完成, got %+v", replayed)
	}
}

// Error Case: update 到达时 main 已停在第一个阻塞点、handler 还没注册 → UnknownUpdate。
// handler 在阻塞点之后才注册，对这次 update 不生效；之后到达的同名 update 正常执行。
func TestUpdateHandlerCoroutine_UpdateBeforeRegistration_Rejected(t *testing.T) {
	nodes := lateRegisterProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	wftc := wftCompletedEvent(2)
	schedBody := history.Event{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Work"}}
	accepted := acceptedEvent(4, "late", "u-1", map[string]any{})

	// live 批：main 停在 activity 上，"late" 还没注册。
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, wftc, schedBody, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	requireUnknownUpdate(t, commands, "late", "u-1")

	// 之后 activity 完成、main 注册 handler；重放时 u-1 仍在它所属的批被拒（与历史配对），
	// 与 activity 完成同批到达的 u-2 由 handler 正常完成（main 先于 handler 协程运行并完成注册）。
	wftc2 := wftCompletedEvent(5)
	rejected := history.Event{ID: 6, Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: "late", UpdateID: "u-1", Outcome: findUpdateCompletes(commands)[0].UpdateOutcome,
	}}
	compBody := history.Event{ID: 7, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{}}}
	accepted2 := acceptedEvent(8, "late", "u-2", map[string]any{})

	replayed, err := NewWorkflowExecutor(nodes, []history.Event{started, wftc, schedBody, accepted, wftc2, rejected, compBody, accepted2}).Execute()
	if err != nil {
		t.Fatalf("replay must reproduce the rejection deterministically: %v", err)
	}
	completes := findUpdateCompletes(replayed)
	if len(completes) != 1 || completes[0].UpdateID != "u-2" {
		t.Fatalf("注册之后到达的 update 应由 handler 完成, got %+v", replayed)
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["late"].(bool); !got {
		t.Fatalf("handler output 应上浮, got %+v", updateOutcomeOutput(t, completes[0]))
	}
}

// T5 Edge Case: 同批两 update → 按 AcceptedEventID 升序 spawn/完成（counter 闭包累加序可观测）。
func TestUpdateHandlerCoroutine_MultiUpdate_AcceptedEventIDOrder(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	acceptedA := acceptedEvent(2, "add", "u-a", map[string]any{"n": int64(1)})
	acceptedB := acceptedEvent(3, "add", "u-b", map[string]any{"n": int64(2)})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, acceptedA, acceptedB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 2 {
		t.Fatalf("expected 2 completes, got %+v", commands)
	}
	if completes[0].UpdateID != "u-a" || completes[1].UpdateID != "u-b" {
		t.Fatalf("完成序应为 AcceptedEventID 升序 [u-a, u-b], got [%s, %s]", completes[0].UpdateID, completes[1].UpdateID)
	}
	// counter 闭包累加序：u-a 先 (+1 → 1)，u-b 后 (+2 → 3)。
	if got, _ := updateOutcomeOutput(t, completes[0])["total"].(float64); got != 1 {
		t.Fatalf("u-a total=1, got %v", updateOutcomeOutput(t, completes[0]))
	}
	if got, _ := updateOutcomeOutput(t, completes[1])["total"].(float64); got != 3 {
		t.Fatalf("u-b total=3, got %v", updateOutcomeOutput(t, completes[1]))
	}
}

// T8 Error Case（比对收紧漂移锚，取代旧整体豁免）: 历史 handler 内调 activity A（键
// update:u-1#a1），代码/历史 type 漂移 → shadow Sched(Enrich) vs 历史 Sched(Other) 失配。
func TestReplayMatch_UpdateHandlerDrift_Detected(t *testing.T) {
	nodes := activityHandlerProgram() // handler 内调 activities.Enrich
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "enrich", "u-1", map[string]any{"id": "x"})
	wftc := wftCompletedEvent(3)
	// 历史记录的是 Other（漂移形态：历史与代码不一致）。
	scheduled := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "update:u-1#a1", ActivityType: "activities.Other"}}
	completed := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{}}}

	_, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc, scheduled, completed}).Execute()
	if err == nil {
		t.Fatalf("handler 内 activity type 漂移应被比对抓住")
	}
	var nd executor.NonDeterministicError
	if !errors.As(err, &nd) {
		t.Fatalf("应为 nonDeterministicError, got %T: %v", err, err)
	}
}

// T9 Happy Path（AddFuture 解禁锚）: handler 内 Selector.AddFuture + Select park →
// 下批 activity 完成 → flushFutureBridges 触达 → 命中、handler 完成。
func TestUpdateHandler_AddFutureInside_CrossBatch(t *testing.T) {
	// handler: f := ExecuteActivity(Fetch); sel := NewSelector; sel.AddFuture(ctx, f, cb{Get}); sel.Select
	addFutureCb := map[string]any{"fn": map[string]any{
		"params":  []any{[]any{"fut", "workflow.ActivityFuture"}},
		"returns": []any{},
		"do": []any{
			map[string]any{"let": map[string]any{"e": map[string]any{"Get": []any{
				map[string]any{"var": "fut"},
				map[string]any{"var": "ctx"},
				map[string]any{"out": "out"},
			}}}},
		},
	}}
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"let": map[string]any{"out": map[string]any{"zero": "map<string,any>"}}},
				map[string]any{"let": map[string]any{"f": map[string]any{"ExecuteActivity": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"symbol": "activities.Fetch"},
					map[string]any{"var": "in"},
				}}}},
				selNew("sel"),
				map[string]any{"expr": map[string]any{"AddFuture": []any{
					map[string]any{"var": "sel"},
					map[string]any{"var": "ctx"},
					map[string]any{"var": "f"},
					addFutureCb,
				}}},
				selSelect("sel"),
				handlerReturn(map[string]any{"var": "out"}),
			},
		},
	}
	nodes := &program.WorkflowConfig{
		Type: "handler-addfuture",
		Activities: map[string]program.ActivityRef{
			"activities.Fetch": {Symbol: "activities.Fetch", Type: "activities.Fetch"},
		},
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "fetch"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "fetch", "u-af", map[string]any{"id": "x"})

	// 轮 1: handler 内 ExecuteActivity 首见发命令 + AddFuture 未 ready + Select park。
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatalf("first round: %v", err)
	}
	var schedule *history.Command
	for i := range commands {
		if commands[i].Type == history.CmdScheduleActivity {
			schedule = &commands[i]
		}
	}
	if schedule == nil || schedule.ActivityID != "update:u-af#a1" {
		t.Fatalf("handler 内 future activity 应调度且键在专属桶, got %+v", commands)
	}
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("Select park 中 update 不得完成, got %+v", commands)
	}

	// 轮 2: activity 完成在批 2 → flushFutureBridges Set → Select 命中 → handler 完成（live）。
	wftc := wftCompletedEvent(3)
	scheduled := history.Event{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: schedule.ActivityID, ActivityType: schedule.ActivityType, Input: schedule.Input}}
	completed := history.Event{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"fetched": true}}}

	commands2, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc, scheduled, completed}).Execute()
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	completes := findUpdateCompletes(commands2)
	if len(completes) != 1 || completes[0].UpdateID != "u-af" {
		t.Fatalf("AddFuture 命中后 handler 应完成（解禁锚）, got %+v", commands2)
	}
}

// T10 Error Case: handler 返回 error → Outcome.Failure 指针非 nil。
// （InvokeClosure err 形态由 T13 的非确定性分叉测试反向锚定：非 non-det 的 err 走 Failure。）
func TestUpdateHandler_Failure_CompletesWithFailure(t *testing.T) {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do":      []any{handlerFail("insufficient balance")},
		},
	}
	nodes := &program.WorkflowConfig{
		Type: "failing-handler",
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "debit"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "debit", "u-f", map[string]any{})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 || completes[0].UpdateOutcome == nil || completes[0].UpdateOutcome.Failure == nil {
		t.Fatalf("handler 返回的 error 应转 Outcome.Failure, got %+v", commands)
	}
	if completes[0].UpdateOutcome.Failure.Message != "insufficient balance" {
		t.Fatalf("failure message = %q, want insufficient balance", completes[0].UpdateOutcome.Failure.Message)
	}
	if len(completes[0].Output) != 0 {
		t.Fatalf("failure 不携带 output, got %+v", completes[0].Output)
	}
}

// T12 Error Case 回归锚（v2 必改 1）: body 已结束 + 前批 handler 已 done 后，live 批新到 update
// → handler 仍被执行（Spawn 直入 s.coroutines；SpawnEager 暂存区形态下本测试必挂：暂存协程在
// 全 done 时永不被插入 → 静默吞 update + 误发提前 CmdCompleteWorkflow）。
func TestUpdateHandlerCoroutine_SpawnAfterAllDone(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	acceptedA := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(1)})
	wftc := wftCompletedEvent(3)
	updCompletedA := completedEvent(4, "add", "u-1", map[string]any{"total": int64(1)})
	// 批 2（live）：body 与 u-1 handler 均已 done 后，新 update 到达。
	acceptedB := acceptedEvent(5, "add", "u-2", map[string]any{"n": int64(2)})

	commands, err := NewWorkflowExecutor(nodes, []history.Event{started, acceptedA, wftc, updCompletedA, acceptedB}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 || completes[0].UpdateID != "u-2" {
		t.Fatalf("全 done 后 live 批新 update 仍应被执行并上报, got %+v", commands)
	}
	if got, _ := updateOutcomeOutput(t, completes[0])["total"].(float64); got != 3 {
		t.Fatalf("u-2 在 u-1 重放累加后 total=3, got %+v", updateOutcomeOutput(t, completes[0]))
	}
}
