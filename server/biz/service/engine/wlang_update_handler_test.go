package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// counterHandlerProgram 构造一个注册 update handler 的 WLang 程序：
//
//	counter := 0
//	workflow.SetUpdateHandler(ctx, name, func(ctx, in map<string,any>) (map<string,any>, error) {
//	    counter = counter + in["n"]
//	    return {"total": counter}, nil
//	})
//
// 闭包按引用捕获外层 counter，多次 update 依次累加。
func counterHandlerProgram(name string) *program.WorkflowConfig {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do": []any{
				map[string]any{"set": map[string]any{"counter": map[string]any{"+": []any{
					map[string]any{"var": "counter"},
					// in["n"] 是 any 类型，与 Go 解码进 map[string]any 相同是 float64，需 type.assert 成 float64 才能与 counter(float64) 相加，
					// 对标 go2wlang 翻译 input["n"].(float64) 的产物。
					map[string]any{"type.assert": []any{
						map[string]any{"map.value": []any{
							map[string]any{"var": "in"},
							map[string]any{"literal": map[string]any{"type": "string", "value": "n"}},
						}},
						map[string]any{"literal": map[string]any{"type": "string", "value": "float64"}},
					}},
				}}}},
				handlerReturn(map[string]any{"map": map[string]any{
					"type": []any{"string", "any"},
					"entries": []any{
						[]any{
							map[string]any{"literal": map[string]any{"type": "string", "value": "total"}},
							map[string]any{"var": "counter"},
						},
					},
				}}),
			},
		},
	}
	return &program.WorkflowConfig{
		Type: "counter-wf",
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"counter": map[string]any{"literal": map[string]any{"type": "float64", "value": "0"}}}},
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": name}},
				handlerFn,
			}}},
		}),
	}
}

// handlerReturn 构造 handler 的成功返回 `return <value>, nil`。
func handlerReturn(value map[string]any) map[string]any {
	return map[string]any{"return": map[string]any{"tuple": []any{
		value,
		map[string]any{"literal": map[string]any{"type": "null", "value": nil}},
	}}}
}

// handlerFail 构造 handler 的失败返回 `return nil, workflow.NewApplicationError(message, "")`。
func handlerFail(message string) map[string]any {
	return map[string]any{"return": map[string]any{"tuple": []any{
		map[string]any{"literal": map[string]any{"type": "null", "value": nil}},
		map[string]any{"NewApplicationError": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"literal": map[string]any{"type": "string", "value": message}},
			map[string]any{"literal": map[string]any{"type": "string", "value": ""}},
		}},
	}}}
}

// acceptedEvent 构造一条 UpdateAccepted：input 作为 handler 唯一的业务参数（map<string,any>）编成一个 payload。
func acceptedEvent(id int64, name, updateID string, input map[string]any) history.Event {
	payload, err := protocol.NewJSONPayload(input)
	if err != nil {
		panic(err)
	}
	return acceptedArgsEvent(id, name, updateID, protocol.Payloads{payload})
}

// acceptedArgsEvent 构造一条携带任意 Args 的 UpdateAccepted。
func acceptedArgsEvent(id int64, name, updateID string, args protocol.Payloads) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{
		Name: name, UpdateID: updateID, Args: &args,
	}}
}

// completedEvent 造一条 update 完成历史事件：成功只存 Outcome.Success。
func completedEvent(t *testing.T, id int64, name, updateID string, output map[string]any) history.Event {
	t.Helper()
	payload, err := protocol.NewJSONPayload(output)
	if err != nil {
		t.Fatalf("encode update output: %v", err)
	}
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
		Name: name, UpdateID: updateID, Outcome: &protocol.UpdateOutcome{Success: &payload},
	}}
}

// updateOutcomeOutput 解开 complete 命令的 Success payload：成功只走 Outcome.Success，
// 不再有「只带 Output map」的命令通道。JSON 解码后数字是 float64。
func updateOutcomeOutput(t *testing.T, cmd history.Command) map[string]any {
	t.Helper()
	if cmd.UpdateOutcome == nil || cmd.UpdateOutcome.Success == nil {
		t.Fatalf("complete command must carry outcome success: %+v", cmd)
	}
	var out map[string]any
	if err := cmd.UpdateOutcome.Success.Decode(&out); err != nil {
		t.Fatalf("decode update output: %v", err)
	}
	return out
}

func findUpdateCompletes(commands []history.Command) []history.Command {
	var out []history.Command
	for _, c := range commands {
		if c.Type == history.CmdCompleteWorkflowUpdate {
			out = append(out, c)
		}
	}
	return out
}

// Happy Path: 注册了 handler 的工作流，收到一个 accepted update 后，
// handler 协程产出 CmdCompleteWorkflowUpdate，Outcome 带 handler 计算结果。
func TestSetUpdateHandler_CompletesAcceptedUpdate(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(5)})

	commands, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 {
		t.Fatalf("expected 1 CmdCompleteWorkflowUpdate, got %+v", commands)
	}
	c := completes[0]
	if c.UpdateID != "u-1" || c.Name != "add" {
		t.Fatalf("unexpected complete command: %+v", c)
	}
	if got, _ := updateOutcomeOutput(t, c)["total"].(float64); got != 5 {
		t.Fatalf("expected total=5, got %v (%T)", updateOutcomeOutput(t, c)["total"], updateOutcomeOutput(t, c)["total"])
	}
}

// 🔴 核心：handler 闭包 counter += in["n"]，多个 accepted 依次执行，counter 正确累加。
// 每一轮重放都从头重跑 body（counter 重置为 0），按 AcceptedEventID 顺序执行全部 accepted，
// 已 completed 的幂等跳过 —— 累加体现在"第 k 个 update 的 total = 前 k 个 n 之和"。
func TestSetUpdateHandler_ClosureCounterAccumulates(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	// 三个 accepted，n = 5, 7, 3。历史全局序由 AcceptedEventID 保证。
	a1 := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(5)})
	a2 := acceptedEvent(3, "add", "u-2", map[string]any{"n": int64(7)})
	a3 := acceptedEvent(4, "add", "u-3", map[string]any{"n": int64(3)})

	commands, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, a1, a2, a3}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 3 {
		t.Fatalf("expected 3 completes, got %d: %+v", len(completes), commands)
	}
	// 累加：u-1 total=5, u-2 total=12, u-3 total=15
	wantTotals := map[string]float64{"u-1": 5, "u-2": 12, "u-3": 15}
	// 顺序也必须按 AcceptedEventID 稳定。
	wantOrder := []string{"u-1", "u-2", "u-3"}
	for i, c := range completes {
		if c.UpdateID != wantOrder[i] {
			t.Fatalf("handler order mismatch at %d: got %s, want %s", i, c.UpdateID, wantOrder[i])
		}
		got, _ := updateOutcomeOutput(t, c)["total"].(float64)
		if got != wantTotals[c.UpdateID] {
			t.Fatalf("update %s total=%v, want %v", c.UpdateID, updateOutcomeOutput(t, c)["total"], wantTotals[c.UpdateID])
		}
	}
}

// Error Case: handler 返回 error → completed-with-failure（Outcome.Failure 指针非 nil，Type 为空）。
func TestSetUpdateHandler_HandlerReturnsError_CompletesWithFailure(t *testing.T) {
	handlerFn := map[string]any{
		"fn": map[string]any{
			"params": []any{
				[]any{"ctx", "workflow.Context"},
				[]any{"in", "map<string,any>"},
			},
			"returns": []any{"map<string,any>", "error"},
			"do":      []any{handlerFail("boom")},
		},
	}
	nodes := &program.WorkflowConfig{
		Type: "fail-wf",
		Program: program.NewWLangProgram([]any{
			map[string]any{"expr": map[string]any{"SetUpdateHandler": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				map[string]any{"literal": map[string]any{"type": "string", "value": "boom_upd"}},
				handlerFn,
			}}},
		}),
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "boom_upd", "u-err", map[string]any{})

	commands, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	completes := findUpdateCompletes(commands)
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete, got %+v", commands)
	}
	failure := completes[0].UpdateOutcome
	if failure == nil || failure.Failure == nil {
		t.Fatalf("expected Outcome.Failure, got %+v", completes[0])
	}
	if failure.Failure.Message != "boom" || failure.Failure.Type != "" {
		t.Fatalf("expected Failure{Message:boom, Type:\"\"}, got %+v", failure.Failure)
	}
	if len(completes[0].Output) != 0 {
		t.Fatalf("failure command should not carry output, got %+v", completes[0].Output)
	}
}

// 幂等语义迁移：旧模型靠 updateCompleted 表短路跳过；
// 协程化后 replay 批重放 handler 全程（照 Temporal），其 Complete 进 replayCommands 与历史
// WorkflowUpdateCompleted 按 UpdateID 配对（比对通过 = 幂等成立），上报侧（commands）无重复。
// 历史构造改为真实 server 形态：Completed 是命令事件归其决策批（前必有 WFTC），
// 旧构造「单批塞 completed」只在测试里存在。
func TestSetUpdateHandler_ReplayedCompleteIsNotReported(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	accepted := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(5)})
	wftc := wftCompletedEvent(3)
	completed := completedEvent(t, 4, "add", "u-1", map[string]any{"total": int64(5)})

	commands, err := interpreter.NewWorkflowExecutor(nodes, []history.Event{started, accepted, wftc, completed}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(findUpdateCompletes(commands)) != 0 {
		t.Fatalf("replay 批重放的 Complete 不得上报（进 replayCommands 只比对）, got %+v", commands)
	}
}

// 确定性重放：同一历史多次重放，handler 产出的 completes 顺序/内容稳定（AcceptedEventID 排序）。
func TestSetUpdateHandler_DeterministicAcrossReplays(t *testing.T) {
	nodes := counterHandlerProgram("add")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	a1 := acceptedEvent(2, "add", "u-1", map[string]any{"n": int64(5)})
	a2 := acceptedEvent(3, "add", "u-2", map[string]any{"n": int64(7)})
	a3 := acceptedEvent(4, "add", "u-3", map[string]any{"n": int64(3)})
	events := []history.Event{started, a1, a2, a3}

	var reference []string
	for i := 0; i < 50; i++ {
		commands, err := interpreter.NewWorkflowExecutor(nodes, events).Execute()
		if err != nil {
			t.Fatal(err)
		}
		var order []string
		for _, c := range findUpdateCompletes(commands) {
			order = append(order, c.UpdateID)
		}
		if i == 0 {
			reference = order
			continue
		}
		if len(order) != len(reference) {
			t.Fatalf("replay %d length drift: %v vs %v", i, order, reference)
		}
		for j := range order {
			if order[j] != reference[j] {
				t.Fatalf("replay %d order drift at %d: %v vs %v", i, j, order, reference)
			}
		}
	}
	if want := []string{"u-1", "u-2", "u-3"}; len(reference) != 3 || reference[0] != want[0] || reference[2] != want[2] {
		t.Fatalf("unexpected deterministic order: %v", reference)
	}
}

// Failure wire：WorkflowUpdateCompletedEvent.Failure → updateRecordsFromEvents → fillCompleted
// → Outcome=failure。验证 handler 失败载荷从历史事件一路还原到调用方 outcome。
func TestSetUpdateHandler_FailureWireReachesOutcome(t *testing.T) {
	events := []history.Event{
		{ID: 1, WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{Name: "boom_upd", UpdateID: "u-err"}},
		{ID: 2, WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{
			Name: "boom_upd", UpdateID: "u-err",
			Outcome: &protocol.UpdateOutcome{Failure: &protocol.Failure{Message: "boom"}},
		}},
	}
	recs := execution.UpdateRecordsFromEvents(events)
	if len(recs) != 1 || recs[0].Outcome == nil || recs[0].Outcome.Failure == nil || recs[0].Outcome.Failure.Message != "boom" {
		t.Fatalf("expected record Failure=boom, got %+v", recs)
	}
	result := execution.CompleteUpdateResult(protocol.WorkflowUpdateResult{UpdateID: "u-err"}, recs[0].Outcome)
	if result.Outcome == nil || result.Outcome.Failure == nil || result.Outcome.Failure.Message != "boom" {
		t.Fatalf("expected Outcome=failure Failure=boom, got %+v", result)
	}
}
