package interpreter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// TestResultNullCollectionsDifferential：Activity 与子 Workflow 返回的结果里 null 的 slice、map 字段（Go 的
// encoding/json 把 nil slice、nil map 编码成 null）在解释器里与原生 Go 一样是 nil：len 为 0、range 不执行、
// 读 map 得到零值。冷重放与 sticky 会话两条路径的结果都与原生 Go 相同。结果解进 map[string]any 时 null 的键保留。
func TestResultNullCollectionsDifferential(t *testing.T) {
	src, err := os.ReadFile(diffWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	typed := func(raw string) string {
		var r DiffResult
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			t.Fatal(err)
		}
		return canonicalJSON(t, summarizeResult(r))
	}
	untyped := func(raw string) string {
		var d map[string]any
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatal(err)
		}
		return canonicalJSON(t, summarizeRawResult(d))
	}
	mapped := func(raw string) string {
		var d map[string]any
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatal(err)
		}
		return canonicalJSON(t, summarizeMapNumbers(d))
	}
	mixed := func(raw string) string {
		var s DiffSnowflake
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		return canonicalJSON(t, summarizeMixedTargets(s, m))
	}
	nilResult, _ := json.Marshal(DiffResult{Inner: &DiffResult{}})
	for _, tc := range []struct {
		name   string
		result string
		native func(string) string
	}{
		{"DiffActivityResult", string(nilResult), typed},
		{"DiffActivityResult", `{"Tags": null, "Attrs": null, "Items": null}`, typed},
		{"DiffActivityResult", `{"Tags": ["a"], "Attrs": {"k": "v"}, "Items": [{"Name": "x", "Qty": 2}]}`, typed},
		{"DiffActivityResult", `{"Tags": [], "Attrs": {}}`, typed},
		{"DiffActivityResult", `{}`, typed},
		{"DiffChildResult", string(nilResult), typed},
		{"DiffChildResult", `{"Tags": ["a"], "Attrs": null}`, typed},
		{"DiffChildResult", `{"Attrs": {"k": "v"}, "Inner": {"Tags": null, "Attrs": {"x": "y"}}}`, typed},
		{"DiffActivityRawResult", `{"Tags": null, "Name": "a"}`, untyped},
		{"DiffActivityResult", `{"V": 1, "M": {"n": 1, "list": [2]}}`, typed},
		{"DiffActivityMixedTargets", `{"ID": 9007199254740993, "Name": "x"}`, mixed},
		{"DiffActivityMixedTargets", `{"ID": 18014398509481985, "Name": "y"}`, mixed},
		{"DiffActivityMapNumbers", `{"amount": 1, "list": [2], "nested": {"n": 3}}`, mapped},
		{"DiffActivityMapNumbers", `{"amount": 1.5, "list": []}`, mapped},
		{"DiffChildResult", `{"V": 2.5, "M": {"n": 1}}`, typed},
	} {
		result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: tc.name, FuncName: tc.name, Filename: diffWorkflowsFile})
		if err != nil {
			t.Fatalf("%s: generate: %v", tc.name, err)
		}
		want := tc.native(tc.result)
		cold, sticky := runResultWorkflow(t, result, tc.result)
		if cold != want || sticky != want {
			t.Errorf("%s(%s): Go = %s, 冷重放 = %s, sticky = %s", tc.name, tc.result, want, cold, sticky)
		}
	}
}

// runResultWorkflow 执行一个调度一个 Activity 或子 Workflow 的 Workflow：第一个 Workflow Task 产出调度命令，结果
// （按存储读回 History 的方式解码：jsonvalue，整数是 int64）到达后第二个 Workflow Task 完成 Workflow。返回冷重放与 sticky
// 会话的输出。
func runResultWorkflow(t *testing.T, result sdkgen.Result, resultJSON string) (cold, sticky string) {
	t.Helper()
	var output map[string]any
	if err := jsonvalue.Unmarshal([]byte(resultJSON), &output); err != nil {
		t.Fatal(err)
	}
	first := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
	}
	commands, err := NewWorkflowExecutor(result.Workflow, first).Execute()
	if err != nil || len(commands) != 1 {
		t.Fatalf("first workflow task: commands=%+v err=%v", commands, err)
	}
	completed := []history.Event{executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker"))}
	var resultEvents []history.Event
	switch command := commands[0]; command.Type {
	case history.CmdScheduleActivity:
		completed = append(completed, executionSessionEvent(5, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
			ActivityID: command.ActivityID, ActivityType: command.ActivityType,
		})))
		resultEvents = append(resultEvents, executionSessionEvent(6, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{
			ActivityID: command.ActivityID, ScheduledEventID: 5, Output: output,
		})))
	case history.CmdStartChildWorkflow:
		completed = append(completed, executionSessionEvent(5, history.NewChildWorkflowStartedEvent(command.WorkflowType, command.ChildID, "child-run", "", nil)))
		resultEvents = append(resultEvents, executionSessionEvent(6, history.NewChildWorkflowCompletedEvent(command.ChildID, "child-run", 5, output)))
	default:
		t.Fatalf("unexpected command %+v", command)
	}
	second := append(resultEvents,
		executionSessionEvent(7, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(8, history.NewWorkflowTaskStartedEvent(7, "worker", "build", 0)))

	all := append(append(append([]history.Event(nil), first...), completed...), second...)
	coldCommands, err := NewWorkflowExecutor(result.Workflow, all).Execute()
	cold = terminalOutcome(t, coldCommands, err)

	session := NewExecutionSession(context.Background(), result.Workflow, 1)
	t.Cleanup(func() { _ = session.Close() })
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	activation, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "first", ExecutionGeneration: 1, Events: first, ThroughEventID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(history.WorkflowTaskCompletionAck{
		Accepted: true, CommittedThroughEventID: 5, CommittedEvents: completed,
		ActivationID: activation.ActivationID, CommandDigest: activation.CommandDigest, ExecutionGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	activation, err = session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "second", ExecutionGeneration: 1, Events: second, ThroughEventID: 8})
	sticky = terminalOutcome(t, activation.Commands, err)
	return cold, sticky
}

func terminalOutcome(t *testing.T, commands []history.Command, err error) string {
	t.Helper()
	if err != nil {
		return "execute: " + err.Error()
	}
	for _, command := range commands {
		if command.Type == history.CmdCompleteWorkflow {
			return canonicalJSON(t, command.Output)
		}
	}
	return fmt.Sprintf("no completion: %+v", commands)
}

// TestUpdateArgumentAnyNumbersDifferential：Update 参数是结构体时，any 字段与 map[string]any 字段里的 JSON 数字
// 与 Go 的 encoding/json 一样是 float64。
func TestUpdateArgumentAnyNumbersDifferential(t *testing.T) {
	src, err := os.ReadFile(diffWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: "DiffUpdateAny", FuncName: "DiffUpdateAny", Filename: diffWorkflowsFile})
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{`{"V": 1, "M": {"n": 1}}`, `{"V": 1.5, "M": {"n": 2}}`, `{"V": "x"}`} {
		var in DiffInput
		if err := json.Unmarshal([]byte(args), &in); err != nil {
			t.Fatal(err)
		}
		want := canonicalJSON(t, DiffOut{Sum: anyNumberFlags(in.V, in.M), Value: in.M})

		started := executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0))
		accepted := acceptedArgsEvent(2, "probe", "u-1", protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(args)}})
		commands, err := NewWorkflowExecutor(result.Workflow, []history.Event{started, accepted}).Execute()
		if err != nil {
			t.Fatal(err)
		}
		completes := findUpdateCompletes(commands)
		if len(completes) != 1 {
			t.Fatalf("update args %s: commands = %+v", args, commands)
		}
		if got := canonicalJSON(t, updateOutcomeOutput(t, completes[0])); got != want {
			t.Errorf("update args %s: Go = %s, 解释器 = %s", args, want, got)
		}
	}
}

// TestMapPayloadNumbersDifferential：Signal 与 Update 参数直接解进 map[string]any 时，来自 JSON 的数字与 Go 的
// encoding/json 一样是 float64：`d["amount"].(float64)` 成立，`d["amount"] == 1` 为 false，再编码不变。
// 载荷按存储读回 History 的方式（jsonvalue，整数是 int64）进入解释器。
func TestMapPayloadNumbersDifferential(t *testing.T) {
	src, err := os.ReadFile(diffWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	generate := func(name string) sdkgen.Result {
		result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: name, FuncName: name, Filename: diffWorkflowsFile})
		if err != nil {
			t.Fatalf("%s: generate: %v", name, err)
		}
		return result
	}
	signalFlow, updateFlow := generate("DiffSignalNumbers"), generate("DiffUpdateMapNumbers")
	for _, payload := range []string{
		`{"amount": 1, "list": [2, "x"], "nested": {"n": 3}}`,
		`{"amount": 1.5}`,
		`{"amount": 9007199254740993}`,
	} {
		var native map[string]any
		if err := json.Unmarshal([]byte(payload), &native); err != nil {
			t.Fatal(err)
		}
		want := canonicalJSON(t, summarizeMapNumbers(native))
		var stored map[string]any
		if err := jsonvalue.Unmarshal([]byte(payload), &stored); err != nil {
			t.Fatal(err)
		}

		events := []history.Event{
			executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
			executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
			executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
			executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker")),
			executionSessionEvent(5, history.NewWorkflowSignaledEvent("pay", stored)),
			executionSessionEvent(6, history.NewWorkflowTaskScheduledEvent(0)),
			executionSessionEvent(7, history.NewWorkflowTaskStartedEvent(6, "worker", "build", 0)),
		}
		commands, err := NewWorkflowExecutor(signalFlow.Workflow, events).Execute()
		if got := terminalOutcome(t, commands, err); got != want {
			t.Errorf("signal %s: Go = %s, 解释器 = %s", payload, want, got)
		}

		accepted := acceptedArgsEvent(2, "probe", "u-1", protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(payload)}})
		commands, err = NewWorkflowExecutor(updateFlow.Workflow, []history.Event{events[0], accepted}).Execute()
		if err != nil {
			t.Fatal(err)
		}
		completes := findUpdateCompletes(commands)
		if len(completes) != 1 {
			t.Fatalf("update %s: commands = %+v", payload, commands)
		}
		if got := canonicalJSON(t, updateOutcomeOutput(t, completes[0])); got != want {
			t.Errorf("update %s: Go = %s, 解释器 = %s", payload, want, got)
		}
	}
}

// TestSignalNestedPayloadDifferential：Signal 载荷里的嵌套数组与嵌套对象按 x.([]any)、x.(map[string]any) 与
// type switch 读取，结果与 Go 相同。
func TestSignalNestedPayloadDifferential(t *testing.T) {
	src, err := os.ReadFile(diffWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: "DiffSignalNested", FuncName: "DiffSignalNested", Filename: diffWorkflowsFile})
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{
		`{"items": [{"name": "a"}, {"name": "b"}, [1, 2, 3], "s", 2, null], "meta": {"n": 3}}`,
		`{"items": {"name": "a"}, "meta": [1, 2]}`,
		`{"items": [], "meta": {}}`,
	} {
		var native map[string]any
		if err := json.Unmarshal([]byte(payload), &native); err != nil {
			t.Fatal(err)
		}
		want := canonicalJSON(t, summarizeNested(native))
		var stored map[string]any
		if err := jsonvalue.Unmarshal([]byte(payload), &stored); err != nil {
			t.Fatal(err)
		}
		events := []history.Event{
			executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
			executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
			executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
			executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker")),
			executionSessionEvent(5, history.NewWorkflowSignaledEvent("order", stored)),
			executionSessionEvent(6, history.NewWorkflowTaskScheduledEvent(0)),
			executionSessionEvent(7, history.NewWorkflowTaskStartedEvent(6, "worker", "build", 0)),
		}
		commands, err := NewWorkflowExecutor(result.Workflow, events).Execute()
		if got := terminalOutcome(t, commands, err); got != want {
			t.Errorf("signal %s: Go = %s, 解释器 = %s", payload, want, got)
		}
	}
}
