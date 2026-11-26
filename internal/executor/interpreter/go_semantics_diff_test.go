package interpreter

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
)

const diffWorkflowsFile = "go_semantics_diff_workflows_test.go"

// TestGoSemanticsDifferential 用 go_semantics_diff_workflows_test.go 里的函数做差分：原生 Go 直接调用的结果
// （输入按 encoding/json 解码）与解释器执行翻译产物的结果（输入按 History 的方式解码）必须相同。
// Go 里 panic 的写法，解释器以 workflow_panic 让 Workflow Task 失败。
func TestGoSemanticsDifferential(t *testing.T) {
	src, err := os.ReadFile(diffWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		fn    any
		name  string
		input string
	}{
		{DiffAnyEqualsTrue, "DiffAnyEqualsTrue", `{"approved": true}`},
		{DiffAnyEqualsTrue, "DiffAnyEqualsTrue", `{"approved": "true"}`},
		{DiffAnyEqualsTrue, "DiffAnyEqualsTrue", `{}`},
		{DiffAnyEqualsString, "DiffAnyEqualsString", `{"action": "cancel"}`},
		{DiffAnyEqualsString, "DiffAnyEqualsString", `{"action": "ship"}`},
		{DiffAnyNotEqualsString, "DiffAnyNotEqualsString", `{"status": "approved"}`},
		{DiffAnyNotEqualsString, "DiffAnyNotEqualsString", `{"status": 1}`},
		{DiffStringEqualsAny, "DiffStringEqualsAny", `{"status": "approved"}`},
		{DiffAnyNumberFromJSON, "DiffAnyNumberFromJSON", `{"n": 3}`},
		{DiffAnyNumberFromJSON, "DiffAnyNumberFromJSON", `{"n": "3"}`},
		{DiffAnyLocalInt, "DiffAnyLocalInt", `{}`},
		{DiffAnyEqualsAny, "DiffAnyEqualsAny", `{"a": "v", "b": "v"}`},
		{DiffAnyEqualsAny, "DiffAnyEqualsAny", `{"a": "v", "b": 1}`},
		{DiffAnyStructEquals, "DiffAnyStructEquals", `{}`},
		{DiffSwitchOnAny, "DiffSwitchOnAny", `{"kind": "a"}`},
		{DiffSwitchOnAny, "DiffSwitchOnAny", `{"kind": "c"}`},
		{DiffSwitchOnAny, "DiffSwitchOnAny", `{"kind": 1}`},
		{DiffSwitchOnAny, "DiffSwitchOnAny", `{}`},
		{DiffSwitchOnAnyWithInit, "DiffSwitchOnAnyWithInit", `{"kind": true}`},
		{DiffSwitchOnAnyWithInit, "DiffSwitchOnAnyWithInit", `{"kind": "b"}`},
		{DiffSwitchOnAnyWithInit, "DiffSwitchOnAnyWithInit", `{"kind": "true"}`},
		{DiffNilSlice, "DiffNilSlice", `{}`},
		{DiffNilMap, "DiffNilMap", `{}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a"}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a", "Tags": null, "Attrs": null}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a", "Tags": [], "Attrs": {}}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Tags": ["x"], "Attrs": {"k": "v"}}`},
		{DiffInitNilMap, "DiffInitNilMap", `{}`},
		{DiffInitNilMap, "DiffInitNilMap", `{"Attrs": {"a": "b"}}`},
		{DiffMissingKeyStruct, "DiffMissingKeyStruct", `{}`},
		{DiffMissingKeyStructCommaOK, "DiffMissingKeyStructCommaOK", `{}`},
		{DiffMissingKeyStructArgument, "DiffMissingKeyStructArgument", `{}`},
		{DiffSliceWithinCap, "DiffSliceWithinCap", `{}`},
		{DiffSizedSliceWithCap, "DiffSizedSliceWithCap", `{}`},
		{DiffSliceBeyondCap, "DiffSliceBeyondCap", `{}`},
		{DiffAnyUncomparable, "DiffAnyUncomparable", `{}`},
	} {
		want := runNativeDiff(t, tc.fn, tc.input)
		got := runInterpretedDiff(t, src, tc.name, tc.input)
		if got != want {
			t.Errorf("%s(%s): Go = %s, 解释器 = %s", tc.name, tc.input, want, got)
		}
	}
}

// runNativeDiff 原生调用 fn，返回规范化的输出 JSON；fn panic 时返回 "panic"。
func runNativeDiff(t *testing.T, fn any, input string) (outcome string) {
	t.Helper()
	value := reflect.ValueOf(fn)
	in := reflect.New(value.Type().In(1))
	if err := json.Unmarshal([]byte(input), in.Interface()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() != nil {
			outcome = "panic"
		}
	}()
	results := value.Call([]reflect.Value{reflect.Zero(value.Type().In(0)), in.Elem()})
	if err, _ := results[1].Interface().(error); err != nil {
		return "error: " + err.Error()
	}
	return canonicalJSON(t, results[0].Interface())
}

// runInterpretedDiff 生成 name 的 WLang 程序并由解释器执行；输入按 History 事件的方式解码（数字是 json.Number）。
func runInterpretedDiff(t *testing.T, src []byte, name, input string) string {
	t.Helper()
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: name, FuncName: name, Filename: diffWorkflowsFile})
	if err != nil {
		t.Fatalf("%s: generate: %v", name, err)
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(input)))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowStartedEvent(payload, "", "", "", "", 0)
	started.ID = 1
	started.Timestamp = time.Date(2026, 11, 27, 8, 0, 0, 0, time.UTC)
	commands, err := NewWorkflowExecutor(result.Workflow, []history.Event{started}).Execute()
	if err != nil {
		if executor.WorkflowTaskBlockedCause(err) == history.WorkflowTaskFailedCauseWorkflowPanic {
			return "panic"
		}
		return "execute: " + err.Error()
	}
	for _, command := range commands {
		switch command.Type {
		case history.CmdCompleteWorkflow:
			return canonicalJSON(t, command.Output)
		case history.CmdFailWorkflow:
			return "error: " + command.Error
		}
	}
	return "no terminal command"
}

// Signal 的 Get 目标是 map[string]any 时，按载荷字段与字面量比较分支，结果与 Go 相同。
const signalPayloadBranchSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Decision string
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var d map[string]any
	if err := workflow.WaitSignal(ctx, "approval-result").Get(ctx, &d); err != nil {
		return Out{}, err
	}
	if d["approved"] == true {
		return Out{Decision: "ship"}, nil
	}
	if d["action"] == "cancel" {
		return Out{Decision: "cancel"}, nil
	}
	return Out{Decision: "refund"}, nil
}
`

func TestSignalPayloadComparisonFollowsGoSemantics(t *testing.T) {
	for _, tc := range []struct {
		payload map[string]any
		want    string
	}{
		{map[string]any{"approved": true}, "ship"},
		{map[string]any{"approved": false, "action": "cancel"}, "cancel"},
		{map[string]any{"approved": "true"}, "refund"},
	} {
		add, wft := signalHistory(t, signalPayloadBranchSrc)
		add(history.NewWorkflowStartedEvent(nil, "", "", "", "", 0))
		wft()
		add(history.NewWorkflowSignaledEvent("approval-result", tc.payload))
		out, ok := completedOutput(wft())
		if !ok {
			t.Fatalf("payload %v: workflow did not complete", tc.payload)
		}
		if out["Decision"] != tc.want {
			t.Errorf("Signal 载荷 %v：Decision = %q, Go 里是 %q", tc.payload, out["Decision"], tc.want)
		}
	}
}

func canonicalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var generic any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(generic)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
