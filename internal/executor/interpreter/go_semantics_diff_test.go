package interpreter

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/jsonvalue"
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
		{DiffSwitchOnAnyInitShadow, "DiffSwitchOnAnyInitShadow", `{"a": "x"}`},
		{DiffSwitchOnAnyInitShadow, "DiffSwitchOnAnyInitShadow", `{"a": 1}`},
		{DiffNilSlice, "DiffNilSlice", `{}`},
		{DiffNilMap, "DiffNilMap", `{}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a"}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a", "Tags": null, "Attrs": null}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Name": "a", "Tags": [], "Attrs": {}}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Tags": ["x"], "Attrs": {"k": "v"}}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Children": [{"Tags": null, "Attrs": null}, {"Tags": ["a"], "Attrs": {}}], "Lookup": {"k": null}}`},
		{DiffInputNilFields, "DiffInputNilFields", `{"Children": null, "Lookup": null}`},
		{DiffInitNilMap, "DiffInitNilMap", `{}`},
		{DiffMapInputNumbers, "DiffMapInputNumbers", `{"amount": 1, "list": [2, "x"], "nested": {"n": 3}}`},
		{DiffMapInputNumbers, "DiffMapInputNumbers", `{"amount": 1.5}`},
		{DiffMapInputNumbers, "DiffMapInputNumbers", `{"amount": "1"}`},
		{DiffMapInputNumbers, "DiffMapInputNumbers", `{"amount": 9007199254740993}`},
		{DiffMapInputNested, "DiffMapInputNested", `{"items": [{"name": "a"}, [1, 2], "s", 2, null, true], "meta": {"n": 3, "k": "v"}}`},
		{DiffMapInputNested, "DiffMapInputNested", `{"items": {"name": "a"}, "meta": [1]}`},
		{DiffMapInputNested, "DiffMapInputNested", `{}`},
		{DiffAssertNested, "DiffAssertNested", `{"items": [1, 2], "meta": {"a": 1}}`},
		{DiffAssertNested, "DiffAssertNested", `{"items": {"a": 1}, "meta": {"a": 1}}`},
		{DiffAssertNested, "DiffAssertNested", `{"items": [1], "meta": "x"}`},
		{DiffAssertNested, "DiffAssertNested", `{"meta": {}}`},
		{DiffInputAny, "DiffInputAny", `{"V": 1, "M": {"n": 1}}`},
		{DiffInputAny, "DiffInputAny", `{"V": 1.5, "M": {"n": 2, "list": [1, {"x": 3}]}}`},
		{DiffInputAny, "DiffInputAny", `{"V": "1", "M": {"n": "1"}}`},
		{DiffInputAny, "DiffInputAny", `{"V": 12345678901234567890, "M": {"n": 1e3}}`},
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

// runInterpretedDiff 生成 name 的 WLang 程序并由解释器执行；输入按存储读回 History 的方式解码（jsonvalue：整数是
// int64，带小数的是 float64）。
func runInterpretedDiff(t *testing.T, src []byte, name, input string) string {
	t.Helper()
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: name, FuncName: name, Filename: diffWorkflowsFile})
	if err != nil {
		t.Fatalf("%s: generate: %v", name, err)
	}
	var payload map[string]any
	if err := jsonvalue.Unmarshal([]byte(input), &payload); err != nil {
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
	// 数字按字面量比较（UseNumber），超过 2^53 的整数不在比较前丢失精度。
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(generic)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
