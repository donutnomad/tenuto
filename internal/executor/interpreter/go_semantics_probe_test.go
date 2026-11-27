package interpreter

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

const probeWorkflowsFile = "go_semantics_probe_workflows_test.go"

// TestGoSemanticsProbes：go_semantics_probe_workflows_test.go 里的函数原生调用的结果（输入按 encoding/json 解码）
// 与解释器执行生成程序的结果（输入按 History 的方式解码）相同，冷重放与 sticky 会话两条路径都比较。覆盖结构体值
// 语义、slice 别名、map 增删改查、any 与载荷、具名基础类型字段、make 的容量（cap、超出长度的切片、append 共享底层数组）、nil slice 与 map 零值、
// 指针比较。
func TestGoSemanticsProbes(t *testing.T) {
	src, err := os.ReadFile(probeWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		fn    any
		name  string
		input string
	}{
		{SemStructCopyFromSlice, "SemStructCopyFromSlice", `{}`},
		{SemRangeCopy, "SemRangeCopy", `{}`},
		{SemMapStructCopy, "SemMapStructCopy", `{}`},
		{SemMapStructCopyCommaOK, "SemMapStructCopyCommaOK", `{}`},
		{SemStructAssignCopy, "SemStructAssignCopy", `{}`},
		{SemFuncParamCopy, "SemFuncParamCopy", `{}`},
		{SemStoreThenModify, "SemStoreThenModify", `{}`},
		{SemInputStructCopy, "SemInputStructCopy", `{"Order": {"Main": {"Qty": 1}}, "Items": [{"Qty": 2}]}`},
		{SemInputMapStructCopy, "SemInputMapStructCopy", `{"ByName": {"a": {"Qty": 1}}}`},
		{SemStructReturnedCopy, "SemStructReturnedCopy", `{}`},
		{SemClosureStructCopy, "SemClosureStructCopy", `{}`},
		{SemSliceAlias, "SemSliceAlias", `{}`},
		{SemAppendBeyondCapCopies, "SemAppendBeyondCapCopies", `{}`},
		{SemSubsliceAppendOverwrites, "SemSubsliceAppendOverwrites", `{}`},
		{SemCopyBuiltin, "SemCopyBuiltin", `{}`},
		{SemDeleteDuringRange, "SemDeleteDuringRange", `{}`},
		{SemRemoveFromSlice, "SemRemoveFromSlice", `{}`},
		{SemRangeOverModifiedSlice, "SemRangeOverModifiedSlice", `{}`},
		{SemMapCRUD, "SemMapCRUD", `{}`},
		{SemMapOfSlices, "SemMapOfSlices", `{}`},
		{SemMapOfStructUpdate, "SemMapOfStructUpdate", `{"Items": [{"Name": "a", "Qty": 1}, {"Name": "b", "Qty": 2}, {"Name": "a", "Qty": 3}]}`},
		{SemMapPointerValues, "SemMapPointerValues", `{"Ptrs": {"a": {"Qty": 1}, "b": null}}`},
		{SemPointerEquality, "SemPointerEquality", `{"Ptrs": {"a": {"Qty": 1}, "b": {"Qty": 1}}}`},
		// 指向基础类型、slice 与 map 的指针内容不同时，按内容比较的结果与 Go 相同，不 panic。
		{SemDifferenceScalarPointers, "SemDifferenceScalarPointers", `{"A":1,"B":2,"S":"x","T":"y","L":[1],"M":[2],"N":{"k":1},"O":{"k":2},"P":{"x":1,"y":2}}`},
		{SemDifferenceScalarPointers, "SemDifferenceScalarPointers", `{}`},
		{SemAnyNilPointer, "SemAnyNilPointer", `{}`},
		{SemAnyTypedContainers, "SemAnyTypedContainers", `{}`},
		{SemPointerIdentity, "SemPointerIdentity", `{"Ptrs": {"a": {"Qty": 1}, "b": {"Qty": 1}}}`},
		{SemAnyFieldFromInput, "SemAnyFieldFromInput", `{}`},
		{SemAnyFieldFromInput, "SemAnyFieldFromInput", `{"V": "x"}`},
		{SemAnyFieldFromInput, "SemAnyFieldFromInput", `{"V": 1}`},
		{SemAnyFieldFromInput, "SemAnyFieldFromInput", `{"V": true}`},
		{SemAnyFieldFromInput, "SemAnyFieldFromInput", `{"V": null}`},
		{SemTypeSwitchPayload, "SemTypeSwitchPayload", `{"s": "x", "n": 2, "b": true, "l": [1, 2], "m": {"a": 1}, "z": null}`},
		{SemAnyMapMutation, "SemAnyMapMutation", `{}`},
		{SemAnyNestedCompare, "SemAnyNestedCompare", `{"meta": {"status": "paid", "retries": 2}}`},
		{SemSwitchInitInLoop, "SemSwitchInitInLoop", `{}`},
		{SemSwitchInitAnyInLoop, "SemSwitchInitAnyInLoop", `{"list": ["x", 1, "skip", 1, "stop", "y"]}`},
		{SemSwitchInitReturn, "SemSwitchInitReturn", `{"v": "a"}`},
		{SemSwitchInitReturn, "SemSwitchInitReturn", `{"v": "b"}`},
		{SemLabeledContinue, "SemLabeledContinue", `{}`},
		{SemClosureLoopCapture, "SemClosureLoopCapture", `{}`},
		{SemDeferInLoop, "SemDeferInLoop", `{}`},
		{SemErrorsSprintf, "SemErrorsSprintf", `{}`},
		{SemAppendNilLocal, "SemAppendNilLocal", `{}`},
		{SemAppendMissingMapKey, "SemAppendMissingMapKey", `{}`},
		{SemAppendPresentMapKey, "SemAppendPresentMapKey", `{}`},
		{SemAppendInputNilField, "SemAppendInputNilField", `{}`},
		{SemAppendAfterCapCopy, "SemAppendAfterCapCopy", `{}`},
		{SemAppendCapSelf, "SemAppendCapSelf", `{}`},
		{SemMakeCapAlias, "SemMakeCapAlias", `{}`},
		{SemMakeSliceBeyondLen, "SemMakeSliceBeyondLen", `{}`},
		{SemAppendSizedCopy, "SemAppendSizedCopy", `{}`},
		{SemAppendCapPassToFunc, "SemAppendCapPassToFunc", `{}`},
		{SemAppendRuntimeZeroLenCap, "SemAppendRuntimeZeroLenCap", `{}`},
		{SemAppendRuntimeZeroLenCap, "SemAppendRuntimeZeroLenCap", `{"Items":[{"Qty":1}]}`},
		{SemAppendCapStruct, "SemAppendCapStruct", `{"Items":[{"Qty":1},{"Qty":2}]}`},
		{SemAppendCapStruct, "SemAppendCapStruct", `{"Items":[]}`},
		{SemAppendCapStrings, "SemAppendCapStrings", `{}`},
		{SemAppendCapSpread, "SemAppendCapSpread", `{"Order":{"Main":{"Tags":["a"]}}}`},
		{SemPreallocAppend, "SemPreallocAppend", `{}`},
		{SemPreallocAppend, "SemPreallocAppend", `{"Order": {"ID": "o1"}, "Items": [{"Name": "a", "Qty": 1}, {"Name": "b", "Qty": 2}]}`},
		{SemAppendZeroStructField, "SemAppendZeroStructField", `{}`},
		{SemAppendAfterNilAssign, "SemAppendAfterNilAssign", `{}`},
		{SemAppendMissingInputMapKey, "SemAppendMissingInputMapKey", `{"Items":[{"Name":"a"},{"Name":"b"},{"Name":"a"}]}`},
		{SemAppendMissingAnyMapKey, "SemAppendMissingAnyMapKey", `{}`},
		{SemRangeLenMissingSlice, "SemRangeLenMissingSlice", `{}`},
		{SemNilSlices, "SemNilSlices", `{}`},
		{SemNilSlices, "SemNilSlices", `{"Items": [{"Name": "a", "Qty": 1}, {"Name": "b", "Qty": 3}]}`},
		{SemMissingKeyCollections, "SemMissingKeyCollections", `{"Items": [{"Name": "a", "Tags": ["x"]}, {"Name": "b"}, {"Name": "a", "Tags": ["y", "z"]}]}`},
		{SemCapConstLen, "SemCapConstLen", `{}`},
		{SemCapConstLenInput, "SemCapConstLenInput", `{}`},
		{SemNullFields, "SemNullFields", `{"Order": null}`},
		{SemNullFields, "SemNullFields", `{"Order": {"Main": null, "ID": null}}`},
		{SemNullFields, "SemNullFields", `{"Order": {"Main": {"Name": null, "Qty": null}}}`},
		{SemNullFields, "SemNullFields", `{"Status": null, "ID": null}`},
		{SemNullFields, "SemNullFields", `{"Order": {"Items": [null]}}`},
		{SemNamedStringField, "SemNamedStringField", `{}`},
		{SemNamedStringField, "SemNamedStringField", `{"Status": "pending"}`},
		{SemNamedStringField, "SemNamedStringField", `{"Status": "paid"}`},
		{SemNamedStringField, "SemNamedStringField", `{"Status": ""}`},
		{SemNamedStringField, "SemNamedStringField", `{"Status": null}`},
		{SemZeroBasicFields, "SemZeroBasicFields", `{}`},
		{SemZeroBasicFields, "SemZeroBasicFields", `{"Name": null, "Qty": null, "OK": null}`},
		{SemNamedVariants, "SemNamedVariants", `{"Status": "paid", "Prio": 2}`},
		{SemNamedVariants, "SemNamedVariants", `{"Status": "pending"}`},
		{SemNamedIntOrder, "SemNamedIntOrder", `{"Status": "paid", "Prio": 2}`},
		{SemNamedIntOrder, "SemNamedIntOrder", `{}`},
		{SemNamedOrderLines, "SemNamedOrderLines", `{"Prio": 2, "Lines": [{"SKU": "a", "Status": "paid", "Prio": 2}, {"SKU": "b", "Status": "pending"}, {"SKU": "c"}, {"SKU": "d", "Status": "void", "Prio": 3}], "Statuses": ["pending", "paid"], "Limits": {"paid": 3}}`},
		{SemNamedOrderLines, "SemNamedOrderLines", `{"Lines": null, "Statuses": null, "Limits": null}`},
		{SemNamedConversions, "SemNamedConversions", `{"Lines": [{"SKU": "a", "Status": "paid"}, {"SKU": "b", "Status": "pending"}, {"SKU": "c", "Status": "paid"}]}`},
		{SemHelperRewrites, "SemHelperRewrites", `{"action":"approve"}`},
		{SemHelperRewrites, "SemHelperRewrites", `{"urgent":true}`},
		{SemDurationMap, "SemDurationMap", `{}`},
		{SemSwitchInitTerminating, "SemSwitchInitTerminating", `{"kind":"a"}`},
		{SemSwitchInitTerminating, "SemSwitchInitTerminating", `{"kind":"z"}`},
		{SemSwitchInitTerminatingMain, "SemSwitchInitTerminatingMain", `{"kind":"a"}`},
	} {
		want := runNativeDiff(t, tc.fn, tc.input)
		cold, sticky := runInputWorkflow(t, generateProbe(t, src, tc.name), tc.input)
		if cold != want || sticky != want {
			t.Errorf("%s(%s):\n  Go     = %s\n  冷重放 = %s\n  sticky = %s", tc.name, tc.input, want, cold, sticky)
		}
	}
}

// TestGoSemanticsDocumentedDifferences：go-subset 文档「与 Go 的差异」列出的写法，解释器在两条路径上的结果与文档
// 描述一致（与原生 Go 不同）。
func TestGoSemanticsDocumentedDifferences(t *testing.T) {
	src, err := os.ReadFile(probeWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		fn          any
		name, input string
		want        string
	}{
		// any 里 int 与 int64 不区分（1、100、10000），具名类型与底层类型不区分（1000）。
		{SemDifferenceAnyTyped, "SemDifferenceAnyTyped", `{"ID": 3, "Status": "ok"}`, `{"Count":11101,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
		{SemDifferenceAnyNumbers, "SemDifferenceAnyNumbers", `{"n": 3}`, `{"Count":100,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
		// 类型化的 slice、map 存进 map[string]any、[]any 之后断言成 []any、map[string]any 成立（1、10）。
		{SemDifferenceAnyContainerNested, "SemDifferenceAnyContainerNested", `{}`, `{"Count":11,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
		{SemDifferenceAnyNamed, "SemDifferenceAnyNamed", `{"Status": "ok"}`, `{"Count":11,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
		// 指针放进 any 后比较（1）、含指针字段的结构体比较（10）按内容。
		{SemDifferencePointerContent, "SemDifferencePointerContent", `{"Plain": {"a": {"Qty": 1}, "b": {"Qty": 1}}}`, `{"Count":11,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
		{SemDifferencePointerInAnyPanics, "SemDifferencePointerInAnyPanics", `{"Ptrs": {"a": {"Qty": 1}, "b": {"Qty": 1}}}`, "panic"},
		// 指向基础类型、具名基础类型、slice、map 的指针按内容比较：内容相同的两个对象相等（1、100、1000、10000、100000）。
		{SemDifferenceScalarPointers, "SemDifferenceScalarPointers", `{"A":1,"B":1,"S":"x","T":"x","L":[1],"M":[1],"N":{"k":1},"O":{"k":1},"P":{"x":1,"y":1}}`, `{"Count":111111,"Flag":false,"List":null,"Sum":0,"Text":"","Value":null}`},
	} {
		native := runNativeDiff(t, tc.fn, tc.input)
		if native == tc.want {
			t.Fatalf("%s: 原生 Go 的结果 %s 与解释器相同，不再是差异，从文档与本表删除", tc.name, native)
		}
		cold, sticky := runInputWorkflow(t, generateProbe(t, src, tc.name), tc.input)
		if cold != tc.want || sticky != tc.want {
			t.Errorf("%s(%s): 文档 = %s，冷重放 = %s，sticky = %s（Go = %s）", tc.name, tc.input, tc.want, cold, sticky, native)
		}
	}
}

// TestNamedBasicTypesInResults：Activity 与子 Workflow 的结果解进结构体，具名字符串、具名整数字段与常量比较、
// switch、有序比较的分支与 Go 相同。
func TestNamedBasicTypesInResults(t *testing.T) {
	src, err := os.ReadFile(probeWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"SemActivityStatusBranch", "SemChildStatusBranch"} {
		result := generateProbe(t, src, name)
		for _, payload := range []string{`{"Status": "paid"}`, `{"Status": "paid", "Prio": 2}`, `{"Status": "pending"}`, `{"Status": "void"}`, `{}`, `{"Status": null}`} {
			want := canonicalJSON(t, SemOut{Text: nativeRoute(t, payload)})
			cold, sticky := runResultWorkflow(t, result, payload)
			if cold != want || sticky != want {
				t.Errorf("%s 结果 %s：Go = %s，冷重放 = %s，sticky = %s", name, payload, want, cold, sticky)
			}
		}
	}
}

// TestNamedBasicTypesInSignal：Signal 解进结构体，具名类型字段的分支与 Go 相同。
func TestNamedBasicTypesInSignal(t *testing.T) {
	src, err := os.ReadFile(probeWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	result := generateProbe(t, src, "SemSignalStatusBranch")
	for _, payload := range []string{`{"Status": "paid", "Prio": 2}`, `{"Status": "pending"}`, `{}`} {
		want := canonicalJSON(t, SemOut{Text: nativeRoute(t, payload)})
		var signal map[string]any
		if err := jsonvalue.Unmarshal([]byte(payload), &signal); err != nil {
			t.Fatal(err)
		}
		cold, sticky := runSecondTask(t, result, []history.Event{history.NewWorkflowSignaledEvent("task", signal)}, func(commands []history.Command, err error) string {
			return terminalOutcome(t, commands, err)
		})
		if cold != want || sticky != want {
			t.Errorf("Signal %s：Go = %s，冷重放 = %s，sticky = %s", payload, want, cold, sticky)
		}
	}
}

// TestNamedBasicTypesInUpdate：Update 参数解进结构体，具名类型字段的分支与具名整数的算术与 Go 相同。
func TestNamedBasicTypesInUpdate(t *testing.T) {
	src, err := os.ReadFile(probeWorkflowsFile)
	if err != nil {
		t.Fatal(err)
	}
	result := generateProbe(t, src, "SemUpdateStatusBranch")
	for _, payload := range []string{`{"Status": "paid", "Prio": 2}`, `{"Status": "paid", "Prio": 1}`, `{"Status": "pending"}`, `{"Status": null}`} {
		var task SemTask
		if err := json.Unmarshal([]byte(payload), &task); err != nil {
			t.Fatal(err)
		}
		want := canonicalJSON(t, SemOut{Text: semRoute(task.Status, task.Prio), Count: int(task.Prio) + 1})
		accepted := acceptedArgsEvent(0, "route", "u-1", protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(payload)}})
		cold, sticky := runSecondTask(t, result, []history.Event{accepted}, func(commands []history.Command, err error) string {
			if err != nil {
				return "execute: " + err.Error()
			}
			completes := findUpdateCompletes(commands)
			if len(completes) != 1 {
				return "no update completion"
			}
			return canonicalJSON(t, updateOutcomeOutput(t, completes[0]))
		})
		if cold != want || sticky != want {
			t.Errorf("Update %s：Go = %s，冷重放 = %s，sticky = %s", payload, want, cold, sticky)
		}
	}
}

func nativeRoute(t *testing.T, payload string) string {
	t.Helper()
	var task SemTask
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		t.Fatal(err)
	}
	return semRoute(task.Status, task.Prio)
}

func generateProbe(t *testing.T, src []byte, name string) sdkgen.Result {
	t.Helper()
	result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: name, FuncName: name, Filename: probeWorkflowsFile})
	if err != nil {
		t.Fatalf("%s: generate: %v", name, err)
	}
	return result
}

// runInputWorkflow 在一个 Workflow Task 里执行只依赖输入的 Workflow，返回冷重放（按 History 新建执行器）与 sticky
// 会话（ExecutionSession 激活）的结果；输入按存储读回 History 的方式解码（jsonvalue，整数是 int64）。Workflow Task
// 以 workflow_panic 失败时结果是 "panic"。
func runInputWorkflow(t *testing.T, result sdkgen.Result, input string) (cold, sticky string) {
	t.Helper()
	var payload map[string]any
	if err := jsonvalue.Unmarshal([]byte(input), &payload); err != nil {
		t.Fatal(err)
	}
	first := firstWorkflowTask(payload)
	commands, err := NewWorkflowExecutor(result.Workflow, first).Execute()
	cold = probeOutcome(t, commands, err)

	session := NewExecutionSession(context.Background(), result.Workflow, 1)
	defer session.Close()
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	activation, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "first", ExecutionGeneration: 1, Events: first, ThroughEventID: 3})
	sticky = probeOutcome(t, activation.Commands, err)
	return cold, sticky
}

// runSecondTask 执行在第一个 Workflow Task 里阻塞、由 arrivals（Signal、Update 等外部事件）唤醒的 Workflow：
// arrivals 之后是第二个 Workflow Task。返回第二个 Workflow Task 在冷重放与 sticky 会话上经 outcome 得到的结果。
func runSecondTask(t *testing.T, result sdkgen.Result, arrivals []history.Event, outcome func([]history.Command, error) string) (cold, sticky string) {
	t.Helper()
	first := firstWorkflowTask(map[string]any{})
	if _, err := NewWorkflowExecutor(result.Workflow, first).Execute(); err != nil {
		t.Fatalf("first workflow task: %v", err)
	}
	completed := []history.Event{executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker"))}
	var second []history.Event
	for _, event := range arrivals {
		second = append(second, executionSessionEvent(int64(5+len(second)), event))
	}
	scheduled := int64(5 + len(second))
	second = append(second,
		executionSessionEvent(scheduled, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(scheduled+1, history.NewWorkflowTaskStartedEvent(scheduled, "worker", "build", 0)))

	all := append(append(append([]history.Event(nil), first...), completed...), second...)
	cold = outcome(NewWorkflowExecutor(result.Workflow, all).Execute())

	session := NewExecutionSession(context.Background(), result.Workflow, 1)
	defer session.Close()
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	activation, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "first", ExecutionGeneration: 1, Events: first, ThroughEventID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(history.WorkflowTaskCompletionAck{
		Accepted: true, CommittedThroughEventID: 4, CommittedEvents: completed,
		ActivationID: activation.ActivationID, CommandDigest: activation.CommandDigest, ExecutionGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	activation, err = session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "second", ExecutionGeneration: 1, Events: second, ThroughEventID: scheduled + 1})
	sticky = outcome(activation.Commands, err)
	return cold, sticky
}

func firstWorkflowTask(input map[string]any) []history.Event {
	return []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(input, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(3, history.NewWorkflowTaskStartedEvent(2, "worker", "build", 0)),
	}
}

func probeOutcome(t *testing.T, commands []history.Command, err error) string {
	t.Helper()
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

const sideEffectMapSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Flags int
	Kind  string
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var v map[string]any
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) map[string]any {
		return map[string]any{"ratio": 1.0, "n": 2}
	}).Get(ctx, &v); err != nil {
		return Out{}, err
	}
	flags := 0
	if v["ratio"] == 1.0 {
		flags += 1
	}
	if v["n"] == 2.0 {
		flags += 10
	}
	if v["n"] == 2 {
		flags += 100
	}
	kind := "other"
	switch v["n"].(type) {
	case float64:
		kind = "float64"
	case int:
		kind = "int"
	case int64:
		kind = "int64"
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return Out{}, err
	}
	return Out{Flags: flags, Kind: kind}, nil
}
`

// TestSideEffectMapValueLiveMatchesReplay：SideEffect 的值 Get 进 map[string]any 时，sticky（live 执行闭包）与
// 冷重放（读回持久化后的 marker）都与 Go 一样得到 float64（Temporal SDK 语义：值经 JSON 编码后由 Get 解码）。
func TestSideEffectMapValueLiveMatchesReplay(t *testing.T) {
	result, err := sdkgen.GenerateFromSource([]byte(sideEffectMapSrc), sdkgen.Options{WorkflowType: "W", FuncName: "W", Filename: "flows.go"})
	if err != nil {
		t.Fatal(err)
	}
	first := firstWorkflowTask(map[string]any{})
	commands, err := NewWorkflowExecutor(result.Workflow, first).Execute()
	if err != nil {
		t.Fatal(err)
	}
	var marker, timer history.Command
	for _, c := range commands {
		switch c.Type {
		case history.CmdRecordMarker:
			marker = c
		case history.CmdScheduleTimer:
			timer = c
		}
	}
	// 持久化后读回：marker 的值按存储的方式（JSON → jsonvalue）解码。
	encoded, _ := json.Marshal(marker.Input)
	var stored map[string]any
	if err := jsonvalue.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	all := append(append([]history.Event(nil), first...),
		executionSessionEvent(4, history.NewWorkflowTaskCompletedEvent(2, "worker")),
		executionSessionEvent(5, history.NewMarkerRecordedEvent(marker.MarkerName, marker.MarkerID, stored)),
		executionSessionEvent(6, history.NewTimerScheduledEvent(timer.TimerID, map[string]any{"delay_ns": int64(timer.Delay)}, timer.Delay.Milliseconds())),
		executionSessionEvent(7, history.NewTimerFiredEvent(timer.TimerID, 6)),
		executionSessionEvent(8, history.NewWorkflowTaskScheduledEvent(0)),
		executionSessionEvent(9, history.NewWorkflowTaskStartedEvent(8, "worker", "build", 0)),
	)
	coldCommands, err := NewWorkflowExecutor(result.Workflow, all).Execute()
	cold := terminalOutcome(t, coldCommands, err)

	session := NewExecutionSession(context.Background(), result.Workflow, 1)
	defer session.Close()
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatal(err)
	}
	activation, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "first", ExecutionGeneration: 1, Events: first, ThroughEventID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(history.WorkflowTaskCompletionAck{
		Accepted: true, CommittedThroughEventID: 6, CommittedEvents: all[3:6],
		ActivationID: activation.ActivationID, CommandDigest: activation.CommandDigest, ExecutionGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	activation, err = session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "second", ExecutionGeneration: 1, Events: all[6:], ThroughEventID: 9})
	sticky := terminalOutcome(t, activation.Commands, err)
	want := canonicalJSON(t, map[string]any{"Flags": 11, "Kind": "float64"})
	if cold != sticky || cold != want {
		t.Errorf("Go = %s\n  sticky（live 执行闭包）= %s\n  冷重放（读回 marker）= %s", want, sticky, cold)
	}
}
