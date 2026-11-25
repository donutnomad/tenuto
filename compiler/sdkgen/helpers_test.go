package sdkgen_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// Workflow 代码里的标准库函数与同包普通函数
// ============================================
// 测试策略:
// - Happy Path: helper（普通函数、递归、多返回值、值接收者方法、带 ctx 调度 Activity 与 timer）与
//   strings/strconv/fmt/math 在解释器里按 Go 语义求值
// - Happy Path: helper 分布在包的多个文件里，经桥调度 Activity；Activity 契约照常登记
// - Happy Path: helper 的函数体在产物里，改 helper 改变内容版本
// - Error Cases: 每种拒绝写法报 file:line（含 helper 所在的其他文件）
// ============================================

const helperWorkflowSrc = `package flows

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

type Line struct {
	SKU   string
	Qty   int
	Cents int
}

func (l Line) Total() int { return l.Qty * l.Cents }

func normalize(sku string) string { return strings.ToUpper(strings.TrimSpace(sku)) }

func fib(n int) int {
	if n < 2 {
		return n
	}
	return fib(n-1) + fib(n-2)
}

func parseQty(raw string) (int, error) {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("bad qty %s: %w", raw, err)
	}
	return n, nil
}

func reserve(ctx workflow.Context, sku string) (string, error) {
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, map[string]any{"sku": sku}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	if err != nil {
		return "", err
	}
	id, _ := out["id"].(string)
	return id, nil
}

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	raw, _ := input["qty"].(string)
	qty, err := parseQty(raw)
	if err != nil {
		return nil, err
	}
	line := Line{SKU: normalize(" ab-1 "), Qty: qty, Cents: 250}
	id, err := reserve(ctx, line.SKU)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"label":   fmt.Sprintf("%s x%d = %05d", line.SKU, line.Qty, line.Total()),
		"fib":     fib(10),
		"max":     math.Max(1.5, 2.5),
		"reserve": id,
		"parts":   strings.Join(strings.Split("a-b-c", "-"), "+"),
	}, nil
}
`

func generateHelperWorkflow(t *testing.T, src string) *program.WorkflowConfig {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: "helper-flow", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return result.Workflow
}

func TestHelpersAndStdlibRunInInterpreter(t *testing.T) {
	wf := generateHelperWorkflow(t, helperWorkflowSrc)
	events := []historypkg.Event{historypkg.NewWorkflowStartedEvent(map[string]any{"qty": "3"}, "", "", "helper-flow", "", 0)}
	add := func(event historypkg.Event) {
		event.ID = int64(len(events) + 1)
		event.Timestamp = time.Unix(1_800_000_000, 0)
		events = append(events, event)
	}
	events[0].ID = 1

	commands, err := interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleTimer {
		t.Fatalf("first task: want the helper's timer, got %+v", commands)
	}
	add(historypkg.Event{WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
	add(historypkg.NewTimerScheduledEvent(commands[0].TimerID, nil, commands[0].Delay.Milliseconds()))
	add(historypkg.NewTimerFiredEvent(commands[0].TimerID, int64(len(events))))

	commands, err = interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleActivity || commands[0].Input["sku"] != "AB-1" {
		t.Fatalf("second task: want the helper's activity for AB-1, got %+v", commands)
	}
	scheduled := commands[0]
	add(historypkg.Event{WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
	add(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: scheduled.ActivityID, ActivityType: scheduled.ActivityType}))
	add(historypkg.Event{ActivityCompleted: &historypkg.ActivityCompletedEvent{ActivityID: scheduled.ActivityID, ScheduledEventID: int64(len(events)), ActivityType: scheduled.ActivityType, Output: map[string]any{"id": "r-1"}}})

	commands, err = interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("third task: want completion, got %+v", commands)
	}
	want := map[string]any{"label": "AB-1 x3 = 00750", "fib": int64(55), "max": 2.5, "reserve": "r-1", "parts": "a+b+c"}
	for key, value := range want {
		if commands[0].Output[key] != value {
			t.Fatalf("output %s = %#v, want %#v (all: %+v)", key, commands[0].Output[key], value, commands[0].Output)
		}
	}
}

// strconv.Atoi 的错误经 helper 用 %w 包装后作为 Workflow 的失败返回，文本与 Go 相同。
func TestHelperErrorFailsWorkflowWithGoErrorText(t *testing.T) {
	wf := generateHelperWorkflow(t, helperWorkflowSrc)
	events := []historypkg.Event{historypkg.NewWorkflowStartedEvent(map[string]any{"qty": "x"}, "", "", "helper-flow", "", 0)}
	events[0].ID = 1
	commands, err := interpreter.NewWorkflowExecutor(wf, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdFailWorkflow {
		t.Fatalf("want CmdFailWorkflow, got %+v", commands)
	}
	if want := `bad qty x: strconv.Atoi: parsing "x": invalid syntax`; !strings.Contains(commands[0].Error, want) {
		t.Fatalf("error %q does not contain %q", commands[0].Error, want)
	}
}

// helper 的函数体在产物里：只改 helper 也改变 Workflow 的内容版本。
func TestHelperChangeChangesContentVersion(t *testing.T) {
	before, err := program.WorkflowConfigVersion(generateHelperWorkflow(t, helperWorkflowSrc))
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(helperWorkflowSrc, "return l.Qty * l.Cents", "return l.Qty*l.Cents + 1", 1)
	after, err := program.WorkflowConfigVersion(generateHelperWorkflow(t, changed))
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("changing a helper body must change the workflow content version")
	}
	again, err := program.WorkflowConfigVersion(generateHelperWorkflow(t, helperWorkflowSrc))
	if err != nil {
		t.Fatal(err)
	}
	if again != before {
		t.Fatal("generating the same source twice must give the same content version")
	}
}

// helper 分布在包的三个文件里，带 ctx 的 helper 经生成的桥调度 Activity。
func TestCrossFileHelpersScheduleThroughBridge(t *testing.T) {
	result := generateFixture(t, "helpers")
	ref, ok := result.Workflow.Activities["helpers.Charge"]
	if !ok || ref.Type != "payments.charge" || ref.TaskQueue != "payments" {
		t.Fatalf("activities = %+v, want helpers.Charge → payments.charge on payments", result.Workflow.Activities)
	}
	programJSON := string(mustMarshal(t, result.Workflow.Program))
	for _, want := range []string{"__wlang_func_charge", "__wlang_func_orderTotal", "__wlang_method_Item_Total", "__wlang_func_money"} {
		if !strings.Contains(programJSON, want) {
			t.Fatalf("program lacks %s\n%s", want, programJSON)
		}
	}
	if strings.Contains(programJSON, "CallCharge") {
		t.Fatalf("bridge call in a helper was not rewritten\n%s", programJSON)
	}
	input := map[string]any{"order_id": "o-1", "items": []any{
		map[string]any{"sku": "a", "qty": 2, "cents": 150},
		map[string]any{"sku": "b", "qty": 1, "cents": 5},
	}}
	events := []historypkg.Event{historypkg.NewWorkflowStartedEvent(input, "", "", "helpers-flow", "", 0)}
	events[0].ID = 1
	commands, err := interpreter.NewWorkflowExecutor(result.Workflow, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].ActivityType != "payments.charge" || fmt.Sprint(commands[0].Input["cents"]) != "305" {
		t.Fatalf("want payments.charge with 305 cents, got %+v", commands)
	}
	scheduled := commands[0]
	events = append(events,
		historypkg.Event{ID: 2, Timestamp: time.Unix(0, 2), WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}},
		historypkg.Event{ID: 3, Timestamp: time.Unix(0, 3), ActivityScheduled: &historypkg.ActivityScheduledEvent{ActivityID: scheduled.ActivityID, ActivityType: scheduled.ActivityType, TaskQueue: scheduled.TaskQueue, Input: scheduled.Input}},
		historypkg.Event{ID: 4, Timestamp: time.Unix(0, 4), ActivityCompleted: &historypkg.ActivityCompletedEvent{ActivityID: scheduled.ActivityID, ScheduledEventID: 3, ActivityType: scheduled.ActivityType, Output: map[string]any{"charge_id": "c-9"}}},
	)
	commands, err = interpreter.NewWorkflowExecutor(result.Workflow, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow ||
		commands[0].Output["label"] != "O-1 3.05" || commands[0].Output["charge_id"] != "c-9" {
		t.Fatalf("want completion with label O-1 3.05 and charge c-9, got %+v", commands)
	}
}

// 包内其他文件里的 helper 出错时，诊断指向那个文件的行号。
func TestCrossFileHelperDiagnosticNamesItsFile(t *testing.T) {
	dir := copyFixture(t, "helpers")
	steps := dir + "/steps.go"
	source, err := os.ReadFile(steps)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(source), `return fmt.Sprintf("%d.%02d", cents/100, cents%100)`, `return fmt.Sprintf("%d.%02d", cents/100, cents%100) + time.Now().String()`, 1)
	if err := os.WriteFile(steps, []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = generateFixtureIn(t, dir)
	if err == nil {
		t.Fatal("time.Now in a helper must fail generation")
	}
	for _, want := range []string{"steps.go:11", "time.Now", "workflow.Now(ctx)"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
}

func TestHelperRejections(t *testing.T) {
	const header = `package flows

import (
	"fmt"
	"strings"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

`
	cases := []struct {
		name  string
		decls string
		body  string
		want  []string
	}{
		{"pointer receiver", "type C struct{ N int }\n\nfunc (c *C) Inc() { c.N++ }\n", "c := C{}; c.Inc(); return c.N, nil", []string{"w.go:", "pointer receiver"}},
		{"generic helper", "func id[T any](v T) T { return v }\n", "return id(1), nil", []string{"w.go:10", "generic function id is not supported"}},
		{"variadic helper", "func sum(xs ...int) int { return len(xs) }\n", "return sum(1, 2), nil", []string{"w.go:10", "variadic function sum is not supported"}},
		{"package variable", "var limit = 3\n\nfunc get() int { return limit }\n", "return get(), nil", []string{"w.go:12", "package-level variable limit is not supported"}},
		{"function value", "func two() int { return 2 }\n", "f := two; return f(), nil", []string{"w.go:", "本包函数 two 不能作为值使用"}},
		{"method value", "type P struct{ N int }\n\nfunc (p P) Get() int { return p.N }\n", "f := P{N: 1}.Get; return f(), nil", []string{"w.go:", "方法 Get 不能作为值使用"}},
		{"stdlib in helper", "func title(s string) string { return strings.Title(s) }\n", "_ = title(\"x\"); return 0, nil", []string{"w.go:10", "strings.Title is not supported"}},
		{"Sprintf verb in helper", "func show(n int) string { return fmt.Sprintf(\"%s\", n) }\n", "_ = show(1); return 0, nil", []string{"w.go:10", "verb %s does not accept integer operand"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := header + tc.decls + "\nfunc W(ctx workflow.Context, input map[string]any) (int, error) {\n\t" + tc.body + "\n}\n"
			_, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowType: "reject", FuncName: "W", Filename: "w.go"})
			if err == nil {
				t.Fatal("want generation error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func generateFixture(t *testing.T, fixture string) sdkgen.PackageResult {
	t.Helper()
	result, err := generateFixtureIn(t, copyFixture(t, fixture))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return result
}

func generateFixtureIn(t *testing.T, dir string) (sdkgen.PackageResult, error) {
	t.Helper()
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("write bridge: %v", err)
		}
	}
	return sdkgen.Generate(plan)
}
