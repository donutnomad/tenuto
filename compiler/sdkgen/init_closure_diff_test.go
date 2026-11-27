package sdkgen_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// if/switch/for 初始化语句里的闭包：原生 Go 与解释器差分
// ============================================
// 测试策略:
// - Happy Path: 初始化语句里的闭包经桥调度 Activity（calc.double），同一份源码用原生 Go 直接执行、
//   用 sdkgen 产物在解释器里执行，每个输入的结果一致
// - Edge Cases: 闭包在 if 初始化里被赋给变量、作为参数传给 helper（`if v, err := apply(..., func...); err != nil`），
//   在 switch/for 初始化里被赋给变量后在分支与循环体里多次调用
// ============================================

// initClosureInputs 是每个 fixture 包的输入；覆盖各分支（if 条件真/假、switch 两个 case、for 零次/多次迭代）。
var initClosureInputs = map[string][]int{
	"ifinit":     {-2, 0, 3},
	"switchinit": {4, 7},
	"forinit":    {0, 5, 200},
}

func TestInitStatementClosuresMatchNativeGo(t *testing.T) {
	root := copyFixture(t, "initclosure")
	moduleRoot, modulePath := moduleRootAndPath(t)
	rootImport := modulePath + "/" + filepath.ToSlash(mustRel(t, moduleRoot, root))

	interpreted := map[string]int{}
	var bridges []string
	for pkg, inputs := range initClosureInputs {
		result := generateFixtureDir(t, filepath.Join(root, pkg))
		for _, bridge := range result.Bridges {
			bridges = append(bridges, bridge.Path)
		}
		for _, n := range inputs {
			interpreted[fmt.Sprintf("%s/%d", pkg, n)] = runDoublingWorkflow(t, result.Workflow, n)
		}
	}

	native := runInitClosureNative(t, moduleRoot, root, rootImport, bridges)
	if len(native) != len(interpreted) {
		t.Fatalf("原生结果 %v 与解释器结果 %v 的条目数不同", native, interpreted)
	}
	for key, want := range native {
		if got, ok := interpreted[key]; !ok || got != want {
			t.Errorf("%s: 解释器 = %d，原生 Go = %d", key, got, want)
		}
	}
}

// runDoublingWorkflow 在解释器里执行 Workflow：每条 calc.double 调度按 n*2 完成，直到 Workflow 完成，返回输出 n。
func runDoublingWorkflow(t *testing.T, wf *program.WorkflowConfig, n int) int {
	t.Helper()
	started := historypkg.NewWorkflowStartedEvent(map[string]any{"n": n}, "", "", wf.Type, "", 0)
	started.ID = 1
	events := []historypkg.Event{started}
	add := func(event historypkg.Event) {
		event.ID = int64(len(events) + 1)
		event.Timestamp = time.Unix(1_800_000_000, int64(event.ID))
		events = append(events, event)
	}
	for range 50 {
		commands, err := interpreter.NewWorkflowExecutor(wf, events).Execute()
		if err != nil {
			t.Fatalf("%s(%d): Execute: %v", wf.Type, n, err)
		}
		if len(commands) != 1 {
			t.Fatalf("%s(%d): 期望一条命令，实际 %+v", wf.Type, n, commands)
		}
		command := commands[0]
		switch command.Type {
		case historypkg.CmdCompleteWorkflow:
			return jsonInt(t, command.Output["n"])
		case historypkg.CmdScheduleActivity:
			if command.ActivityType != "calc.double" {
				t.Fatalf("%s(%d): 调度了 %s", wf.Type, n, command.ActivityType)
			}
			add(historypkg.Event{WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
			add(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{
				ActivityID: command.ActivityID, ActivityType: command.ActivityType, TaskQueue: command.TaskQueue, Input: command.Input,
			}))
			add(historypkg.Event{ActivityCompleted: &historypkg.ActivityCompletedEvent{
				ActivityID: command.ActivityID, ScheduledEventID: int64(len(events)), ActivityType: command.ActivityType,
				Output: map[string]any{"n": jsonInt(t, command.Input["n"]) * 2},
			}})
		default:
			t.Fatalf("%s(%d): 意外的命令 %+v", wf.Type, n, command)
		}
	}
	t.Fatalf("%s(%d): 50 个决策内没有完成", wf.Type, n)
	return 0
}

func jsonInt(t *testing.T, value any) int {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		t.Fatalf("%#v 不是整数: %v", value, err)
	}
	return n
}

// runInitClosureNative 把 calc 的桥换成直接调用 Activity 实现的原生版本，用 go run 执行同一份 Workflow 源码。
func runInitClosureNative(t *testing.T, moduleRoot, root, rootImport string, bridges []string) map[string]int {
	t.Helper()
	nativeBridge := `package calc

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func CallDouble(ctx workflow.Context, in Num, opts ...workflow.ActivityOptions) (Num, error) {
	return Double(context.Background(), in)
}
`
	for _, path := range bridges {
		if err := os.WriteFile(path, []byte(nativeBridge), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var imports, calls strings.Builder
	for pkg, inputs := range initClosureInputs {
		fmt.Fprintf(&imports, "\t%q\n", rootImport+"/"+pkg)
		for _, n := range inputs {
			fmt.Fprintf(&calls, "\trecord(%q, %d, %s.Flow)\n", pkg, n, pkg)
		}
	}
	mainSource := fmt.Sprintf(`package main

import (
	"encoding/json"
	"fmt"
	"os"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	%q
%s)

var results = map[string]int{}

func record(pkg string, n int, flow func(workflow.Context, calc.Num) (calc.Num, error)) {
	out, err := flow(nil, calc.Num{N: n})
	if err != nil {
		panic(err)
	}
	results[fmt.Sprintf("%%s/%%d", pkg, n)] = out.N
}

func main() {
%s	_ = json.NewEncoder(os.Stdout).Encode(results)
}
`, rootImport+"/calc", imports.String(), calls.String())
	mainDir := filepath.Join(root, "nativerun")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "main.go"), []byte(mainSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./"+filepath.ToSlash(mustRel(t, moduleRoot, mainDir)))
	command.Dir = moduleRoot
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("原生执行失败: %v\n%s", err, stderr)
	}
	results := map[string]int{}
	if err := json.Unmarshal(output, &results); err != nil {
		t.Fatalf("解析原生结果: %v\n%s", err, output)
	}
	return results
}
