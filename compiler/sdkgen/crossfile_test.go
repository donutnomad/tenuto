package sdkgen_test

// 同包跨文件 activity marker 的回归：@Workflow、@Activity 实现与 DTO 分别在
// workflow.go / activities.go / types.go 三个文件里时，生成器重写出来的 marker
// 必须仍然是包级函数符号。
//
// 历史缺口（本文件锁住）：重写把 `out, err := Call(...)` 一律写成
// `var out T; err := <ExecuteActivity>.Get(...)`。第二条同形语句里 err 已在作用域，
// `:=` 左侧没有新变量，整包不通过类型检查；翻译器那时会静默退回单文件解析，
// 看不到同包其它文件的包级函数，marker 被翻译成变量引用，生成报
// “翻译产物里找不到 activity ... 的 marker（实际符号 []）”。同文件示例看不出来，
// 因为 marker 声明就在被解析的那个文件里。

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// crossFileMarkers 是 fixture 三个 activity 在产物里应有的 marker 符号。
var crossFileMarkers = []string{"splitpkg.Charge", "splitpkg.Record", "splitpkg.Reverse"}

// TestSamePackageCrossFileMarkers 覆盖 fresh 首次生成、marker 形态、二次字节稳定、
// 生成包可编译，以及真实解释器的正向与补偿路径。
func TestSamePackageCrossFileMarkers(t *testing.T) {
	dir := copyFixture(t, "splitpkg")

	first := generateSplitFixture(t, dir)

	// 1) 每条 ExecuteActivity 的 marker 都是 symbol，且正好是三个跨文件函数。
	if got := crossFileMarkerSymbols(t, first); !equalStrings(got, crossFileMarkers) {
		t.Fatalf("marker 符号 = %v，期望 %v", got, crossFileMarkers)
	}
	if vars := crossFileMarkerVars(t, first); len(vars) != 0 {
		t.Fatalf("marker 不应被翻译成变量引用，实际 %v", vars)
	}

	// 2) ActivityRef 按注解写 type/queue/version，并带 wire 指纹。
	wantTypes := map[string]string{
		"splitpkg.Charge":  "split.charge",
		"splitpkg.Record":  "split.record",
		"splitpkg.Reverse": "split.reverse",
	}
	for symbol, wantType := range wantTypes {
		ref, ok := first.Workflow.Activities[symbol]
		if !ok {
			t.Fatalf("activities 缺少 %s: %+v", symbol, first.Workflow.Activities)
		}
		if ref.Type != wantType || ref.TaskQueue != "split" || ref.Version != "v1" {
			t.Fatalf("%s 的注解字段 = %+v，期望 type=%s queue=split version=v1", symbol, ref, wantType)
		}
		if !strings.HasPrefix(ref.Input, "wire:") || !strings.HasPrefix(ref.Output, "wire:") {
			t.Fatalf("%s 的 wire 指纹缺失: %+v", symbol, ref)
		}
	}

	// 3) 桥与生成 helper 可编译：真实 go build 一次生成出来的包。
	buildPackage(t, dir)

	// 4) 第二次生成字节稳定（全部可注册产物 + 生成文件）。
	second := generateSplitFixture(t, dir)
	firstBytes := splitArtifactBytes(t, first)
	secondBytes := splitArtifactBytes(t, second)
	if len(firstBytes) != len(secondBytes) {
		t.Fatalf("产物条目数变化: %d → %d", len(firstBytes), len(secondBytes))
	}
	for name, want := range firstBytes {
		if string(secondBytes[name]) != string(want) {
			t.Fatalf("%s 第二次生成字节不稳定：\n--- first\n%s\n--- second\n%s", name, want, secondBytes[name])
		}
	}

	// 5) 真实解释器：正向路径与补偿路径。
	nodes := first.Workflow
	assertCrossFileHappyPath(t, nodes)
	assertCrossFileCompensationPath(t, nodes)
}

// TestActivityMarkerShadowedByLocalDeclaration 锁住「同名局部函数值遮蔽 activity 实现名」
// 的处理：翻译产物里 marker 是变量引用（Go 原意），生成器必须指出遮蔽原因，
// 而不是只报“找不到 marker（实际符号 []）”。
func TestActivityMarkerShadowedByLocalDeclaration(t *testing.T) {
	dir := copyFixture(t, "shadowed")
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("写桥文件: %v", err)
		}
	}
	if _, err := sdkgen.Generate(plan); err == nil {
		t.Fatal("marker 被局部声明遮蔽时必须报错")
	} else {
		for _, want := range []string{"变量引用", "遮蔽", "Charge", "workflow.go:"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("错误 %q 缺少 %q", err.Error(), want)
			}
		}
	}
}

// generateSplitFixture 跑一次 Scan → 写桥 → Generate，返回产物。
func generateSplitFixture(t *testing.T, dir string) sdkgen.PackageResult {
	t.Helper()
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("写桥文件: %v", err)
		}
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return result
}

// splitArtifactBytes 序列化全部可注册产物与生成文件，用于字节稳定性比对。
func splitArtifactBytes(t *testing.T, result sdkgen.PackageResult) map[string][]byte {
	t.Helper()
	out := map[string][]byte{
		"workflow": mustMarshal(t, result.Workflow),
		"embedded": result.Embedded.Content,
	}
	for _, bridge := range result.Bridges {
		out["bridge:"+filepath.Base(bridge.Path)] = bridge.Content
	}
	return out
}

// buildPackage 用真实 go build 编译生成出来的包（含 CallX 桥与 artifacts helper）。
func buildPackage(t *testing.T, dir string) {
	t.Helper()
	moduleRoot, _ := moduleRootAndPath(t)
	rel, err := filepath.Rel(moduleRoot, dir)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "build", "./"+filepath.ToSlash(rel))
	command.Dir = moduleRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("生成包不可编译: %v\n%s", err, output)
	}
}

// crossFileMarkerSymbols 返回产物里 ExecuteActivity 的 marker 符号（去重排序）。
func crossFileMarkerSymbols(t *testing.T, result sdkgen.PackageResult) []string {
	t.Helper()
	symbols, _ := scanMarkerForms(t, result)
	out := make([]string, 0, len(symbols))
	for symbol := range symbols {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// crossFileMarkerVars 返回被翻译成变量引用的 marker（去重排序）。
func crossFileMarkerVars(t *testing.T, result sdkgen.PackageResult) []string {
	t.Helper()
	_, vars := scanMarkerForms(t, result)
	out := make([]string, 0, len(vars))
	for name := range vars {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// scanMarkerForms 递归扫描产物，按 ExecuteActivity 第 2 个业务参数的写法分类。
func scanMarkerForms(t *testing.T, result sdkgen.PackageResult) (map[string]bool, map[string]bool) {
	t.Helper()
	raw := mustMarshal(t, result.Workflow.Program)
	var envelope struct {
		Program []any `json:"program"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("解析产物: %v", err)
	}
	symbols := map[string]bool{}
	vars := map[string]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if call, ok := value["ExecuteActivity"].([]any); ok && len(call) >= 3 {
				if marker, ok := call[2].(map[string]any); ok {
					if symbol, ok := marker["symbol"].(string); ok {
						symbols[symbol] = true
					}
					if name, ok := marker["var"].(string); ok {
						vars[name] = true
					}
				}
			}
			for _, item := range value {
				walk(item)
			}
		case []any:
			for _, item := range value {
				walk(item)
			}
		}
	}
	walk(envelope.Program)
	return symbols, vars
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// assertCrossFileHappyPath 断言两个 activity 完成后 workflow 返回正确的 output。
func assertCrossFileHappyPath(t *testing.T, nodes *program.WorkflowConfig) {
	t.Helper()
	history := []historypkg.Event{startedSplitEvent(t, "order-1", 4200)}
	charge := executeSingleSchedule(t, nodes, history, "split.charge")
	history = appendSplitScheduled(history, charge)
	history = appendSplitCompleted(history, charge, map[string]any{"charge_id": "charge-order-1"})

	record := executeSingleSchedule(t, nodes, history, "split.record")
	history = appendSplitScheduled(history, record)
	history = appendSplitCompleted(history, record, map[string]any{"receipt": "receipt-charge-order-1"})

	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("重放 Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("期望 CmdCompleteWorkflow，实际 %+v", commands)
	}
	if commands[0].Output["charge_id"] != "charge-order-1" || commands[0].Output["receipt"] != "receipt-charge-order-1" {
		t.Fatalf("workflow 输出没有回写 activity 结果: %+v", commands[0].Output)
	}
}

// assertCrossFileCompensationPath 断言登记失败后先调度撤销扣款补偿，再以
// 应用错误文案进入失败终态。
func assertCrossFileCompensationPath(t *testing.T, nodes *program.WorkflowConfig) {
	t.Helper()
	history := []historypkg.Event{startedSplitEvent(t, "order-2", 100)}
	charge := executeSingleSchedule(t, nodes, history, "split.charge")
	history = appendSplitScheduled(history, charge)
	history = appendSplitCompleted(history, charge, map[string]any{"charge_id": "charge-order-2"})

	record := executeSingleSchedule(t, nodes, history, "split.record")
	history = appendSplitScheduled(history, record)
	history = append(history, historypkg.Event{
		ID:        nextSplitEventID(history),
		Timestamp: time.Unix(0, int64(len(history))+10),
		ActivityFailed: &historypkg.ActivityFailedEvent{
			ActivityID: record.ActivityID, ScheduledEventID: recordScheduledEventID(history), ActivityType: record.ActivityType,
			Error:   "record store unavailable",
			Failure: &protocol.Failure{Message: "record store unavailable", Type: "RecordStoreUnavailable"},
		},
	})

	reverse := executeSingleSchedule(t, nodes, history, "split.reverse")
	history = appendSplitScheduled(history, reverse)
	history = appendSplitCompleted(history, reverse, map[string]any{"reversed": true})

	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("补偿重放 Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdFailWorkflow {
		t.Fatalf("期望 CmdFailWorkflow，实际 %+v", commands)
	}
	if commands[0].Error != "record failed, charge reversed" {
		t.Fatalf("失败文案 = %q，期望 record failed, charge reversed", commands[0].Error)
	}
}

// startedSplitEvent 构造 WorkflowStarted。
func startedSplitEvent(t *testing.T, orderID string, amount int64) historypkg.Event {
	t.Helper()
	event := historypkg.NewWorkflowStartedEvent(
		map[string]any{"order_id": orderID, "amount": amount}, "", "", "splitpkg-flow", "Split Package Flow", 0)
	event.ID = 1
	return event
}

// executeSingleSchedule 执行一次决策，要求恰好调度一条指定 activity。
func executeSingleSchedule(t *testing.T, nodes *program.WorkflowConfig, history []historypkg.Event, wantType string) historypkg.Command {
	t.Helper()
	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleActivity {
		t.Fatalf("期望一条 ScheduleActivity(%s)，实际 %+v", wantType, commands)
	}
	if commands[0].ActivityType != wantType || commands[0].TaskQueue != "split" {
		t.Fatalf("调度 = %s/%s，期望 %s/split", commands[0].ActivityType, commands[0].TaskQueue, wantType)
	}
	return commands[0]
}

// appendSplitScheduled 追加 WorkflowTaskCompleted + ActivityScheduled 事件对。
func appendSplitScheduled(history []historypkg.Event, scheduled historypkg.Command) []historypkg.Event {
	id := nextSplitEventID(history)
	history = append(history, historypkg.Event{
		ID: id, Timestamp: time.Unix(0, int64(id)),
		WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{},
	})
	history = append(history, historypkg.Event{
		ID: id + 1, Timestamp: time.Unix(0, int64(id+1)),
		ActivityScheduled: &historypkg.ActivityScheduledEvent{
			ActivityID: scheduled.ActivityID, ActivityType: scheduled.ActivityType,
			TaskQueue: scheduled.TaskQueue, Input: scheduled.Input,
		},
	})
	return history
}

// appendSplitCompleted 追加 ActivityCompleted 事件。
func appendSplitCompleted(history []historypkg.Event, scheduled historypkg.Command, output map[string]any) []historypkg.Event {
	return append(history, historypkg.Event{
		ID:        nextSplitEventID(history),
		Timestamp: time.Unix(0, int64(len(history))+20),
		ActivityCompleted: &historypkg.ActivityCompletedEvent{
			ActivityID: scheduled.ActivityID, ScheduledEventID: recordScheduledEventID(history),
			ActivityType: scheduled.ActivityType, Output: output,
		},
	})
}

// recordScheduledEventID 返回最近一条 ActivityScheduled 的事件 ID。
func recordScheduledEventID(history []historypkg.Event) int64 {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].ActivityScheduled != nil {
			return history[i].ID
		}
	}
	return 0
}

// nextSplitEventID 返回下一个事件 ID。
func nextSplitEventID(history []historypkg.Event) int64 {
	return int64(len(history) + 1)
}
