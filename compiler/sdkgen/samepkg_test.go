package sdkgen_test

// 同包 remote 契约的回归：workflow 与 contract 在同一个包里，编排直接调用生成的桥。
//
// 这里刻意走外部测试包（sdkgen_test）：interpreter 依赖 sdkgen，同包测试会形成导入环。
// 用例不只比对 JSON，而是真的用解释器干跑一次，断言调度到 Annotation 声明的 activity type
// 与 queue，并且完成后能把 activity 输出写回 workflow 返回值。

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// copySamePackageFixture 把 testdata/samepkg 复制到 testdata 下的临时目录。
func copyFixture(t *testing.T, fixture string) string {
	t.Helper()
	source, err := filepath.Abs(filepath.Join("testdata", fixture))
	if err != nil {
		t.Fatal(err)
	}
	target, err := os.MkdirTemp(filepath.Join("testdata"), "tmp-"+fixture+"-")
	if err != nil {
		t.Fatal(err)
	}
	if target, err = filepath.Abs(target); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(target) })
	// 递归复制并改写包内 import 前缀：否则副本里的 workflow 仍 import 原 fixture 的
	// contract 包，生成桥会写回仓库 testdata（测试污染工作区）。
	moduleRoot, modulePath := moduleRootAndPath(t)
	sourceImport := modulePath + "/" + filepath.ToSlash(mustRel(t, moduleRoot, source))
	targetImport := modulePath + "/" + filepath.ToSlash(mustRel(t, moduleRoot, target))
	if err := copyFixtureTree(t, source, target, sourceImport, targetImport); err != nil {
		t.Fatal(err)
	}
	return target
}

func mustRel(t *testing.T, base, target string) string {
	t.Helper()
	rel, err := filepath.Rel(base, target)
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

func moduleRootAndPath(t *testing.T) (string, string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(dir, "go.mod"))
		if readErr == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
					return dir, strings.TrimSpace(rest)
				}
			}
			t.Fatalf("%s/go.mod has no module line", dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func copyFixtureTree(t *testing.T, source, target, sourceImport, targetImport string) error {
	t.Helper()
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		from := filepath.Join(source, entry.Name())
		to := filepath.Join(target, entry.Name())
		if entry.IsDir() {
			if err := os.MkdirAll(to, 0o755); err != nil {
				return err
			}
			if err := copyFixtureTree(t, from, to, sourceImport, targetImport); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasPrefix(entry.Name(), "zz_") {
			continue
		}
		data, err := os.ReadFile(from)
		if err != nil {
			return err
		}
		rewritten := strings.ReplaceAll(string(data), sourceImport, targetImport)
		if err := os.WriteFile(to, []byte(rewritten), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// TestSamePackageRemoteBridgeIsRewrittenAndScheduled 覆盖同包 remote 的完整链路：
// `return CallCharge(ctx, in)` → 生成器重写 → ExecuteActivity(flows.ChargeRemoteActivity)+Get
// → 解释器干跑实际调度 payments.charge.v1 / payments 队列 → 完成后回写输出。
func TestSamePackageRemoteBridgeIsRewrittenAndScheduled(t *testing.T) {
	dir := copyFixture(t, "samepkg")
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

	// 1) 产物里 marker 是同包未限定符号，且 ActivityRef 按注解写。
	var ref *string
	for symbol, activity := range result.Workflow.Activities {
		if activity.Type != "payments.charge.v1" {
			continue
		}
		chosen := symbol
		ref = &chosen
		if symbol != "flows.ChargeRemoteActivity" {
			t.Fatalf("同包 remote marker 符号 = %q，期望 flows.ChargeRemoteActivity", symbol)
		}
		if activity.TaskQueue != "payments" {
			t.Fatalf("queue = %q，期望 payments", activity.TaskQueue)
		}
		if !strings.HasPrefix(activity.Input, "wire:") || !strings.HasPrefix(activity.Output, "wire:") {
			t.Fatalf("input/output 应为 wire 指纹: %+v", activity)
		}
	}
	if ref == nil {
		t.Fatalf("activities 里没有 payments.charge.v1: %+v", result.Workflow.Activities)
	}

	program := string(mustMarshal(t, result.Workflow.Program))
	for _, want := range []string{`"symbol":"flows.ChargeRemoteActivity"`, `"ExecuteActivity"`, `"Get"`} {
		if !strings.Contains(program, want) {
			t.Fatalf("WLang 缺少 %s\n%s", want, program)
		}
	}
	if strings.Contains(program, `"CallCharge"`) {
		t.Fatalf("WLang 里仍有未重写的 CallCharge\n%s", program)
	}

	// 2) 干跑：真实调度命令指向注解声明的 activity type 与 queue。
	nodes := result.Workflow
	history := []historypkg.Event{
		historypkg.NewWorkflowStartedEvent(map[string]any{"order_id": "order-1", "amount": 4200}, "", "", "samepkg-flow", "Same Package Flow", 0),
	}
	history[0].ID = 1
	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("干跑 Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdScheduleActivity {
		t.Fatalf("期望一条 ScheduleActivity，实际 %+v", commands)
	}
	scheduled := commands[0]
	if scheduled.ActivityType != "payments.charge.v1" {
		t.Fatalf("调度 activity type = %q，期望 payments.charge.v1", scheduled.ActivityType)
	}
	if scheduled.TaskQueue != "payments" {
		t.Fatalf("调度 task queue = %q，期望 payments", scheduled.TaskQueue)
	}
	if scheduled.Input["order_id"] != "order-1" || fmt.Sprint(scheduled.Input["amount"]) != "4200" {
		t.Fatalf("调度输入没有带上结构体变量的字段: %+v", scheduled.Input)
	}

	// 3) 灌入 activity 完成事实 → workflow 完成并回写 activity 输出。
	history = append(history,
		historypkg.Event{ID: 2, Timestamp: time.Unix(0, 2), WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}},
		historypkg.Event{ID: 3, Timestamp: time.Unix(0, 3), ActivityScheduled: &historypkg.ActivityScheduledEvent{
			ActivityID: scheduled.ActivityID, ActivityType: scheduled.ActivityType, TaskQueue: scheduled.TaskQueue, Input: scheduled.Input,
		}},
		historypkg.Event{ID: 4, Timestamp: time.Unix(0, 4), ActivityCompleted: &historypkg.ActivityCompletedEvent{
			ActivityID: scheduled.ActivityID, ScheduledEventID: 3, ActivityType: scheduled.ActivityType,
			Output: map[string]any{"charge_id": "charge-order-1", "policy": "v1"},
		}},
	)
	commands, err = interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("重放 Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("期望 CmdCompleteWorkflow，实际 %+v", commands)
	}
	if commands[0].Output["charge_id"] != "charge-order-1" || commands[0].Output["policy"] != "v1" {
		t.Fatalf("workflow 输出没有回写 activity 结果: %+v", commands[0].Output)
	}
}

// mustMarshal 序列化产物用于断言。
func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestDirectActivityCallFailsWithBridgeHint 固定“直调 @Activity 实现”的失败语义：
// 不生成的桥调用必须给 file:line 明确诊断，并指向生成的桥名；
// 不能把它当普通函数内联进 WLang 而静默丢失 activity 派发。
func TestDirectActivityCallFailsWithBridgeHint(t *testing.T) {
	dir := copyFixture(t, "directcall")
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
		t.Fatal("direct activity call must fail generation")
	} else {
		for _, want := range []string{"直接调用了 @Activity 实现", "payments.charge", "CallCharge"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err.Error(), want)
			}
		}
	}
}

// [高熵密钥] 同包直调 @Activity 实现同样必须报显式错误（不给更多调用语法开后门）。
func TestSamePackageDirectActivityCallFails(t *testing.T) {
	dir := copyFixture(t, "directcall_samepkg")
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
		t.Fatal("same-package direct activity call must fail generation")
	} else {
		for _, want := range []string{"直接调用了 @Activity 实现", "payments.charge", "CallCharge"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err.Error(), want)
			}
		}
	}
}

// [高熵密钥] 产物不能依赖 checkout 路径：登记按内容 hash 幂等，WorkflowConfig 里不能出现源码目录的绝对路径。
func TestArtifactIgnoresCheckoutPath(t *testing.T) {
	dir := copyFixture(t, "orders")
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
	if raw := string(mustMarshal(t, result.Workflow)); strings.Contains(raw, dir) {
		t.Fatalf("workflow.json leaks checkout path %s", dir)
	}
}
