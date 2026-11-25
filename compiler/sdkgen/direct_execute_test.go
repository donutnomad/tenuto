package sdkgen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	programpkg "github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

func generateFixtureDir(t *testing.T, dir string) sdkgen.PackageResult {
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

func refByType(t *testing.T, result sdkgen.PackageResult, activityType string) programpkg.ActivityRef {
	t.Helper()
	for _, ref := range result.Workflow.Activities {
		if ref.Type == activityType {
			return ref
		}
	}
	t.Fatalf("activities 里没有 type %q: %+v", activityType, result.Workflow.Activities)
	return programpkg.ActivityRef{}
}

// 第 4 项：workflow.ExecuteActivity(ctx, pkg.Func, ...) 的被调函数带 @Activity 注解时，
// activity ref 的 type/queue/version/I/O 指纹与生成的 CallX 桥完全一致；没有注解的函数
// 以符号写法作 type、不声明队列（由 Server 按 Worker 登记选路）、不带契约。
func TestDirectExecuteActivityUsesAnnotation(t *testing.T) {
	direct := generateFixtureDir(t, copyFixture(t, "executeactivity"))

	bridgeDir := copyFixture(t, "executeactivity")
	workflowFile := filepath.Join(bridgeDir, "workflow.go")
	source, err := os.ReadFile(workflowFile)
	if err != nil {
		t.Fatal(err)
	}
	bridged := string(source)
	for old, replacement := range map[string]string{
		"var charged activities.ChargeOutput\n\tif err := workflow.ExecuteActivity(ctx, activities.Charge, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &charged); err != nil {":           "charged, err := activities.CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})\n\tif err != nil {",
		"var notified NotifyOutput\n\tif err := workflow.ExecuteActivity(ctx, Notify, NotifyInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notified); err != nil {": "_, err = CallNotify(ctx, NotifyInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})\n\tif err != nil {",
	} {
		if !strings.Contains(bridged, old) {
			t.Fatalf("fixture 里找不到待替换的片段 %q", old)
		}
		bridged = strings.Replace(bridged, old, replacement, 1)
	}
	if err := os.WriteFile(workflowFile, []byte(bridged), 0o644); err != nil {
		t.Fatal(err)
	}
	viaBridge := generateFixtureDir(t, bridgeDir)

	for _, activityType := range []string{"payments.charge", "orders.notify"} {
		got, want := refByType(t, direct, activityType), refByType(t, viaBridge, activityType)
		if got.TaskQueue != want.TaskQueue || got.Version != want.Version || got.Input != want.Input || got.Output != want.Output {
			t.Fatalf("%s: 直接调用的 ref %+v 与桥的 ref %+v 不一致", activityType, got, want)
		}
		if got.Version == "" || !strings.HasPrefix(got.Input, "wire:") || !strings.HasPrefix(got.Output, "wire:") {
			t.Fatalf("%s: ref 缺少注解的版本或 I/O 指纹: %+v", activityType, got)
		}
	}
	if ref := refByType(t, direct, "payments.charge"); ref.TaskQueue != "payments" || ref.Version != "v1" {
		t.Fatalf("payments.charge ref = %+v", ref)
	}
	if ref := refByType(t, direct, "orders.notify"); ref.TaskQueue != "notifications" || ref.Version != "v2" {
		t.Fatalf("orders.notify ref = %+v", ref)
	}
	plain := refByType(t, direct, "activities.Audit")
	if plain.TaskQueue != "" || plain.Version != "" || plain.Input != "" || plain.Output != "" {
		t.Fatalf("没有注解的函数不应带契约: %+v", plain)
	}

	// 干跑：直接调用调度到注解声明的 type 与 queue。
	nodes := direct.Workflow
	started := historypkg.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1", "amount": 7}, "", "", "executeactivity-flow", "", 0)
	started.ID = 1
	commands, err := interpreter.NewWorkflowExecutor(nodes, []historypkg.Event{started}).Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(commands) != 1 || commands[0].ActivityType != "payments.charge" || commands[0].TaskQueue != "payments" {
		t.Fatalf("期望调度 payments.charge@payments，实际 %+v", commands)
	}
	if commands[0].Input["order_id"] != "o-1" {
		t.Fatalf("调度输入 = %+v", commands[0].Input)
	}
}

// 没有注解的函数也可以收 struct 字面量输入：record 绑定到 ExecuteActivity 的 any 形参，按类型表编码成 JSON。
func TestDirectExecuteActivityWithoutAnnotationAcceptsStructLiteral(t *testing.T) {
	dir := copyFixture(t, "executeactivity")
	workflowFile := filepath.Join(dir, "workflow.go")
	source, err := os.ReadFile(workflowFile)
	if err != nil {
		t.Fatal(err)
	}
	const old = `map[string]any{"order_id": in.OrderID}`
	if !strings.Contains(string(source), old) {
		t.Fatalf("fixture 里找不到 %q", old)
	}
	changed := strings.Replace(string(source), old, "activities.AuditInput{OrderID: in.OrderID}", 1)
	if err := os.WriteFile(workflowFile, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sdkgen.Generate(plan); err != nil {
		t.Fatalf("Generate: %v", err)
	}
}
