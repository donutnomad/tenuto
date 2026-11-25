package sdkgen

import (
	"strings"
	"testing"
)

// ============================================
// AddFuture 内联形态放宽 + AddSignalFuture/AddChildFuture
// ============================================
// 测试策略:
// - T-S4 Happy Path: AddFuture 第 2 参内联 workflow.ExecuteActivity(...) → 放行 + 符号收集
// - T-S5 Error Case: AddFuture 第 2 参字段/未登记 ident → 仍报错
// - T-S6 Happy Path: AddSignalFuture/AddChildFuture 的变量与内联形态放行; Error Case: 种类不匹配报错
// ============================================

const selExtHeader = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

`

func generateSelExt(t *testing.T, body string) (Result, error) {
	t.Helper()
	src := selExtHeader + "func W(ctx workflow.Context, input map[string]any) error {\n" + body + "\n\treturn nil\n}\n"
	return GenerateFromSource([]byte(src), Options{WorkflowType: "sel-ext", FuncName: "W", Filename: "w.go"})
}

// T-S4 Happy Path: AddFuture 第 2 参内联 ExecuteActivity → 放行、符号进表。
func TestSelectorAddFuture_InlineExecuteActivity_Allowed(t *testing.T) {
	result, err := generateSelExt(t, `
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, workflow.ExecuteActivity(ctx, activities.Inline, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}), func(f workflow.ActivityFuture) {
		var out map[string]any
		_ = f.Get(ctx, &out)
	})
	sel.Select(ctx)`)
	if err != nil {
		t.Fatalf("内联 ExecuteActivity 应放行, got err: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.Inline"]; !ok {
		t.Fatalf("内联形态符号应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// T-S6 Happy Path: AddSignalFuture/AddChildFuture 的变量形态与内联形态放行。
func TestSelectorAddTypedFutures_VarAndInline_Allowed(t *testing.T) {
	result, err := generateSelExt(t, `
	sf := workflow.WaitSignal(ctx, "approve")
	cf := workflow.ExecuteChildWorkflow(ctx, "child-wf", input)
	sel := workflow.NewSelector(ctx)
	sel.AddSignalFuture(ctx, sf, func(f workflow.SignalFuture) {
		var out map[string]any
		_ = f.Get(ctx, &out)
	})
	sel.AddChildFuture(ctx, cf, func(f workflow.ChildWorkflowFuture) {
		var out map[string]any
		_ = f.Get(ctx, &out)
	})
	sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "reprice"), func(f workflow.SignalFuture) {
		var out map[string]any
		_ = f.Get(ctx, &out)
	})
	sel.Select(ctx)`)
	if err != nil {
		t.Fatalf("变量/内联形态应放行, got err: %v", err)
	}
	if !strings.Contains(string(result.ProgramJSON), "AddSignalFuture") {
		t.Fatalf("执行态应透传 AddSignalFuture host 调用")
	}
}
