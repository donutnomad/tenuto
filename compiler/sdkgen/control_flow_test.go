package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRangeBodyActivityIsCollected(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type OrderInput struct {
	Items []string ` + "`json:\"items\"`" + `
}

func OrderWorkflow(ctx workflow.Context, in OrderInput) error {
	for _, item := range in.Items {
		var out map[string]any
		err := workflow.ExecuteActivity(ctx, activities.Notify, item, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		if err != nil {
			return err
		}
	}
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowID: "range-flow", FuncName: "OrderWorkflow", Filename: "range_workflow.go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Workflow.Activities["activities.Notify"]; !ok || len(result.Workflow.Activities) != 1 {
		t.Fatalf("expected one activity symbol, got %+v", result.Workflow.Activities)
	}
}

// Happy Path: 非结构化并发 wg.Go(ctx, f)（收窄后签名 func(ctx)，起了即返回不 join，
// join 由 WaitGroup 显式做），go2wlang 透传成 host 调用 {"Go":[wg, ctx, {"fn":...}]}——不再报错。
// 每条 lane 内联闭包会翻成 {"fn":...} 闭包实参，送入 host 方法 waitGroup.Go。
func TestGenerateFromSource_WorkflowGo_TranspilesToHostCall(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func OrderWorkflow(ctx workflow.Context) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var out map[string]any
		err := workflow.ExecuteActivity(ctx, activities.A, "a", workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		_ = err
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var out map[string]any
		err := workflow.ExecuteActivity(ctx, activities.B, "b", workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		_ = err
	})
	wg.Wait(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowID: "go-flow", FuncName: "OrderWorkflow", Filename: "go_workflow.go"})
	if err != nil {
		t.Fatalf("expected wg.Go to transpile, got error: %v", err)
	}
	// 断言生成的 WLang 程序含 {"Go":[...]} host 调用（命名 Go 直达 P1，无需符号映射）。
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"Go":[`) {
		t.Fatalf("expected {\"Go\":[...]} host call in program, got: %s", raw)
	}
	// 两条 lane 各是一个闭包字面量（{"fn":...}），透传为 wg.Go 的实参。
	if strings.Count(string(raw), `"fn":`) < 2 {
		t.Fatalf("expected two lane closures in program, got: %s", raw)
	}
}

// Error Case（回归）：wg.Go lane 内引用的 activity 符号必须进 Program.Activities 表。
// 否则运行时 wlang 解释器求值 activities.X 会报 E_SYMBOL not found。历史 bug：
// scanActivities 遇 lane Go 直接 return nil，跳过 lane 闭包，lane 内符号漏收集。
func TestGenerateFromSource_WorkflowGo_LaneActivitiesRegistered(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func OrderWorkflow(ctx workflow.Context) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var out map[string]any
		e := workflow.ExecuteActivity(ctx, activities.CheckBalance, "a", workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		_ = e
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var reserve map[string]any
		e1 := workflow.ExecuteActivity(ctx, activities.Reserve, "b", workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		_ = e1
		var sign map[string]any
		e2 := workflow.ExecuteActivity(ctx, activities.Sign, "c", workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign)
		_ = e2
	})
	wg.Wait(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowID: "go-flow-acts", FuncName: "OrderWorkflow", Filename: "go_acts.go"})
	if err != nil {
		t.Fatalf("expected wg.Go to transpile, got error: %v", err)
	}
	for _, sym := range []string{"activities.CheckBalance", "activities.Reserve", "activities.Sign"} {
		if _, ok := result.Workflow.Activities[sym]; !ok {
			t.Fatalf("lane 内符号 %q 应进 Program.Activities 表（否则运行时 E_SYMBOL），实际表: %+v", sym, result.Workflow.Activities)
		}
	}
}
