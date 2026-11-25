package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

// ============================================
// workflow.Selector sdkgen 识别 + go2wlang 透传测试
// ============================================
// 照 Temporal selectorImpl 移植（internal_workflow.go:140-145, 1416-1556）。
// 测试策略：
// - Happy Path : sel := NewSelector + sel.AddReceive/AddSend/AddDefault/Select 透传成 host 调用
//                （{"AddReceive":[sel,...]} 等，接收者=selector 变量 → 分派 selectorHandle 方法）
// - 回归       : selector 操作展示态放行——不 lower 成展示 Step（类比 channel op），且不报 unsupported
// - 回调翻译   : receive 回调 func(ch Channel, more bool) 翻成 fn（params 含 workflow.Channel + boolean）
// ============================================

// Happy Path: sel.AddReceive/AddSend/AddDefault/Select 经 go2wlang 透传成 host 调用；
// selector 变量被识别、操作不进 Activities 表、不报 unsupported；回调翻成内联 fn。
func TestGenerateFromSource_Selector_TranspilesToHostCall(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func SelectorWorkflow(ctx workflow.Context) error {
	ch1 := workflow.NewChannel(ctx)
	ch2 := workflow.NewChannel(ctx)
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, ch1, func(c workflow.Channel, more bool) {
		var out map[string]any
		_ = c.Receive(ctx, &out)
	})
	sel.AddSend(ctx, ch2, "payload", func() {
	})
	sel.AddDefault(ctx, func() {
	})
	sel.Select(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "sel-flow", FuncName: "SelectorWorkflow", Filename: "sel_workflow.go"})
	if err != nil {
		t.Fatalf("expected selector ops to transpile, got error: %v", err)
	}
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"NewSelector":[`, `"AddReceive":[`, `"AddSend":[`, `"AddDefault":[`, `"Select":[`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("expected %s host call in program, got: %s", want, raw)
		}
	}
	// 回调翻译：receive 回调 func(ch Channel, more bool) → fn，params 含 workflow.Channel 型名（host Select
	// 执行回调时按此型名传 channelHandle，valueAssignableTo 精确匹配依赖此）。
	if !strings.Contains(string(raw), `"workflow.Channel"`) {
		t.Fatalf("expected receive callback param typed workflow.Channel, got: %s", raw)
	}
	// 回归：selector 操作不是 Activity，不进 Activities 表。
	for symbol := range result.Workflow.Activities {
		switch symbol {
		case "AddReceive", "AddSend", "AddDefault", "Select", "NewSelector":
			t.Fatalf("selector 操作不应进 Activities 表，got %q", symbol)
		}
	}
}

// 回归: 链式赋值形态 sel = sel.AddReceive(...) 也被识别（覆盖 default 分支的 isSelectorOpCall + 链式 record）。
func TestGenerateFromSource_Selector_Chained_Recognized(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func ChainedSelectorWorkflow(ctx workflow.Context) error {
	ch := workflow.NewChannel(ctx)
	sel := workflow.NewSelector(ctx)
	sel = sel.AddReceive(ctx, ch, func(c workflow.Channel, more bool) {
	})
	sel.Select(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "chained-sel", FuncName: "ChainedSelectorWorkflow", Filename: "chained_sel.go"})
	if err != nil {
		t.Fatalf("expected chained selector ops to transpile, got error: %v", err)
	}
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"NewSelector":[`, `"AddReceive":[`, `"Select":[`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("expected %s host call in program, got: %s", want, raw)
		}
	}
}

// Happy Path: sel.AddFuture(ctx, f, cb) 放行翻译、
// future 变量标 used（无「must be awaited with Get」误报）、activity 符号收进 Activities 表。
func TestGenerate_SelectorAddFuture(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	f := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
		var out map[string]any
		_ = fut.Get(ctx, &out)
	})
	sel.Select(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "sel-fut", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("AddFuture 应放行翻译（future 经 AddFuture 即视为 used），got error: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.A"]; !ok {
		t.Fatalf("AddFuture 的 future 符号应收进 Activities 表, got %v", result.Workflow.Activities)
	}
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), "AddFuture") {
		t.Fatalf("AddFuture 应透传成 host 调用, program=%s", string(raw))
	}
}

// Happy Path: 第 2 参内联 workflow.ExecuteActivity(...)
// → 放行翻译 + 符号进 Activities 表（内联无变量 → 无 used 检查问题；符号由放行点
// collectGoLaneActivities 全树收集）。v2 修订：旧的「内联报错拒绝」边界已被本方案取代。
func TestGenerate_SelectorAddFuture_InlineExecuteActivity_Allowed(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}), func(fut workflow.ActivityFuture) {
	})
	sel.Select(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "sel-fut-inline", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("内联 ExecuteActivity 作 AddFuture 第 2 参应放行翻译, got err: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.A"]; !ok {
		t.Fatalf("内联 ExecuteActivity 的符号应进 Activities 表, got %v", result.Workflow.Activities)
	}
}

// AddFuture 的第 2 参可以是任意 ActivityFuture 表达式（切片元素、变量）；种类由 Go 类型检查保证。
func TestGenerate_SelectorAddFuture_SliceElement_Allowed(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	var futures []workflow.ActivityFuture
	futures = append(futures, workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}))
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, futures[0], func(fut workflow.ActivityFuture) {
	})
	sel.Select(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "sel-fut-slice", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Workflow.Activities["activities.A"]; !ok {
		t.Fatalf("activities = %v", result.Workflow.Activities)
	}
}
