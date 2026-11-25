package interpreter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 端到端：真实 Go 源码 → sdkgen → engine 执行。
//
// **必须走真实翻译产物**。构造一个裸 *types.Record 去喂 executeWLang 的 switch
// 会得到假绿——真实产物里 (T, error) 是 tuple，裸 Record 根本不是它的形态
// （见 wlang_typed_record_test.go 的 TestUnpackTupleIsRequiredForBodyReturn）。
func generateAndRun(t *testing.T, src, funcName string, input map[string]any) (executor.DebugReplayResult, map[string]any, error) {
	t.Helper()

	gen, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{
		WorkflowType: "typed-e2e-wf",
		FuncName:     funcName,
		Filename:     "workflow.go",
	})
	if err != nil {
		t.Fatalf("GenerateFromSource: %v", err)
	}
	artifact := gen.Workflow.Program

	programJSON, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}

	events := []history.Event{history.NewWorkflowStartedEvent(input, "", "", "typed-e2e-wf", "", 0)}

	// 借 debug replay 入口：它走的是同一个 executeWLang，是最短的端到端路径。
	result, runErr := ReplayWLangForDebug(context.Background(), programJSON, events, nil)

	// 取执行器实际算出的 workflow 输出。
	activities := &program.WorkflowConfig{
		Type:       "e2e",
		Program:    artifact,
		Activities: map[string]program.ActivityRef{},
	}
	exec := NewWorkflowExecutorWithContext(context.Background(), activities, events)
	_, _ = exec.Execute()
	return result, exec.workflowReturnOutput, runErr
}

const typedWorkflowSrc = `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type OrderInput struct {
	Amount int64 ` + "`json:\"amount\"`" + `
	Label  string ` + "`json:\"label\"`" + `
}

type PaymentOutput struct {
	Total int64 ` + "`json:\"total\"`" + `
	OK    bool  ` + "`json:\"ok\"`" + `
}

func OrderWorkflow(ctx workflow.Context, in OrderInput) (PaymentOutput, error) {
	return PaymentOutput{Total: in.Amount, OK: true}, nil
}`

// TestTypedEntryInputReachesBody 断言入口参数按 Entry.InputVar 绑定：body 用作者写的参数名 in
// 读到字段，而不是报 [E_SYMBOL] variable path "in.Amount" not found。
func TestTypedEntryInputReachesBody(t *testing.T) {
	_, output, err := generateAndRun(t, typedWorkflowSrc, "OrderWorkflow", map[string]any{
		"amount": 4200,
		"label":  "x",
	})
	if err != nil {
		if strings.Contains(err.Error(), "E_SYMBOL") {
			t.Fatalf("typed 入参未注入，body 找不到形参符号: %v", err)
		}
		t.Fatalf("ReplayWLangForDebug: %v", err)
	}
	if len(output) == 0 {
		t.Fatal("workflow 输出为空 —— typed 出参被静默丢弃")
	}
	// JSON tag 生效：键是 total/ok。
	total, ok := output["total"]
	if !ok {
		t.Fatalf("输出缺少 total 字段: %#v", output)
	}
	// in.Amount 真的被读到了（4200 来自入参，不是常量）。
	if num, isNum := total.(json.Number); !isNum || num.String() != "4200" {
		t.Fatalf("total 应等于入参 amount=4200，实际 %#v", total)
	}
	if output["ok"] != true {
		t.Fatalf("ok 字段丢失: %#v", output)
	}
}

// TestNonTypedWorkflowStillRuns 守住回落路径：非 typed workflow 的 Entry 是
// 零值，不能因为改造而跑不起来。
func TestNonTypedWorkflowStillRuns(t *testing.T) {
	src := `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

func BareWorkflow(ctx workflow.Context) error { return nil }`

	if _, _, err := generateAndRun(t, src, "BareWorkflow", map[string]any{"a": 1}); err != nil {
		t.Fatalf("非 typed workflow 应正常执行: %v", err)
	}
}

// TestTypedEntryDecodeFailureSurfaces：入参与 descriptor 不匹配时必须报出
// 可识别的错误，而不是静默塞个空值继续跑。
func TestTypedEntryDecodeFailureSurfaces(t *testing.T) {
	_, _, err := generateAndRun(t, typedWorkflowSrc, "OrderWorkflow", map[string]any{
		"amount": "not-a-number",
	})
	if err == nil {
		t.Fatal("入参类型不匹配应报错")
	}
	if !strings.Contains(err.Error(), "decode workflow entry input") {
		t.Fatalf("错误应指明是 entry input 解码失败，实际: %v", err)
	}
}

// TestEntryInputBindsUnderAnyParameterName：入口形参不叫 input 时 body 也能读到入参。
// 此前 map[string]any 入参只绑定为 input，形参叫 req 时报 [E_SYMBOL] variable path "req" not found。
func TestEntryInputBindsUnderAnyParameterName(t *testing.T) {
	for _, tc := range []struct{ name, src string }{
		{"map[string]any", `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, req map[string]any) (map[string]any, error) {
	return map[string]any{"label": req["label"]}, nil
}`},
		{"struct 指针", `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type OrderInput struct {
	Label string ` + "`json:\"label\"`" + `
}

func W(ctx workflow.Context, order *OrderInput) (map[string]any, error) {
	return map[string]any{"label": order.Label}, nil
}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, output, err := generateAndRun(t, tc.src, "W", map[string]any{"label": "x"})
			if err != nil {
				t.Fatalf("ReplayWLangForDebug: %v", err)
			}
			if output["label"] != "x" {
				t.Fatalf("输出应回显入参 label=x，实际 %#v", output)
			}
		})
	}
}
