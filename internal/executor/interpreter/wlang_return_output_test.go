package interpreter

import (
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// workflow body return 数据上浮测试
// ============================================
// 测试策略:
// - Happy Path: body `return map[...]` → CmdCompleteWorkflow.Output 携带该 map
// - Edge Case: body 返回 error 签名 + `return nil` → Output 保持 nil（不误报数据）
//
// 背景: executeWLang 曾丢弃 RunProgram 返回值（error 部分已修，见
// wlang_lane_error_return_test.go），数据部分此前同样被丢弃——
// CmdCompleteWorkflow 不带 Output，server 的 NewWorkflowCompletedEvent(cmd.Output)
// 只能落 nil。对标 Temporal: WorkflowExecutionCompleted 事件携带 body 的 result。
// 注: go2wlang 限制 return 只支持零或一个值，数据返回签名为 `func W(...) map[string]any`。
// ============================================

func probeExecReturn(t *testing.T, src string, events []history.Event) ([]history.Command, error) {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "probe-ret-out", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	nodes := result.Workflow
	return NewWorkflowExecutor(nodes, events).Execute()
}

func findCompleteCmd(cmds []history.Command) (history.Command, bool) {
	for _, c := range cmds {
		if c.Type == history.CmdCompleteWorkflow {
			return c, true
		}
	}
	return history.Command{}, false
}

// Happy Path: body `return map[string]any{...}` → CmdCompleteWorkflow.Output 携带返回的数据。
func TestReturnOutput_MapData_FlowsToCompleteWorkflow(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) map[string]any {
	return map[string]any{"total": 42, "echo": input["k"]}
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecReturn(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应产 CmdCompleteWorkflow, got %+v", cmds)
	}
	if cmd.Output == nil {
		t.Fatalf("body return 的 map 应上浮到 CmdCompleteWorkflow.Output, got nil (数据被丢弃)")
	}
	if got, want := cmd.Output["total"], int64(42); got != want {
		t.Fatalf("Output[total] = %v (%T), want %v", got, got, want)
	}
	if got, want := cmd.Output["echo"], "v"; got != want {
		t.Fatalf("Output[echo] = %v (%T), want %v", got, got, want)
	}
	t.Logf("✅ body return 数据上浮: Output=%v", cmd.Output)
}

// Edge Case: error 签名 + `return nil` → 正常完成且 Output 保持 nil（不把 nil error 误当数据）。
func TestReturnOutput_NilErrorReturn_OutputStaysNil(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecReturn(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	cmd, ok := findCompleteCmd(cmds)
	if !ok {
		t.Fatalf("应产 CmdCompleteWorkflow, got %+v", cmds)
	}
	if cmd.Output != nil {
		t.Fatalf("return nil 不应产生 Output, got %v", cmd.Output)
	}
	t.Logf("✅ 对照通过: return nil 时 Output 保持 nil")
}
