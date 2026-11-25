package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

// TC-G1
// Happy Path（Layer 0 前置闸门）：工作流注册 SetQueryHandler（单返回值内联闭包）后，go2wlang 应把 workflow.SetQueryHandler(...) 宿主调用按调用名
// AST 透传到生成的 WLang 程序中（闭包登记）。
// 目的：验证 B1 动态 Query Handler 全案的最前置依赖——DSL marker 能被翻译层透传，无需改
// go2wlang 主路径。若本测试失败即触发 S1 预案（仅调 marker 签名匹配已支持形态）。
// 预期：生成的 program JSON 中包含 "SetQueryHandler" host 调用。
func TestGenerateFromSource_SetQueryHandler_KeepsClosure(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func CounterWorkflow(ctx workflow.Context, input map[string]any) error {
	workflow.SetQueryHandler(ctx, "getCount", func(ctx workflow.Context, in map[string]any) map[string]any {
		return in
	})
	return nil
}
`)

	result, err := GenerateFromSource(src, Options{WorkflowType: "counter-flow", FuncName: "CounterWorkflow", Filename: "counter.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	programBytes, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatalf("marshal program: %v", err)
	}
	prog := string(programBytes)

	if !strings.Contains(prog, "SetQueryHandler") {
		t.Fatalf("expected SetQueryHandler host call kept in program, got:\n%s", prog)
	}
}

// 第 9 项：query handler 的形状在生成期校验，错误带文件与行号。
func TestGenerateFromSource_SetQueryHandler_RejectsWrongShape(t *testing.T) {
	cases := map[string]struct {
		call string
		want string
	}{
		"not inline":       {`workflow.SetQueryHandler(ctx, "q", handler)`, "handler must be an inline function"},
		"no context":       {`workflow.SetQueryHandler(ctx, "q", func(in map[string]any) map[string]any { return in })`, "func(ctx workflow.Context, input map[string]any) map[string]any"},
		"typed input":      {`workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in string) map[string]any { return nil })`, "func(ctx workflow.Context, input map[string]any) map[string]any"},
		"error result":     {`workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) (map[string]any, error) { return in, nil })`, "func(ctx workflow.Context, input map[string]any) map[string]any"},
		"scalar result":    {`workflow.SetQueryHandler(ctx, "q", func(ctx workflow.Context, in map[string]any) int { return 1 })`, "func(ctx workflow.Context, input map[string]any) map[string]any"},
		"empty query name": {`workflow.SetQueryHandler(ctx, "", func(ctx workflow.Context, in map[string]any) map[string]any { return in })`, "query name is empty"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	var handler func(ctx workflow.Context, in map[string]any) map[string]any
	_ = handler
	if err := ` + tc.call + `; err != nil {
		return err
	}
	return nil
}
`)
			_, err := GenerateFromSource(src, Options{WorkflowType: "q", FuncName: "W", Filename: "q.go"})
			if err == nil {
				t.Fatal("generation accepted a mis-shaped query handler")
			}
			for _, want := range []string{tc.want, "q.go:8", "workflow.SetQueryHandler"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// handler 的 Context 形参必须是 sdk/workflow 的 Context，其他包里同名的 Context 不算。
func TestGenerateFromSource_HandlerRejectsForeignContext(t *testing.T) {
	for name, call := range map[string]string{
		"query":  `workflow.SetQueryHandler(ctx, "q", func(c context.Context, in map[string]any) map[string]any { return in })`,
		"update": `workflow.SetUpdateHandler(ctx, "u", func(c context.Context) error { return nil })`,
	} {
		t.Run(name, func(t *testing.T) {
			src := []byte(`package flows

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func W(ctx workflow.Context, input map[string]any) error {
	if err := ` + call + `; err != nil {
		return err
	}
	return nil
}

var _ context.Context
`)
			_, err := GenerateFromSource(src, Options{WorkflowType: "q", FuncName: "W", Filename: "q.go"})
			if err == nil || !strings.Contains(err.Error(), "workflow.Context") {
				t.Fatalf("err = %v, want a workflow.Context shape error", err)
			}
		})
	}
}
