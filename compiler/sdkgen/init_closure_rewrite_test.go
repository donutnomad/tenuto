package sdkgen

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// 闭包写在 if 的初始化语句里（文档与 upgrade-demo 的
// `if err := workflow.SetUpdateHandler(ctx, ..., func(...) {...}); err != nil` 写法），闭包体里的 CallX 被重写两次，
// 生成的源码无法解析。同一个闭包写成 `setErr := workflow.SetUpdateHandler(...)` 再判断时重写正常。
func TestRewriteCallInsideClosureInIfInit(t *testing.T) {
	forms := map[string]string{
		"assign-then-if": `	setErr := workflow.SetUpdateHandler(ctx, "charge", func(ctx workflow.Context, id string) (ChargeOutput, error) {
		out, err := CallCharge(ctx, ChargeInput{OrderID: id}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out, err
	})
	if setErr != nil {
		return ChargeOutput{}, setErr
	}
	return ChargeOutput{}, nil`,
		"if-init": `	if err := workflow.SetUpdateHandler(ctx, "charge", func(ctx workflow.Context, id string) (ChargeOutput, error) {
		out, err := CallCharge(ctx, ChargeInput{OrderID: id}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out, err
	}); err != nil {
		return ChargeOutput{}, err
	}
	return ChargeOutput{}, nil`,
	}
	for name, body := range forms {
		t.Run(name, func(t *testing.T) {
			contracts, ownPath := rewriteFixture()
			source := `package orders

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type ChargeInput struct {
	OrderID string
}

type ChargeOutput struct {
	ChargeID string
}

func CallCharge(ctx workflow.Context, in ChargeInput, opts ...workflow.ActivityOptions) (ChargeOutput, error) {
	panic("bridge")
}

func W(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
` + body + `
}
`
			result, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
			if err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			if _, err := parser.ParseFile(token.NewFileSet(), "out.go", result.Source, 0); err != nil {
				t.Fatalf("rewritten source does not parse: %v\n%s", err, result.Source)
			}
			if n := strings.Count(string(result.Source), "ExecuteActivity"); n != 1 {
				t.Fatalf("ExecuteActivity appears %d times, want 1\n%s", n, result.Source)
			}
		})
	}
}
