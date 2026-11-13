package sdkgen

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
)

func generatedArtifact(t *testing.T, src, funcName string) program.WLangProgram {
	t.Helper()
	result, err := GenerateFromSource([]byte(src), Options{
		WorkflowID: "entry-metadata-wf",
		FuncName:   funcName,
		Filename:   "workflow.go",
	})
	if err != nil {
		t.Fatalf("GenerateFromSource: %v", err)
	}
	return result.Workflow.Program
}

// TestGeneratedArtifactCarriesEntryMetadata：sdkgen 把 go2wlang 提取的 entry
// 透传进 artifact，且 entry 指向的 ID 必须是 Types 的 key——否则运行时
// DecodeJSONValue 会因 "no JSON descriptor" 失败，而 artifact 看起来是合法的。
func TestGeneratedArtifactCarriesEntryMetadata(t *testing.T) {
	src := `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type OrderInput struct {
	Amount int64 ` + "`json:\"amount\"`" + `
}

type PaymentOutput struct {
	OK bool ` + "`json:\"ok\"`" + `
}

func OrderWorkflow(ctx workflow.Context, in OrderInput) (PaymentOutput, error) {
	return PaymentOutput{OK: true}, nil
}`

	artifact := generatedArtifact(t, src, "OrderWorkflow")

	if artifact.Entry.InputVar != "in" {
		t.Errorf("Entry.InputVar = %q, want \"in\"", artifact.Entry.InputVar)
	}
	if artifact.Entry.Input == "" || artifact.Entry.Output == "" {
		t.Fatalf("typed workflow 应同时有 Input/Output: %+v", artifact.Entry)
	}
	for _, ref := range []struct{ field, id string }{
		{"Input", artifact.Entry.Input},
		{"Output", artifact.Entry.Output},
	} {
		if _, ok := artifact.Types[ref.id]; !ok {
			t.Fatalf("Entry.%s = %q 不是 Types 的 key（keys: %v）", ref.field, ref.id, typeKeys(artifact.Types))
		}
	}

	// 生成的 artifact 必须能过注册期校验，否则生产端产出的东西注册不进去。
	if err := artifact.ValidateArtifact(); err != nil {
		t.Fatalf("生成的 artifact 未通过 ValidateArtifact: %v", err)
	}
}

// TestNonTypedWorkflowKeepsZeroEntry 守住版本不变性：无入参、无业务返回的 workflow
// Entry 是零值，artifact 序列化逐字节不变、版本号不翻转。
func TestNonTypedWorkflowKeepsZeroEntry(t *testing.T) {
	artifact := generatedArtifact(t, `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

func BareWorkflow(ctx workflow.Context) error { return nil }`, "BareWorkflow")
	if artifact.Entry != (program.WLangEntryDescriptor{}) {
		t.Fatalf("无入参 workflow 的 Entry 应为零值: %+v", artifact.Entry)
	}
}

// TestMapInputCarriesInputVar：map[string]any 入参不叫 input 时，Entry 只带 InputVar，
// Worker 按它绑定入参；此前 InputVar 为空，body 引用 req 时报 [E_SYMBOL]。
func TestMapInputCarriesInputVar(t *testing.T) {
	artifact := generatedArtifact(t, `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

func MapWorkflow(ctx workflow.Context, req map[string]any) error { return nil }`, "MapWorkflow")
	if want := (program.WLangEntryDescriptor{InputVar: "req"}); artifact.Entry != want {
		t.Fatalf("Entry = %+v, want %+v", artifact.Entry, want)
	}
	if err := artifact.ValidateArtifact(); err != nil {
		t.Fatalf("ValidateArtifact: %v", err)
	}
}

// TestWorkflowInputMustBeJSONObject：Run 的 Input 是 JSON object，标量、切片与 any 入参
// 在生成时报出 文件:行:列。
func TestWorkflowInputMustBeJSONObject(t *testing.T) {
	for _, tc := range []struct{ name, param, wantType string }{
		{"标量", "name string", "string"},
		{"切片", "ids []string", "[]string"},
		{"DTO 切片", "items []Item", "[]Item"},
		{"any", "payload any", "any"},
		{"命名标量", "code Code", "Code"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type Item struct {
	ID string ` + "`json:\"id\"`" + `
}

type Code string

func W(ctx workflow.Context, ` + tc.param + `) error { return nil }`
			_, err := GenerateFromSource([]byte(src), Options{WorkflowID: "wf", FuncName: "W", Filename: "workflow.go"})
			if err == nil {
				t.Fatal("非 JSON object 入参应在生成时报错")
			}
			if want := "workflow.go:11:"; !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "输入类型 "+tc.wantType+" 不是 JSON object") {
				t.Fatalf("错误应含位置 %q 与类型 %s: %v", want, tc.wantType, err)
			}
		})
	}
	for _, param := range []string{"in Item", "in *Item", "req map[string]any", "byID map[string]Item"} {
		src := `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type Item struct {
	ID string ` + "`json:\"id\"`" + `
}

func W(ctx workflow.Context, ` + param + `) error { return nil }`
		if _, err := GenerateFromSource([]byte(src), Options{WorkflowID: "wf", FuncName: "W", Filename: "workflow.go"}); err != nil {
			t.Fatalf("%s 应通过生成: %v", param, err)
		}
	}
}

func typeKeys(m map[string]program.WLangTypeDescriptor) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
