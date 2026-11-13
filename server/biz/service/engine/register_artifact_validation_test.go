package engine

import (
	"strings"
	"testing"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	wlangtypes "github.com/donutnomad/wlang/types"
)

func wlangWorkflow(program programpkg.WLangProgram) *programpkg.WorkflowConfig {
	return &programpkg.WorkflowConfig{
		ID:      "artifact-validation-wf",
		Program: program,
	}
}

func validV2Program() programpkg.WLangProgram {
	return programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
		Program:          []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null"}}}},
	}
}

// TestRegisterRejectsBadEntryDescriptor：注册期校验生效——entry 指向不存在的
// descriptor 的 artifact 不再能注册进去（此前要到执行时才炸）。
func TestRegisterRejectsBadEntryDescriptor(t *testing.T) {
	program := validV2Program()
	program.Entry = programpkg.WLangEntryDescriptor{InputVar: "in", Input: "orders.Missing"}

	_, err := newTestServer().Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wlangWorkflow(program))
	if err == nil {
		t.Fatal("entry 指向不存在的 descriptor，注册应被拒")
	}
	if !strings.Contains(err.Error(), "has no type descriptor") {
		t.Fatalf("错误信息不符: %v", err)
	}
}

// TestRegisterAcceptsValidArtifacts 守住影响面：注册期校验放行合法 artifact，拒绝没有 schema 的程序。
func TestRegisterAcceptsValidArtifacts(t *testing.T) {
	t.Run("没有 schema 的程序", func(t *testing.T) {
		bare := programpkg.WLangProgram{
			Program: []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null"}}}},
		}
		if _, err := newTestServer().Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wlangWorkflow(bare)); err == nil {
			t.Fatal("没有 schema 的程序应被拒绝")
		}
	})

	t.Run("零值 Entry 的 v2 artifact", func(t *testing.T) {
		if _, err := newTestServer().Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wlangWorkflow(validV2Program())); err != nil {
			t.Fatalf("零值 Entry 应放行（非 typed workflow 必须能注册）: %v", err)
		}
	})

	t.Run("合法 Entry 的 v2 artifact", func(t *testing.T) {
		d := wlangtypes.TypeDescriptor{
			ID:           "orders.In",
			LanguageName: "orders.In",
			Kind:         "record",
			Fields:       []wlangtypes.FieldDescriptor{{GoName: "A", JSONName: "a", TypeID: "int64"}},
		}
		d.Fingerprint = wlangtypes.DescriptorFingerprint(d)

		program := validV2Program()
		program.Types = map[string]programpkg.WLangTypeDescriptor{d.ID: d}
		program.Entry = programpkg.WLangEntryDescriptor{InputVar: "in", Input: d.ID}

		if _, err := newTestServer().Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wlangWorkflow(program)); err != nil {
			t.Fatalf("合法 typed artifact 应放行: %v", err)
		}
	})
}
