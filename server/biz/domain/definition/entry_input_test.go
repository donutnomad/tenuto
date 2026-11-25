package definition_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/infra/memory"
	wlangtypes "github.com/donutnomad/wlang/types"
)

// typedEntryWorkflow 造一个声明了 typed 入参的 workflow config：
// entry.input 指向 orders.In{amount int64}。
func typedEntryWorkflow(id string) *program.WorkflowConfig {
	d := wlangtypes.TypeDescriptor{
		ID:           "orders.In",
		LanguageName: "orders.In",
		Kind:         "record",
		Fields:       []wlangtypes.FieldDescriptor{{GoName: "Amount", JSONName: "amount", TypeID: "int64"}},
	}
	d.Fingerprint = wlangtypes.DescriptorFingerprint(d)

	return &program.WorkflowConfig{
		Type: id,
		Program: program.WLangProgram{
			Schema:           program.WLangProgramSchemaV2,
			Lang:             program.WLangLanguageV1,
			HandlerABISchema: program.WLangHandlerABISchemaV1,
			Program:          []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null"}}}},
			Types:            map[string]program.WLangTypeDescriptor{d.ID: d},
			Entry:            program.WLangEntryDescriptor{InputVar: "in", Input: d.ID},
		},
	}
}

// saveConfig 把配置保存进内存仓储并返回仓储与引用。
func saveConfig(t *testing.T, workflow *program.WorkflowConfig) (definition.ConfigRepository, definition.WorkflowConfigRef) {
	t.Helper()
	repo := memory.NewDefinitionRepository()
	ref, err := repo.SaveWorkflowConfigInNamespace("", workflow)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	return repo, ref
}

// TestCheckEntryInputRejectsMismatchedChildInput 是服务端跨 workflow 校验的锁。
//
// 这条校验只能在服务端做：目标 workflow 的 config version 是服务端解析命令时才
// 确定的（空 version 取 latest），executor 产 CmdStartChildWorkflow 时并不知道
// 目标是谁的哪个版本，拿不到它的 Entry.Input。
func TestCheckEntryInputRejectsMismatchedChildInput(t *testing.T) {
	repo, ref := saveConfig(t, typedEntryWorkflow("child-typed"))

	// amount 声明为 int64，传字符串应被拒。
	err := definition.CheckEntryInput(repo, ref, map[string]any{"amount": "not-a-number"})
	if err == nil {
		t.Fatal("载荷与目标入口类型不符时应报错")
	}
	if !strings.Contains(err.Error(), "does not match entry type") {
		t.Fatalf("错误信息应指明是入口类型不匹配: %v", err)
	}
}

func TestCheckEntryInputAcceptsMatchingInput(t *testing.T) {
	repo, ref := saveConfig(t, typedEntryWorkflow("child-typed-ok"))
	if err := definition.CheckEntryInput(repo, ref, map[string]any{"amount": 42}); err != nil {
		t.Fatalf("合法载荷应放行: %v", err)
	}
}

// TestCheckEntryInputSkipsUntypedWorkflow：没有 typed 入参的 workflow
// （标量入参、无入参）不受影响，任意载荷都放行。
func TestCheckEntryInputSkipsUntypedWorkflow(t *testing.T) {
	untyped := &program.WorkflowConfig{
		Type: "child-untyped",
		Program: program.WLangProgram{
			Program: []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null"}}}},
		},
	}
	repo, ref := saveConfig(t, untyped)
	if err := definition.CheckEntryInput(repo, ref, map[string]any{"anything": "goes"}); err != nil {
		t.Fatalf("无 typed 入参的 workflow 应放行任意载荷: %v", err)
	}
}

// TestCheckEntryInputMissingConfig：目标版本不存在时返回仓储的 not-found 错误。
func TestCheckEntryInputMissingConfig(t *testing.T) {
	repo := memory.NewDefinitionRepository()
	err := definition.CheckEntryInput(repo, definition.WorkflowConfigRef{WorkflowType: "missing", Version: "v"}, nil)
	if !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("error = %v, want ErrWorkflowConfigNotFound", err)
	}
}
