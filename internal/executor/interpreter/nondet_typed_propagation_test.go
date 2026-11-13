package interpreter

import (
	"errors"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	"strings"
	"testing"
)

// ============================================
// typed non-det 传播回归锚
// ============================================
// 测试策略:
// - Error Case: R 类护栏 panic 经 WLang host 调用（safeCall）触发 → Execute 返回的 error
//   必须 errors.As 命中 nonDeterministicError。此前 safeCall 用 %v 把 panic 值洗成无类型
//   E_PANIC 字符串（错误链断裂），Block 分类（worker errors.As）静默失效——修复 =
//   wlang safeCall 对 error 类型 panic 值保留 Cause 链（LangError.Unwrap 现成）。
// 注: matchReplayWithHistory 失败路径不经 safeCall（executor 直返 typed error），其 errors.As
// 断言已由 wlang_replay_match_test.go 的 assertNonDeterministic 全系列（T7-T10）覆盖，此处不重复。
// ============================================

// Error Case: R 类 panic 穿过 wlang safeCall 后 errors.As 仍能判出 nonDeterministicError。
func TestNonDetError_SurvivesExecuteUnwrapped_RGuardPath(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func TypedGuardFlow(ctx workflow.Context, input map[string]any) error {
	var cfg map[string]any
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) any { return map[string]any{"region": "sg"} }).Get(ctx, &cfg)
	if err != nil {
		return err
	}
	return nil
}
`)
	nodes := compileWLangSource(t, src, "sdk-typed-guard", "TypedGuardFlow", "typed_guard_flow.go")

	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	taskCompleted := history.NewWorkflowTaskCompletedEvent(0, "")
	_, err := NewWorkflowExecutor(nodes, []history.Event{started, taskCompleted}).Execute()
	if err == nil {
		t.Fatal("replay 缺 SideEffect marker 应返回错误, got nil")
	}
	var ndErr executor.NonDeterministicError
	if !errors.As(err, &ndErr) {
		t.Fatalf("R 类护栏错误经 wlang safeCall 后应保留类型(errors.As 命中 nonDeterministicError), got %T: %v", err, err)
	}
	if !strings.Contains(ndErr.Error(), "no cached marker for SideEffect") {
		t.Fatalf("恢复出的 typed error 消息不符: %v", ndErr)
	}
	t.Logf("✅ R 类路径 typed 传播: %v", ndErr)
}
