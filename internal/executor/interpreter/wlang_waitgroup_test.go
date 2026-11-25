package interpreter

import (
	"strings"
	"testing"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// workflow.WaitGroup host 原语测试（用 Await 挂起机制实现，对标 Temporal waitGroupImpl :1908-1953）
// ============================================
// 测试策略：
// - Happy Path : 空组 Wait 立即返回 → workflow 完成（不 blocked、产 CmdCompleteWorkflow）
// - Edge Case  : Add(2) 后 Wait → n>0 挂起（blocked、无 complete）
// - 核心闭环   : Add(2) 后两次 Done → n==0 → Wait 立即返回 → 完成
// - Error Case : Add(-1) → 负计数 panic（守护 n<0 护栏，变异测试点）
// - Error Case : Add(1) + Done + Done → Done 多于 Add → 负计数 panic
// - 确定性     : 同历史多次重放，blocked/complete 稳定
// ============================================

// wgNewCall 构造 `let <varName> = workflow.NewWaitGroup(ctx)`。
func wgNewCall(varName string) map[string]any {
	return map[string]any{"let": map[string]any{varName: map[string]any{
		"NewWaitGroup": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
		},
	}}}
}

// wgIntCall 构造 `<wgVar>.<method>(ctx, <delta>)`（Add，delta 为 int64 字面量）。
func wgIntCall(wgVar, method string, delta int) map[string]any {
	return map[string]any{"expr": map[string]any{method: []any{
		map[string]any{"var": wgVar},
		map[string]any{"var": "ctx"},
		map[string]any{"literal": map[string]any{"type": "int64", "value": itoa(delta)}},
	}}}
}

// wgNoArgCall 构造 `<wgVar>.<method>(ctx)`（Done / Wait，仅 ctx 参数）。
func wgNoArgCall(wgVar, method string) map[string]any {
	return map[string]any{"expr": map[string]any{method: []any{
		map[string]any{"var": wgVar},
		map[string]any{"var": "ctx"},
	}}}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// wgProgram 构造一个只含 WaitGroup 语句的最小工作流体。
func wgProgram(stmts ...map[string]any) *programpkg.WorkflowConfig {
	program := make([]any, 0, len(stmts)+1)
	for _, s := range stmts {
		program = append(program, s)
	}
	program = append(program, map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}})
	return &programpkg.WorkflowConfig{
		Type:    "waitgroup-wf",
		Program: programpkg.NewWLangProgram(program),
	}
}

// Happy Path: 空组（NewWaitGroup 后直接 Wait，n==0）→ Wait 立即返回 → workflow 完成。
func TestWaitGroup_EmptyWait_ReturnsImmediately(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("空组 Wait 不应 blocked，got blocked=true")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("空组 Wait 应立即返回并完成 workflow，got %+v", commands)
	}
}

// Edge Case: Add(2) 后 Wait → n=2>0 → Wait 挂起（blocked，无 complete）。
func TestWaitGroup_AddThenWait_Blocks(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgIntCall("wg", "Add", 2),
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !exec.blocked {
		t.Fatalf("Add(2) 后 Wait 应 blocked（n>0），got blocked=false")
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("n>0 时 Wait 挂起，禁止完成 workflow，got %+v", commands)
	}
}

// 核心闭环: Add(2) + 两次 Done → n==0 → Wait 立即返回 → 完成。
func TestWaitGroup_AddThenTwoDone_Releases(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgIntCall("wg", "Add", 2),
		wgNoArgCall("wg", "Done"),
		wgNoArgCall("wg", "Done"),
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("两次 Done 后 n==0，Wait 不应 blocked，got blocked=true")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("n==0 时 Wait 应立即返回并完成 workflow，got %+v", commands)
	}
}

// Error Case: Add(-1) 单独调用 → n 变负 → panic("negative WaitGroup counter")。
// 变异点：删掉 host Add 里的 n<0 护栏，此断言必红。
func TestWaitGroup_AddNegative_Panics(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgIntCall("wg", "Add", -1),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	_, err := exec.Execute()
	if err == nil {
		t.Fatalf("Add(-1) 应 panic 负计数并转成 execErr，got err=nil")
	}
	if !strings.Contains(err.Error(), "negative WaitGroup counter") {
		t.Fatalf("负计数错误消息应含 'negative WaitGroup counter'，got %q", err.Error())
	}
}

// Error Case: Add(1) + Done + Done → 第二次 Done 令 n=-1 → panic 负计数（Done 多于 Add）。
func TestWaitGroup_DoneMoreThanAdd_Panics(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgIntCall("wg", "Add", 1),
		wgNoArgCall("wg", "Done"),
		wgNoArgCall("wg", "Done"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	_, err := exec.Execute()
	if err == nil {
		t.Fatalf("Done 多于 Add 应 panic 负计数，got err=nil")
	}
	if !strings.Contains(err.Error(), "negative WaitGroup counter") {
		t.Fatalf("负计数错误消息应含 'negative WaitGroup counter'，got %q", err.Error())
	}
}

// 确定性重放：Add(2)+Wait 挂起场景，同历史多次重放，blocked/complete 稳定。
func TestWaitGroup_ReplayDeterminism(t *testing.T) {
	nodes := wgProgram(
		wgNewCall("wg"),
		wgIntCall("wg", "Add", 2),
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	events := []history.Event{started}

	var refBlocked, refComplete bool
	for i := 0; i < 50; i++ {
		exec := NewWorkflowExecutor(nodes, events)
		commands, err := exec.Execute()
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		gotComplete := hasCompleteWorkflow(commands)
		if i == 0 {
			refBlocked = exec.blocked
			refComplete = gotComplete
			continue
		}
		if exec.blocked != refBlocked || gotComplete != refComplete {
			t.Fatalf("replay %d drift: blocked=%v complete=%v (ref blocked=%v complete=%v)",
				i, exec.blocked, gotComplete, refBlocked, refComplete)
		}
	}
	if !refBlocked || refComplete {
		t.Fatalf("Add(2)+Wait 参考轮应 blocked=true complete=false，got blocked=%v complete=%v", refBlocked, refComplete)
	}
}
