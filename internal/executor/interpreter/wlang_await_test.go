package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// workflow.Await host 方法测试（Goexit + 重放）
// ============================================
// 测试策略：
// - Happy Path: cond=true → 继续执行 → workflow 完成（产 CmdCompleteWorkflow）
// - P0 回归:   cond=false 且主流程无 pending → 设 e.blocked=true → 不误判完成（无 CmdCompleteWorkflow）
// - 双轮 replay: 第一轮 cond=false 设 blocked+Goexit → 第二轮历史置条件 → cond=true → 完成
// ============================================

// awaitReadyProgram 手工构造一个「主流程读 signal 值置 ready，然后 Await(ready)」的 WLang 程序：
//
//	let ready = false
//	let sig = zero(map<string,any>)
//	workflow.WaitSignal(ctx, "trigger").Get(ctx, sig)   // 历史有 signal → 非阻塞拿到值
//	set ready = sig["ready"].(boolean)                  // signal 值决定 ready
//	workflow.Await(ctx, func() bool { return ready })   // ready=false → blocked+Goexit；true → 继续
//	return null
//
// 之所以手工构造：直接手写 WLang AST 与现有 handler/activity 测试同款（不经 go2wlang），
// 让两轮 replay 用同一 program、不同 signal 值驱动 cond 从 false→true。
func awaitReadyProgram() *program.WorkflowConfig {
	condFn := map[string]any{
		"fn": map[string]any{
			"params":  []any{},
			"returns": []any{"boolean"},
			"do": []any{
				map[string]any{"return": map[string]any{"var": "ready"}},
			},
		},
	}
	return &program.WorkflowConfig{
		Type: "await-wf",
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"ready": map[string]any{"literal": map[string]any{"type": "boolean", "value": "false"}}}},
			map[string]any{"let": map[string]any{"sig": map[string]any{"zero": "map<string,any>"}}},
			map[string]any{"expr": map[string]any{"Get": []any{
				map[string]any{"WaitSignal": []any{
					map[string]any{"pkg": "workflow"},
					map[string]any{"var": "ctx"},
					map[string]any{"literal": map[string]any{"type": "string", "value": "trigger"}},
				}},
				map[string]any{"var": "ctx"},
				map[string]any{"out": "sig"},
			}}},
			// set ready = sig["ready"].(boolean)
			map[string]any{"set": map[string]any{"ready": map[string]any{"type.assert": []any{
				map[string]any{"map.value": []any{
					map[string]any{"var": "sig"},
					map[string]any{"literal": map[string]any{"type": "string", "value": "ready"}},
				}},
				map[string]any{"literal": map[string]any{"type": "string", "value": "boolean"}},
			}}}},
			map[string]any{"expr": map[string]any{"Await": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				condFn,
			}}},
			map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
		}),
	}
}

// awaitInlineProgram 构造一个「无任何 pending、只有一个 Await(内联 bool)」的最小 WLang 程序：
//
//	let ready = <ready>
//	workflow.Await(ctx, func() bool { return ready })
//	return null
//
// 用于 cond=true（继续完成）与 cond=false（P0 回归：blocked 阻止误判完成）两个单轮场景。
func awaitInlineProgram(ready bool) *program.WorkflowConfig {
	readyVal := "false"
	if ready {
		readyVal = "true"
	}
	condFn := map[string]any{
		"fn": map[string]any{
			"params":  []any{},
			"returns": []any{"boolean"},
			"do": []any{
				map[string]any{"return": map[string]any{"var": "ready"}},
			},
		},
	}
	return &program.WorkflowConfig{
		Type: "await-inline-wf",
		Program: program.NewWLangProgram([]any{
			map[string]any{"let": map[string]any{"ready": map[string]any{"literal": map[string]any{"type": "boolean", "value": readyVal}}}},
			map[string]any{"expr": map[string]any{"Await": []any{
				map[string]any{"pkg": "workflow"},
				map[string]any{"var": "ctx"},
				condFn,
			}}},
			map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
		}),
	}
}

func hasCompleteWorkflow(commands []history.Command) bool {
	for _, c := range commands {
		if c.Type == history.CmdCompleteWorkflow {
			return true
		}
	}
	return false
}

// Happy Path: cond 立即满足（内联 ready=true）→ Await 返回 → workflow 跑完 → 产 CmdCompleteWorkflow。
func TestAwait_ConditionTrue_Continues(t *testing.T) {
	nodes := awaitInlineProgram(true)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("cond=true 不应设 blocked，got blocked=true")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("cond=true 应继续并完成 workflow，got %+v", commands)
	}
}

// 🔴 P0 回归：主流程无任何 pending（无 activity/timer/child），只一个 cond=false 的 Await。
// cond=false → host 设 e.blocked=true 并 park，主协程没有返回 → 不产 CmdCompleteWorkflow。
// 若删掉 host 里的 e.blocked=true，此断言必红（守护 P0 漏洞不复现）。
func TestAwait_BlockedPreventsFalseCompletion(t *testing.T) {
	nodes := awaitInlineProgram(false)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !exec.blocked {
		t.Fatalf("cond=false 必须设 e.blocked=true（P0），got blocked=false")
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("cond=false 且无 pending 时禁止误判完成，got CmdCompleteWorkflow: %+v", commands)
	}
}

// 🔴 核心闭环（双轮 replay）：第一轮 signal 值 ready=false → Await 设 blocked+Goexit（无 complete）；
// 第二轮历史 signal 值 ready=true → Await cond=true → workflow 完成。
// 验证「设 blocked → Goexit → 下轮重放 → cond 满足 → 继续」的等待原语闭环。
func TestAwait_ConditionFalse_Goexit_ThenReplayPasses(t *testing.T) {
	nodes := awaitReadyProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	// 第一轮：signal trigger 值 ready=false → cond=false → blocked+Goexit。
	sigFalse := history.Event{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{
		Name: "trigger", Input: map[string]any{"ready": false},
	}}
	exec1 := NewWorkflowExecutor(nodes, []history.Event{started, sigFalse})
	commands1, err := exec1.Execute()
	if err != nil {
		t.Fatalf("first round: %v", err)
	}
	if !exec1.blocked {
		t.Fatalf("第一轮 cond=false 应设 e.blocked=true，got blocked=false")
	}
	if hasCompleteWorkflow(commands1) {
		t.Fatalf("第一轮 cond=false 禁止完成 workflow，got %+v", commands1)
	}

	// 第二轮：signal trigger 值 ready=true → cond=true → 继续 → 完成。
	sigTrue := history.Event{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{
		Name: "trigger", Input: map[string]any{"ready": true},
	}}
	exec2 := NewWorkflowExecutor(nodes, []history.Event{started, sigTrue})
	commands2, err := exec2.Execute()
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	if exec2.blocked {
		t.Fatalf("第二轮 cond=true 不应设 blocked，got blocked=true")
	}
	if !hasCompleteWorkflow(commands2) {
		t.Fatalf("第二轮 cond=true 应完成 workflow，got %+v", commands2)
	}
}

// 确定性重放：同一历史多次重放，命令集合稳定（cond 求值 + blocked 判定确定）。
func TestAwait_ReplayDeterminism(t *testing.T) {
	nodes := awaitReadyProgram()
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	sigFalse := history.Event{ID: 2, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{
		Name: "trigger", Input: map[string]any{"ready": false},
	}}
	events := []history.Event{started, sigFalse}

	var refBlocked bool
	var refComplete bool
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
		t.Fatalf("cond=false 参考轮应 blocked=true complete=false，got blocked=%v complete=%v", refBlocked, refComplete)
	}
}
