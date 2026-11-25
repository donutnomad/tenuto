package interpreter

import (
	"testing"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// workflow.Channel host 原语 executor 级端到端测试
// ============================================
// 照 Temporal channelImpl 移植（internal_workflow.go）：
//   - Send            :946-973（sendAsyncImpl + valueConsumed 循环 + closed panic + unblocked）
//   - Receive         :796-838（receiveAsyncImpl 外层 + hasResult 内层 yield + assignValue + unblocked）
//   - sendAsyncImpl   :979-1000（closed panic → 触发 blockedReceives → 入 buffer → 登记 blockedSend）
//   - receiveAsyncImpl:887-926（recValue → buffer FIFO → closed → 触发 blockedSends → 登记 blockedReceive）
//   - Close           :1002-1010（closed=true + 唤醒 blockedReceives more=false）
//
// 测试策略：
// - Happy Path : buffered channel send 不阻塞入 buffer、receive FIFO 取（:992-994 / :893-909）
// - Happy Path : 跨 lane 通信——lane0 send、lane1 receive（sendAsyncImpl 触发 receiver callback，:983-991）
// - Edge Case  : close 唤醒阻塞的 receiver → more=false（:1002-1010 → Receive :814-816）
// - P0 回归    : unbuffered send 无 receiver → blocked=true → 不误判完成（变异点：Send 漏设 blocked）
// ============================================

// chNewBuffered 构造 `let <varName> = workflow.NewBufferedChannel(ctx, size)`。
func chNewBuffered(varName string, size int) map[string]any {
	return map[string]any{"let": map[string]any{varName: map[string]any{
		"NewBufferedChannel": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
			map[string]any{"literal": map[string]any{"type": "int64", "value": itoa(size)}},
		},
	}}}
}

// chNew 构造 `let <varName> = workflow.NewChannel(ctx)`（非缓冲）。
func chNew(varName string) map[string]any {
	return map[string]any{"let": map[string]any{varName: map[string]any{
		"NewChannel": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
		},
	}}}
}

// chSend 构造 `<chVar>.Send(ctx, <stringLiteral>)`。
func chSend(chVar, value string) map[string]any {
	return map[string]any{"expr": map[string]any{"Send": []any{
		map[string]any{"var": chVar},
		map[string]any{"var": "ctx"},
		map[string]any{"literal": map[string]any{"type": "string", "value": value}},
	}}}
}

// chReceiveInto 构造 `let <outVar> = zero(map<string,any>); let <moreVar> = <chVar>.Receive(ctx, &<outVar>)`。
// 返回两条语句（zero 声明 + Receive 调用），outVar 经 {"out":...} 传引用（照 ActivityFuture.Get 的 out）。
func chReceiveInto(chVar, outVar, moreVar string) []map[string]any {
	return []map[string]any{
		{"let": map[string]any{outVar: map[string]any{"zero": "map<string,any>"}}},
		{"let": map[string]any{moreVar: map[string]any{"Receive": []any{
			map[string]any{"var": chVar},
			map[string]any{"var": "ctx"},
			map[string]any{"out": outVar},
		}}}},
	}
}

// chClose 构造 `<chVar>.Close(ctx)`。
func chClose(chVar string) map[string]any {
	return map[string]any{"expr": map[string]any{"Close": []any{
		map[string]any{"var": chVar},
		map[string]any{"var": "ctx"},
	}}}
}

// chProgram 构造一个只含 channel 语句的最小工作流体（末尾补 return null）。
func chProgram(activities map[string]programpkg.ActivityRef, stmts ...map[string]any) *programpkg.WorkflowConfig {
	program := make([]any, 0, len(stmts)+1)
	for _, s := range stmts {
		program = append(program, s)
	}
	program = append(program, map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}})
	return &programpkg.WorkflowConfig{
		Type:       "channel-wf",
		Activities: activities,
		Program:    programpkg.NewWLangProgram(program),
	}
}

// 🔴 Happy Path（buffered FIFO，照 Temporal sendAsyncImpl :992-994 入 buffer + receiveAsyncImpl :893-909 FIFO）：
// 容量 2 的 buffered channel，两次 Send 不阻塞入 buffer，两次 Receive 按 FIFO 取回 "a"、"b"。
// 全程 main lane 单线、无阻塞 → workflow 完成（不 blocked、产 CmdCompleteWorkflow）。
func TestChannel_Buffered_SendReceiveFIFO(t *testing.T) {
	stmts := []map[string]any{
		chNewBuffered("ch", 2),
		chSend("ch", "a"),
		chSend("ch", "b"),
	}
	stmts = append(stmts, chReceiveInto("ch", "r1", "m1")...)
	stmts = append(stmts, chReceiveInto("ch", "r2", "m2")...)
	nodes := chProgram(nil, stmts...)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("buffered send/receive 全部当场完成，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("buffered FIFO 全程无阻塞应完成 workflow，got %+v", commands)
	}
}

// 🔴 P0 回归（unbuffered send 无 receiver → blocked，变异点：Send 漏设 e.blocked=true）：
// 非缓冲 channel 上 Send 无 receiver、buffer 满（size=0）→ sendAsyncImpl 返回 false → 登记 blockedSend
// → Send 检查-yield 循环设 blocked=true + park。主协程没有返回 → 不产 CmdCompleteWorkflow。
// 变异：删掉 host Send 里的 e.blocked=true，此断言必红（工作流被误判完成）。
func TestChannel_Unbuffered_SendNoReceiver_Blocks(t *testing.T) {
	nodes := chProgram(nil,
		chNew("ch"),
		chSend("ch", "a"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !exec.blocked {
		t.Fatalf("unbuffered send 无 receiver 应 blocked=true（P0），got blocked=false")
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("send 阻塞时禁止完成 workflow，got %+v", commands)
	}
}

// laneFn 构造一条 lane 闭包 func(ctx){ do... }（非结构化 wg.Go 的 lane 签名，returns:[]），
// 闭包体捕获外层作用域的 ch/wg 变量（WLang 闭包捕获）。
func laneFn(do ...map[string]any) map[string]any {
	body := make([]any, 0, len(do))
	for _, s := range do {
		body = append(body, s)
	}
	return map[string]any{
		"fn": map[string]any{
			"params":  []any{[]any{"ctx", "workflow.Context"}},
			"returns": []any{},
			"do":      body,
		},
	}
}

// wgGoLane 构造 `<wgVar>.Go(ctx, <laneFn>)`。
func wgGoLane(wgVar string, lane map[string]any) map[string]any {
	return map[string]any{"expr": map[string]any{"Go": []any{
		map[string]any{"var": wgVar},
		map[string]any{"var": "ctx"},
		lane,
	}}}
}

// 🔴 Happy Path（跨 lane 通信，照 Temporal sendAsyncImpl 触发 blockedReceives callback :983-991）：
// lane0 Send、lane1 Receive（各自捕获外层 ch）。unbuffered channel 上，先跑的一方阻塞 park，
// 后跑的一方经 sendAsyncImpl/receiveAsyncImpl 触发对方 callback → 下轮阻塞方醒来命中 → 双方 return →
// wg.Done×2 → n=0 → main wg.Wait 放行 → CmdCompleteWorkflow。
// 验证：不死循环、收敛正确、经 Wait 放行收尾。
// 变异点：sendAsyncImpl 不触发 blockedReceives callback（或 host 没接线唤醒）→ receiver 永不醒 → 超时/永 blocked。
func TestChannel_CrossLane_SendWakesReceive(t *testing.T) {
	sendLane := laneFn(chSend("ch", "hello"))
	recvLane := laneFn(chReceiveInto("ch", "r", "m")...)
	nodes := chProgram(nil,
		chNew("ch"),
		wgNewCall("wg"),
		wgGoLane("wg", sendLane),
		wgGoLane("wg", recvLane),
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("send/receive 跨 lane 配对成功后双方 return、n=0、Wait 放行，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("跨 lane send 唤醒 receive、双方完成后应经 Wait 放行完成 workflow，got %+v", commands)
	}
}

// 🔴 Happy Path（跨 lane，receiver 先阻塞 park，sender 后到唤醒——显式声明序 receiver 在前）：
// lane0 Receive（先跑，无值 → 登记 blockedReceive + park）、lane1 Send（后跑，sendAsyncImpl 触发
// lane0 的 callback 设 hasResult）→ 下轮 lane0 内层醒来命中 → 双方完成 → Wait 放行完成。
// 验证 blockedReceive 被 send 唤醒的路径（与上一个测试的声明序相反，覆盖 receiver-先 park 分支）。
func TestChannel_CrossLane_ReceiveFirstThenSend(t *testing.T) {
	recvLane := laneFn(chReceiveInto("ch", "r", "m")...)
	sendLane := laneFn(chSend("ch", "world"))
	nodes := chProgram(nil,
		chNew("ch"),
		wgNewCall("wg"),
		wgGoLane("wg", recvLane), // 先声明 → 先跑 → 先 park
		wgGoLane("wg", sendLane), // 后跑 → 唤醒 receiver
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("receiver 先 park、sender 后唤醒后双方 return、n=0、Wait 放行，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("receiver 先阻塞、send 唤醒后应经 Wait 放行完成 workflow，got %+v", commands)
	}
}

// 🔴 Edge Case（close 唤醒阻塞 receiver，照 Temporal Close :1002-1010 → Receive :814-816 返回 more=false）：
// lane0 Receive（先跑、无值 → park）、lane1 Close（后跑 → 唤醒 blockedReceives，more=false）→
// 下轮 lane0 醒来见 closed&empty → Receive 返回 more=false → lane0 return → wg.Done → n=0 → Wait 放行完成。
// 验证 close 能唤醒阻塞的 receiver、工作流正常收尾（不死循环、不永 blocked）。
func TestChannel_Close_WakesBlockedReceiver(t *testing.T) {
	recvLane := laneFn(chReceiveInto("ch", "r", "m")...)
	closeLane := laneFn(chClose("ch"))
	nodes := chProgram(nil,
		chNew("ch"),
		wgNewCall("wg"),
		wgGoLane("wg", recvLane),  // 先跑 → 无值 → park
		wgGoLane("wg", closeLane), // 后跑 → close 唤醒 receiver（more=false）
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("close 唤醒 receiver（more=false）后 receiver return、n=0、Wait 放行，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("close 唤醒阻塞 receiver 后应经 Wait 放行完成 workflow，got %+v", commands)
	}
}
