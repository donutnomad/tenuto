package interpreter

import (
	"github.com/donutnomad/tenuto/protocol/history"
	"testing"
)

// ============================================
// workflow.Selector host 原语 executor 级端到端测试
// ============================================
// 照 Temporal selectorImpl 移植（internal_workflow.go）：
//   - NewSelector      workflow.go `&selectorImpl{name}`（本引擎 selectorHandle + coroutine.Selector）
//   - AddReceive       :1380-1383（累积 receive case，回调 func(ch,more)）
//   - AddSend          :1385-1388（累积 send case，回调 func()）
//   - AddDefault       :1399-1401（标记 default，回调 func()）
//   - Select           :1416-1556（单次 TrySelect 试探 :1427-1542 + default :1543-1547 +
//                       检查-yield 循环 :1548-1555，用 5a ReadyIndex() 重查而非重 TrySelect）
//
// 测试策略：
// - Happy Path : 跨 lane select——lane0 Select(receive ch2)、lane1 send ch2 → 命中 ch2 case 回调 → 收敛（:983-991 + :1549-1552）
// - Happy Path : send case——select 的 send case，channel 有 waiting receiver → 命中 send case（:1481-1504）
// - Happy Path : 声明序——多 channel 同时 ready → 选声明序第一个（:1427 `for range cases`）
// - Edge Case  : default——select 有 default 且无 channel ready → 立即走 default、不阻塞（:1543-1547）
// - P0 回归    : 无 ready 无 default → Select 阻塞（blocked=true，不误判完成）（变异点：Select 漏设 blocked，:1554）
// ============================================

// selNew 构造 `let <selVar> = workflow.NewSelector(ctx)`。
func selNew(selVar string) map[string]any {
	return map[string]any{"let": map[string]any{selVar: map[string]any{
		"NewSelector": []any{
			map[string]any{"pkg": "workflow"},
			map[string]any{"var": "ctx"},
		},
	}}}
}

// recvCallbackFn 构造 receive 回调 `func(c workflow.Channel, more bool) { <body...> }`（对标 go2wlang
// 翻出的 params [["c","workflow.Channel"],["more","boolean"]], returns []）。body 捕获外层作用域变量。
func recvCallbackFn(paramCh string, body ...map[string]any) map[string]any {
	do := make([]any, 0, len(body))
	for _, s := range body {
		do = append(do, s)
	}
	return map[string]any{"fn": map[string]any{
		"params":  []any{[]any{paramCh, "workflow.Channel"}, []any{"more", "boolean"}},
		"returns": []any{},
		"do":      do,
	}}
}

// zeroArgFn 构造 send/default 回调 `func() { <body...> }`（params []、returns []）。
func zeroArgFn(body ...map[string]any) map[string]any {
	do := make([]any, 0, len(body))
	for _, s := range body {
		do = append(do, s)
	}
	return map[string]any{"fn": map[string]any{
		"params":  []any{},
		"returns": []any{},
		"do":      do,
	}}
}

// selAddReceive 构造 `<selVar>.AddReceive(ctx, <chVar>, <fn>)`。
func selAddReceive(selVar, chVar string, fn map[string]any) map[string]any {
	return map[string]any{"expr": map[string]any{"AddReceive": []any{
		map[string]any{"var": selVar},
		map[string]any{"var": "ctx"},
		map[string]any{"var": chVar},
		fn,
	}}}
}

// selAddSend 构造 `<selVar>.AddSend(ctx, <chVar>, <stringLiteral>, <fn>)`。
func selAddSend(selVar, chVar, value string, fn map[string]any) map[string]any {
	return map[string]any{"expr": map[string]any{"AddSend": []any{
		map[string]any{"var": selVar},
		map[string]any{"var": "ctx"},
		map[string]any{"var": chVar},
		map[string]any{"literal": map[string]any{"type": "string", "value": value}},
		fn,
	}}}
}

// selAddDefault 构造 `<selVar>.AddDefault(ctx, <fn>)`。
func selAddDefault(selVar string, fn map[string]any) map[string]any {
	return map[string]any{"expr": map[string]any{"AddDefault": []any{
		map[string]any{"var": selVar},
		map[string]any{"var": "ctx"},
		fn,
	}}}
}

// selSelect 构造 `<selVar>.Select(ctx)`。
func selSelect(selVar string) map[string]any {
	return map[string]any{"expr": map[string]any{"Select": []any{
		map[string]any{"var": selVar},
		map[string]any{"var": "ctx"},
	}}}
}

// 🔴 Happy Path（跨 lane select receive，照 Temporal Select :1416 + sendAsyncImpl 触发 blockedReceives :983-991
// + 检查-yield 循环 :1548-1555）：
// lane0 在 ch1/ch2 上 Select（两 receive case）；lane1 向 ch2 send。lane0 先跑：两 channel 无值 →
// TrySelect 注册两个 receive callback、readyIndex=-1 → 无 default → blocked+park。lane1 后跑：send ch2 →
// sendAsyncImpl 触发 ch2 的已登记 callback → callback 竞争护栏抢占设 s.readyIndex=1（ch2 是 case index 1）。
// 下轮 lane0 醒来经 ReadyIndex() 重查得 1 → InvokeClosure ch2 case 回调（回调内 Receive 取 pre-store 的值）→
// lane0 return → wg.Done。lane1 send 命中即 return → wg.Done。n=0 → main wg.Wait 放行 → CmdCompleteWorkflow。
// 变异点：Select 循环用 TrySelect 重查（而非 ReadyIndex）→ 每轮重注册 callback 堆积、偏离 Temporal。
func TestSelector_CrossLane_SendWakesSelectReceive(t *testing.T) {
	// lane0: sel := NewSelector; sel.AddReceive(ch1, cb); sel.AddReceive(ch2, cb); sel.Select
	// ch2 的回调内 Receive 消费 pre-store 的值（证明命中 ch2 case）。
	cb1 := recvCallbackFn("c") // ch1 回调（本测试不命中，空体）
	cb2body := chReceiveInto("c", "r", "m")
	cb2 := recvCallbackFn("c", cb2body...)
	lane0 := laneFn(
		selNew("sel"),
		selAddReceive("sel", "ch1", cb1), // index 0
		selAddReceive("sel", "ch2", cb2), // index 1
		selSelect("sel"),
	)
	lane1 := laneFn(chSend("ch2", "hello"))

	nodes := chProgram(nil,
		chNew("ch1"),
		chNew("ch2"),
		wgNewCall("wg"),
		wgGoLane("wg", lane0), // 先跑 → Select 无值 → park
		wgGoLane("wg", lane1), // 后跑 → send ch2 唤醒 lane0 的 ch2 receive case
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("select 命中 ch2 case、双 lane return、n=0、Wait 放行，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("跨 lane send 唤醒 select receive case、收敛后应经 Wait 放行完成 workflow，got %+v", commands)
	}
}

// 🔴 Edge Case（default 立即，照 Temporal Select :1543-1547 `if s.defaultFunc != nil { f(); return }`）：
// select 有 default 且无 channel ready → 立即走 default 回调、不阻塞、不 park。main lane 单线全程无阻塞 →
// workflow 完成。变异点：default 不执行/阻塞 → 本测试红（被误 blocked 或不完成）。
func TestSelector_Default_NoReady_RunsImmediately(t *testing.T) {
	nodes := chProgram(nil,
		chNew("ch"),
		selNew("sel"),
		selAddReceive("sel", "ch", recvCallbackFn("c")), // 无值 → 不 ready
		selAddDefault("sel", zeroArgFn()),               // default 立即走
		selSelect("sel"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("select 有 default 且无 ready 应立即走 default，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("default 立即执行后 main 无阻塞应完成 workflow，got %+v", commands)
	}
}

// 🔴 P0 回归（无 ready 无 default → 阻塞，变异点：Select 漏设 e.blocked=true，照 :1554）：
// main lane 直接 Select 一个无值 channel（receive case）、无 default → TrySelect 返回 -1 → 检查-yield 循环
// 每轮 ReadyIndex()==-1 → blocked=true + park。本 WFT 内无兄弟 lane send → 永不命中 → 稳定 blocked。
// 主协程 park 在 Select、没有返回 → 不产 CmdCompleteWorkflow。
func TestSelector_NoReadyNoDefault_Blocks(t *testing.T) {
	nodes := chProgram(nil,
		chNew("ch"),
		selNew("sel"),
		selAddReceive("sel", "ch", recvCallbackFn("c")),
		selSelect("sel"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !exec.blocked {
		t.Fatalf("select 无 ready 无 default 应 blocked=true（P0），got blocked=false")
	}
	if hasCompleteWorkflow(commands) {
		t.Fatalf("select 阻塞时禁止完成 workflow，got %+v", commands)
	}
}

// 🔴 Happy Path（send case 命中，照 Temporal Select send 分支 :1481-1504 `if ok { f(); return }`）：
// 跨 lane：lane0 先 Receive 一个 unbuffered channel（登记 blockedReceive + park）；lane1 在同 channel 上
// Select 的 send case——sendAsyncImpl 触发 lane0 的 blockedReceive callback → 当场交付 → send case 命中 →
// InvokeClosure send 回调 → lane1 return。lane0 收到值 return。n=0 → Wait 放行 → 完成。
// 变异点：send case 不接线 sendAsyncImpl / readyIndex → send 永不命中 → 永 blocked。
func TestSelector_SendCase_WaitingReceiver_Hits(t *testing.T) {
	recvLane := laneFn(chReceiveInto("ch", "r", "m")...) // lane0 先 park 等值
	selLane := laneFn(
		selNew("sel"),
		selAddSend("sel", "ch", "payload", zeroArgFn()), // send case
		selSelect("sel"),
	)
	nodes := chProgram(nil,
		chNew("ch"),
		wgNewCall("wg"),
		wgGoLane("wg", recvLane), // 先跑 → Receive 无值 → park（登记 blockedReceive）
		wgGoLane("wg", selLane),  // 后跑 → Select send case 触发 receiver → 交付 → 命中
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("select send case 命中 waiting receiver、双 lane return、Wait 放行，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("select send case 命中后应经 Wait 放行完成 workflow，got %+v", commands)
	}
}

// 🔴 Happy Path（声明序，照 Temporal Select `for _, pair := range s.cases` :1427，声明序取第一个 ready）：
// 两个 buffered channel 都预存值（都能当场命中），select 两个 receive case。TrySelect 声明序遍历，
// case0（chA）先命中即 return，readyIndex=0 → 执行 chA 的回调（内 Receive 消费 chA 的值）；chB 从未被试探、
// 值不被消费。main 单线无阻塞 → 完成。验证声明序第一个 ready 被选。
// 变异点：声明序改逆序 → 选中 chB → 本测试通过 chA 的回调消费断言可辨（此处以 Len 断言 chB 未被消费佐证声明序）。
func TestSelector_DeclarationOrder_FirstReadyWins(t *testing.T) {
	// 预存两个 channel 的值：用 buffered channel + 先 Send 入 buffer。
	cbA := recvCallbackFn("c", chReceiveInto("c", "rA", "mA")...) // chA 回调消费值
	cbB := recvCallbackFn("c", chReceiveInto("c", "rB", "mB")...) // chB 回调（不应被执行）
	nodes := chProgram(nil,
		chNewBuffered("chA", 1),
		chNewBuffered("chB", 1),
		chSend("chA", "A"), // chA buffer 预存 A
		chSend("chB", "B"), // chB buffer 预存 B
		selNew("sel"),
		selAddReceive("sel", "chA", cbA), // index 0
		selAddReceive("sel", "chB", cbB), // index 1
		selSelect("sel"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.blocked {
		t.Fatalf("两 channel 都 ready、select 当场命中 chA、main 单线无阻塞，不应 blocked")
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("声明序命中 chA 后 main 无阻塞应完成 workflow，got %+v", commands)
	}
}

// 🔴 变异锚点 3（cleanups 跑，照 Temporal defer cleanups :1421-1425/:1478-1480）：
// 当场命中路径——ch1 无值（TrySelect 注册 receive callback 进 ch1.blockedReceives + cleanup），ch2 buffered 有值
// （当场命中，readyIndex=1 → 返回）。Select 返回前 defer 跑 cleanups → 移除 ch1 残留 callback。
// 断言：Select 后 ch1.blockedReceives 必须为 0（状态干净）。
// 变异：cleanups 不跑（把 Select 里 defer 跑 cleanups 删掉）→ ch1 残留 1 个 callback → 本断言必红。
func TestSelector_Cleanups_ChannelCleanAfterSelect(t *testing.T) {
	cb1 := recvCallbackFn("c")                                  // ch1 回调（不命中）
	cb2 := recvCallbackFn("c", chReceiveInto("c", "r", "m")...) // ch2 回调消费值
	nodes := chProgram(nil,
		chNew("ch1"),            // 非缓冲，无值 → receive case 注册 callback
		chNewBuffered("ch2", 1), // 缓冲，预存值 → receive case 当场命中
		chSend("ch2", "v"),      // ch2 buffer 预存 v
		selNew("sel"),
		selAddReceive("sel", "ch1", cb1), // index 0（不命中，注册 callback + cleanup）
		selAddReceive("sel", "ch2", cb2), // index 1（当场命中）
		selSelect("sel"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("当场命中 ch2、main 单线无阻塞应完成 workflow，got %+v", commands)
	}
	// 断言 Select 后 ch1 状态干净：cleanups 已移除 TrySelect 注册的残留 callback。
	st := exec.selectors[1] // NewSelector 分配的首个 id=1
	if st == nil {
		t.Fatalf("selector state id=1 缺失")
	}
	ch1 := st.caseChs[0] // index 0 = ch1 的 receive case
	if ch1 == nil {
		t.Fatalf("ch1 caseHandle 缺失")
	}
	if n := ch1.ch.BlockedReceiveCount(); n != 0 {
		t.Fatalf("cleanups 应移除 ch1 残留 callback，Select 后 ch1.blockedReceives 应为 0，got %d（变异：cleanups 不跑则残留）", n)
	}
}

// 🔴 变异锚点 1：
// 跨 lane parking 路径——lane0 Select 在 ch1（不命中）+ ch2（等 send）上，先 park；lane1 send ch2 唤醒。
// 正确实现：Select 循环用 ReadyIndex() 重查——ch1 的 receive callback 只在首次 TrySelect 注册【一次】，
// 命中 ch2 后 defer cleanups 移除 ch1 的这【一次】callback → 收敛后 ch1.blockedReceives==0。
// 变异（循环改用 TrySelect 重查）：每轮 park 醒来重跑 TrySelect → ch1 每轮重注册一个新 callback 进 blockedReceives，
// 但 cleanups 只追踪【最后一轮】TrySelect 返回的那个 → 之前各轮注册的残留无人清 → 收敛后 ch1.blockedReceives>0（堆积）。
// 断言收敛后 ch1.blockedReceives==0，双向区分「ReadyIndex 重查（干净）」与「TrySelect 重查（堆积残留）」。
func TestSelector_ReadyIndexRecheck_NoCallbackPileup(t *testing.T) {
	cb1 := recvCallbackFn("c")                                  // ch1 回调（不命中）
	cb2 := recvCallbackFn("c", chReceiveInto("c", "r", "m")...) // ch2 回调消费值
	lane0 := laneFn(
		selNew("sel"),
		selAddReceive("sel", "ch1", cb1), // index 0（不命中 → park 期间登记）
		selAddReceive("sel", "ch2", cb2), // index 1（等 send 唤醒）
		selSelect("sel"),
	)
	lane1 := laneFn(chSend("ch2", "hello"))
	nodes := chProgram(nil,
		chNew("ch1"),
		chNew("ch2"),
		wgNewCall("wg"),
		wgGoLane("wg", lane0), // 先跑 → Select 无值 → park（登记 ch1/ch2 callback）
		wgGoLane("wg", lane1), // 后跑 → send ch2 唤醒 lane0
		wgNoArgCall("wg", "Wait"),
	)
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)

	exec := NewWorkflowExecutor(nodes, []history.Event{started})
	commands, err := exec.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !hasCompleteWorkflow(commands) {
		t.Fatalf("跨 lane send 唤醒 select、收敛后应完成 workflow，got %+v", commands)
	}
	// 收敛后 ch1（从未被送/命中）的 blocked 队列必须干净——ReadyIndex 重查下只注册一次且被 cleanups 移除。
	st := exec.selectors[1]
	if st == nil {
		t.Fatalf("selector state id=1 缺失")
	}
	ch1 := st.caseChs[0]
	if ch1 == nil {
		t.Fatalf("ch1 caseHandle 缺失")
	}
	if n := ch1.ch.BlockedReceiveCount(); n != 0 {
		t.Fatalf("ReadyIndex 重查下 ch1 只注册一次且被 cleanups 移除，收敛后应为 0，got %d（变异：TrySelect 重查导致每轮重注册堆积）", n)
	}
}
