package interpreter

import (
	"fmt"
	"runtime"
	"strconv"

	coroutine2 "github.com/donutnomad/tenuto/internal/executor/coroutine"
	wlregistry "github.com/donutnomad/wlang/registry"
	wlruntime "github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"

	"github.com/donutnomad/tenuto/sdk/apperror"
)

// selectorState 是一个 Selector 在 executor 侧的可变状态，各切片下标与 case 声明顺序一致。
// sel 只负责选出 ready 的 case 下标；回调由 host 用该下标取出并执行。
type selectorState struct {
	sel       *coroutine2.Selector
	caseCbs   []*wlruntime.Closure // index → 该 case 的 WLang 回调（receive/send/future case 都有）
	caseChs   []*channelHandle     // index → receive case 的 channelHandle（传回调用；其余 case 为 nil）
	caseFuts  []*futureCase        // index → future case 的原 host future 值 + 型名（其余 case 为 nil）
	defaultCb *wlruntime.Closure   // default 回调（无 default 时 nil）
}

// futureCase 是 future case 命中时传给回调的实参：回调收到 future 自身，在回调里调用 Get 取结果。
// coroutine.Future 上 Set 的值不被这里消费，Get 始终从事实表读取。
type futureCase struct {
	typeName string // InvokeClosure 传参型名（"workflow.ActivityFuture"，经 BindType 别名精确匹配）
	value    any    // 原 engine future 值（ActivityFuture），命中时作回调实参
}

// Go 是 workflow.Go(ctx, f) 的 host 方法：把 f 作为新协程交给顶层 Scheduler 后立即返回，
// 不等待 f，也不收集它的错误；需要 join 时由用户代码使用 WaitGroup。
//
// f 必须是内联闭包。协程 body 用 InvokeClosure 执行它；InvokeClosure 只能串行调用，
// 协作式调度保证任一时刻只有一条协程在运行。
// SpawnEager 只在某条协程运行期间调用才会在当轮被调度，workflow.Go 总是在协程 body 里被调用。
func (api *runtimeWorkflowAPI) Go(ctx workflowContext, f any) error {
	cl, ok := f.(*wlruntime.Closure)
	if !ok {
		return fmt.Errorf("workflow.Go f must be an inline closure, got %T", f)
	}
	api.executor.spawnLane(ctx, cl, nil)
	return nil
}

// spawnLane 为 workflow.Go / WaitGroup.Go 起一条 lane 协程：用 spawnSeq 算出 childLane，
// 协程 body 以携带 childLane 的子 ctx 执行闭包。onDone 在 lane body 结束时触发。
//
// 确定性键：childLane = parentLane(ctx) + "/" + spawnSeq，嵌套 Go 得到 /i/j。lane 内的
// activity / timer / child / random 键都从自己的 lanePath 派生，与其他 lane 的推进快慢无关。
//
// 不变式：lane 在等待时 park（冻结栈），只有 body 真正返回才触发 onDone。因此 WaitGroup 的计数
// 始终等于尚未完成的 lane 数。
func (e *WorkflowExecutor) spawnLane(ctx workflowContext, cl *wlruntime.Closure, onDone func()) {
	if e.queryHandler {
		panic(errQueryHandlerBlocked)
	}
	seq := e.spawnSeq // 从 0 起：首条 lane 得 /0，次条得 /1
	e.spawnSeq++
	childLane := ctx.lanePath + "/" + strconv.Itoa(seq) // 嵌套 Go 自动得 /i/j
	laneCtxVal := types.NewValue("workflow.Context", ctx.withLanePath(childLane))
	wlExec := e.activeWLExec // 与 lane 同 executor 的活 WLang executor（executeWLang 注入）
	e.spawnLaneBody(childLane, func() {
		// 非结构化：lane 的业务结果由用户经共享变量落地。闭包本身的错误（运行时错误、非确定性）
		// 让本次 Workflow Task 失败，与 update handler、selector 回调一致。
		if _, err := wlExec.InvokeClosure(cl, []types.Value{laneCtxVal}); err != nil && e.evalErr == nil {
			e.evalErr = err
		}
	}, onDone)
}

// spawnLaneBody 在顶层 Scheduler 起一条 eager 协程执行 body，是 workflow.Go 与 WaitGroup.Go
// 的共同入口。onDone 非 nil 时在 body 结束时经 defer 触发。
// childLane 只用于协程命名；确定性键由调用方通过子 ctx 派生。
func (e *WorkflowExecutor) spawnLaneBody(childLane string, body func(), onDone func()) {
	e.sched.SpawnEager("lane"+childLane, func(yield func()) {
		// 协程级 recover：lane body 或 onDone 里的 Go panic 记为 evalErr，只让本次 Workflow Task
		// 失败。它注册在最外层，所以也覆盖 onDone 自身的 panic。
		defer func() {
			if r := recover(); r != nil {
				e.recordCoroutinePanic(r)
			}
		}()
		if onDone != nil {
			defer onDone()
		}
		body()
	})
}

// Await 是 workflow.Await(ctx, cond) 的 host 方法：cond 为真时返回 nil；cond 为假且 ctx 已取消时
// 返回取消错误（对标 Temporal Await：先求值 cond，再查 ctx.Done）；否则置 blocked 并 park，
// 每次被恢复后重新求值 cond。
// cond 是 0 参数、返回 bool 的闭包，经 InvokeClosure 求值；求值出错记入 evalErr 并结束当前协程。
func (api *runtimeWorkflowAPI) Await(ctx workflowContext, cond *wlruntime.Closure) error {
	e := api.executor
	for {
		out, err := api.wlExec.InvokeClosure(cond, nil)
		if err != nil {
			e.evalErr = err
			runtime.Goexit()
		}
		if b, ok := out.Go().(bool); ok && b {
			e.blocked = false
			e.progressed()
			return nil
		}
		if ctx.canceled() {
			e.blocked = false
			e.progressed()
			return apperror.NewCanceled()
		}
		e.blocked = true
		e.park()
	}
}

// waitGroup 是 workflow.WaitGroup 的 host 类型。值只携带 executor 与 id，计数存在
// executor.waitGroupN[id]，所以值拷贝共享同一个计数。registry 要求方法使用值接收者。
type waitGroup struct {
	executor *WorkflowExecutor
	id       int
}

// NewWaitGroup 分配下一个 id，计数从 0 开始。
func (api *runtimeWorkflowAPI) NewWaitGroup(ctx workflowContext) waitGroup {
	e := api.executor
	if e.waitGroupN == nil {
		e.waitGroupN = make(map[int]int)
	}
	e.waitGroupSeq++
	id := e.waitGroupSeq
	e.waitGroupN[id] = 0
	return waitGroup{executor: e, id: id}
}

// Add 把计数加上 delta；计数为负时 panic。Add 不唤醒等待者，Wait 在下一轮调度重查计数。
func (wg waitGroup) Add(ctx workflowContext, delta any) {
	e := wg.executor
	if e.waitGroupN == nil {
		e.waitGroupN = make(map[int]int)
	}
	e.waitGroupN[wg.id] += int(int64FromAny(delta))
	if e.waitGroupN[wg.id] < 0 {
		panic("negative WaitGroup counter")
	}
}

// Done 等价于 Add(-1)。
func (wg waitGroup) Done(ctx workflowContext) {
	wg.Add(ctx, -1)
}

// Wait 阻塞到计数归零：计数大于 0 时置 blocked 并 park，每次被恢复后重查。
// 其他 lane 在同一轮里结束会让 RunBatch 再转一轮，Wait 因此能在计数归零的那个批里返回。
func (wg waitGroup) Wait(ctx workflowContext) {
	e := wg.executor
	for {
		if e.waitGroupN[wg.id] <= 0 {
			e.blocked = false
			e.progressed()
			return
		}
		e.blocked = true
		e.park()
	}
}

// Go 等价于 Add(1) 后起一条 lane，lane body 结束时自动 Done。
func (wg waitGroup) Go(ctx workflowContext, f any) error {
	cl, ok := f.(*wlruntime.Closure)
	if !ok {
		return fmt.Errorf("workflow.WaitGroup.Go f must be an inline closure, got %T", f)
	}
	wg.Add(ctx, 1)
	wg.executor.spawnLane(ctx, cl, func() { wg.Done(ctx) })
	return nil
}

// channelHandle 是 workflow.Channel 的 host 类型。值携带底层 *coroutine.Channel 指针，
// 值拷贝共享同一个 channel。registry 要求方法使用值接收者。
type channelHandle struct {
	executor *WorkflowExecutor
	ch       *coroutine2.Channel
}

// NewChannel 创建无缓冲 channel。
func (api *runtimeWorkflowAPI) NewChannel(ctx workflowContext) channelHandle {
	return channelHandle{executor: api.executor, ch: coroutine2.NewChannel()}
}

// NewBufferedChannel 创建容量为 size 的缓冲 channel。
func (api *runtimeWorkflowAPI) NewBufferedChannel(ctx workflowContext, size any) channelHandle {
	return channelHandle{executor: api.executor, ch: coroutine2.NewBufferedChannel(int(int64FromAny(size)))}
}

// Send 发送一个值：能立即被接收或放入缓冲时直接返回；否则登记一次 send callback，
// 置 blocked 并 park，每次被恢复后检查值是否已被消费。等待期间 channel 被关闭则 panic。
// 值原样传递，不做序列化。
func (h channelHandle) Send(ctx workflowContext, v any) {
	e := h.executor
	valueConsumed := false
	cb := coroutine2.NewSendCallback(v, func() { valueConsumed = true })
	if h.ch.SendWithCallback(v, cb) {
		e.blocked = false
		e.progressed()
		return
	}
	for {
		if valueConsumed {
			e.blocked = false
			e.progressed()
			return
		}
		// 循环内查 closed：close 可能在 send 阻塞时被调用。
		if h.ch.Closed() {
			panic("Closed channel")
		}
		e.blocked = true
		e.park()
	}
}

// Receive 接收一个值，返回 more（channel 已关闭且取空时为 false）。有值或已关闭时立即返回；
// 否则登记一次 receive callback，置 blocked 并 park，每次被恢复后检查 callback 是否已收到值。
// 不变式：callback 只登记一次，被恢复后只重查结果，不重新登记。
func (h channelHandle) Receive(ctx workflowContext, out *map[string]any) bool {
	e := h.executor
	var result any
	hasResult := false
	more := false
	cb := coroutine2.NewReceiveCallback(func(v any, m bool) {
		result = v
		hasResult = true
		more = m
	})
	for {
		hasResult = false
		v, ok, m := h.ch.ReceiveWithCallback(cb)
		if !ok && !m { // channel closed and empty
			e.blocked = false
			e.progressed()
			return m
		}
		if ok || !m { // 有值 / closed
			if out != nil {
				*out = mapFromAny(h.executor.registry(), v)
			}
			e.blocked = false
			e.progressed()
			return m
		}
		// 没有值：cb 已登记在 channel 上，下面只等它被触发。
		for {
			if hasResult { // sender 已触发 cb
				if out != nil {
					*out = mapFromAny(h.executor.registry(), result)
				}
				e.blocked = false
				e.progressed()
				return more
			}
			e.blocked = true
			e.park()
		}
	}
}

// Close 关闭 channel 并唤醒所有等待中的 receiver（它们收到 more=false）。
// 关闭后再 Send 会 panic。
func (h channelHandle) Close(ctx workflowContext) {
	h.ch.Close()
	// Close 让等待中的 receiver 变为可返回；标记进展使 RunBatch 再转一轮，它们得以在本批重查。
	h.executor.progressed()
}

// selectorHandle 是 workflow.Selector 的 host 类型。值只携带 executor 与 id，case 与回调存在
// executor.selectors[id]：AddReceive / AddSend / AddDefault 是各自独立的语句，每条语句拿到的是
// 值拷贝且返回值被丢弃，状态必须放在 executor 侧才能累积。registry 要求方法使用值接收者。
type selectorHandle struct {
	executor *WorkflowExecutor
	id       int
}

// NewSelector 分配下一个 id 并创建对应的 selectorState。
func (api *runtimeWorkflowAPI) NewSelector(ctx workflowContext) selectorHandle {
	e := api.executor
	if e.selectors == nil {
		e.selectors = make(map[int]*selectorState)
	}
	e.selectorSeq++
	id := e.selectorSeq
	e.selectors[id] = &selectorState{sel: coroutine2.NewSelector("selector-" + strconv.Itoa(id))}
	return selectorHandle{executor: e, id: id}
}

// AddReceive 累积一个 receive case（对标 Temporal AddReceive）：coroutine.Selector.AddReceive(ch.ch)
// 累积到试探算法，host 侧把回调 cl + 该 channelHandle 存进 caseCbs/caseChs（index 对齐 case 声明序）。返回自身支持链式。
// f 经 any 边界到达时是 *wlruntime.Closure（回调闭包，同 workflow.Go lane / Await cond）。
func (h selectorHandle) AddReceive(ctx workflowContext, ch channelHandle, f any) selectorHandle {
	cl, _ := f.(*wlruntime.Closure)
	st := h.executor.selectors[h.id]
	st.sel.AddReceive(ch.ch)
	chCopy := ch
	st.caseCbs = append(st.caseCbs, cl)
	st.caseChs = append(st.caseChs, &chCopy)
	st.caseFuts = append(st.caseFuts, nil)
	return h
}

// AddSend 累积一个 send case（对标 Temporal AddSend）：coroutine.Selector.AddSend(ch.ch, v)，
// host 侧存回调（send case 无 channelHandle 参数，caseChs 补 nil 对齐 index）。返回自身支持链式。
func (h selectorHandle) AddSend(ctx workflowContext, ch channelHandle, v any, f any) selectorHandle {
	cl, _ := f.(*wlruntime.Closure)
	st := h.executor.selectors[h.id]
	st.sel.AddSend(ch.ch, v)
	st.caseCbs = append(st.caseCbs, cl)
	st.caseChs = append(st.caseChs, nil)
	st.caseFuts = append(st.caseFuts, nil)
	return h
}

// AddFuture 累积一个 activity future case（对标 Temporal AddFuture）。转调统一入口
// addFuture（收敛四种 future 的 type switch 于一处，保持 Temporal 单入口结构）。
func (h selectorHandle) AddFuture(ctx workflowContext, fut any, f any) selectorHandle {
	return h.addFuture(fut, f)
}

// AddSignalFuture 累积一个 signal future case。转调统一入口 addFuture。
func (h selectorHandle) AddSignalFuture(ctx workflowContext, fut any, f any) selectorHandle {
	return h.addFuture(fut, f)
}

// AddChildFuture 累积一个 child workflow future case（对应 Temporal AddFuture(childFuture)）。
// 转调统一入口 addFuture。
func (h selectorHandle) AddChildFuture(ctx workflowContext, fut any, f any) selectorHandle {
	return h.addFuture(fut, f)
}

// addFuture 是三方法收敛的统一 future case 入口（对标 Temporal AddFuture）：
// 建 coroutine.Future，查事实表（poll）——ready → 当场 Set（覆盖「加入前已 ready」，含失败：
// 失败也是 ready，照 Set(nil, err)）；未 ready → 登记 pendingFutureBridges，批间 flushFutureBridges 逐批
// poll。三种 future 事实表全部只在 applyBatch 灌入、批内恒定，所以批间 poll 看到的就是该批的视界。
// type switch default 照 Temporal 的 panic 语义（非法 future 类型是编程错误，如 SideEffectFuture
// 同步语义无 select 意义）：记 evalErr 终止本轮。命中时 invokeCase 以各自 typeName 经 BindType 别名传参。
func (h selectorHandle) addFuture(fut any, f any) selectorHandle {
	cl, _ := f.(*wlruntime.Closure)
	e := h.executor
	st := e.selectors[h.id]
	cofut := coroutine2.NewFuture()
	var poll func() (map[string]any, error, bool)
	var typeName string
	switch v := fut.(type) {
	case ActivityFuture:
		poll, typeName = v.poll, "workflow.ActivityFuture"
	case SignalFuture:
		poll, typeName = v.poll, "workflow.SignalFuture"
	case ChildWorkflowFuture:
		poll, typeName = v.poll, "workflow.ChildWorkflowFuture"
	default:
		e.evalErr = fmt.Errorf("selector AddFuture: unsupported future type %T (仅支持 ActivityFuture / SignalFuture / ChildWorkflowFuture)", fut)
		return h
	}
	if out, failure, ready := poll(); ready {
		cofut.Set(out, failure)
	} else {
		e.pendingFutureBridges = append(e.pendingFutureBridges, futureBridge{cofut: cofut, poll: poll})
	}
	st.caseFuts = append(st.caseFuts, &futureCase{typeName: typeName, value: fut})
	st.sel.AddFuture(cofut)
	st.caseCbs = append(st.caseCbs, cl)
	st.caseChs = append(st.caseChs, nil)
	return h
}

// AddDefault 注册 default 分支（对标 Temporal AddDefault）：coroutine.Selector.AddDefault() 标记
// hasDefault，host 侧存 default 回调。返回自身支持链式。
func (h selectorHandle) AddDefault(ctx workflowContext, f any) selectorHandle {
	cl, _ := f.(*wlruntime.Closure)
	st := h.executor.selectors[h.id]
	st.sel.AddDefault()
	st.defaultCb = cl
	return h
}

// Select 检查-yield 循环，照 Temporal selectorImpl.Select：
//
//	readyIndex, cleanups := sel.TrySelect()   // 单次试探 + 注册 callback（只调一次！）
//	defer 跑 cleanups                          // （移除残留 callback，防 closure 泄漏）
//	if readyIndex >= 0 { InvokeClosure(caseCbs[readyIndex]); progressed; return }  // 当场命中
//	if HasDefault() { InvokeClosure(defaultCb); return }
//	for {                                      // 检查-yield 循环
//	    ri := sel.ReadyIndex()                 // 重查 readyIndex（不重 TrySelect）
//	    if ri >= 0 { InvokeClosure(caseCbs[ri]); blocked=false; progressed; return }
//	    blocked=true; e.park()                 // yield
//	}
//
// 为何不每轮重 TrySelect：TrySelect 会重新注册 callback 进 blocked 队列 → 堆积 + 偏离 Temporal
// （Temporal 试探一次注册，yield 循环只重查 readyBranch，不重试探）。
// 首次 TrySelect 未命中时注册的 callback，会在 channel 稍后被兄弟 lane send/close 触发时设 s.readyIndex，
// host park 醒来经 ReadyIndex() 重查命中。
//
// ⚠️ blocked 语义：无 case ready 且无 default → 设 e.blocked=true 并 park（调试回放据此显示阻塞状态）；
// 命中/default 放行清 blocked + progressed。
func (h selectorHandle) Select(ctx workflowContext) {
	e := h.executor
	st := e.selectors[h.id]
	st.sel.Reset()                             // 「每次 Select 的 readyBranch 是新局部变量」——重复 Select 从干净护栏起步
	readyIndex, cleanups := st.sel.TrySelect() // 只调一次（试探 + 注册 callback）
	defer func() {                             // Select 返回前跑 cleanups，移除 blocked 队列里残留的 callback
		for _, c := range cleanups {
			c()
		}
	}()

	if readyIndex >= 0 { // 当场命中（某 case 试探即 ready）
		st.invokeCase(e, readyIndex)
		e.blocked = false
		e.progressed()
		return
	}
	if st.sel.HasDefault() { // 无 case 当场 ready 且有 default → 立即走 default
		if st.defaultCb != nil {
			e.invokeSelectorClosure(st.defaultCb, ctx)
		}
		e.blocked = false
		e.progressed()
		return
	}
	for { // 检查-yield 循环
		ri := st.sel.ReadyIndex() // 重查 readyIndex（不重 TrySelect）
		if ri >= 0 {              // 某 callback 已被触发设 readyIndex → 命中
			st.invokeCase(e, ri)
			e.blocked = false
			e.progressed()
			return
		}
		e.blocked = true // 无 case ready 无 default → blocked + yield
		e.park()
	}
}

// invokeCase 执行第 index 个 case 的 WLang 回调（coroutine 层选 case，host 执行回调）。
// receive 回调 2 参 `func(ch Channel, more bool)`：传该 case 的 channelHandle（型名 workflow.Channel，
// 使 valueAssignableTo 精确匹配 + 回调内 ch.Receive 方法派发经别名命中）+ more bool。回调内可调
// `ch.Receive(ctx, &out)`——命中时已 pre-store recValue（receiveAsyncImpl 优先级最高），故取得到值。
// send/default 回调 0 参。回调 nil（sdkgen 未翻出闭包，理论不至）时静默跳过。
func (st *selectorState) invokeCase(e *WorkflowExecutor, index int) {
	if fc := st.caseFuts[index]; fc != nil { // future case：1 参回调 func(f ActivityFuture)
		// `p.futureFunc = nil`：future 一旦 ready 恒 ready，命中即标记 consumed，
		// 重复 Select 时 TrySelect 跳过本 case（第 k 次 Select 命中第 k 个 ready future）。
		st.sel.MarkConsumed(index)
		// SignalFuture 在被选中时消费：取走该名称下一条未消费的 Signal（见 SignalFuture）。
		if signal, ok := fc.value.(SignalFuture); ok {
			signal.consume()
		}
		cl := st.caseCbs[index]
		if cl == nil {
			return
		}
		futVal := types.NewValue(fc.typeName, fc.value) // 别名型名，见 executeWLang 的 BindType("workflow.ActivityFuture", ...)
		if _, err := e.activeWLExec.InvokeClosure(cl, []types.Value{futVal}); err != nil {
			e.evalErr = err
		}
		return
	}
	cl := st.caseCbs[index]
	if cl == nil {
		return
	}
	if ch := st.caseChs[index]; ch != nil { // receive case：2 参回调 func(ch Channel, more bool)
		more := st.sel.ReadyMore()
		if !more {
			// 照 Temporal（closed channel 命中时 receiveFunc=nil）：closed case 每次试探恒 ready，
			// 不标记 consumed 会在重复 Select 中压制其他 case。略宽于 Temporal：wake-path 命中也置位
			//（Temporal 只在当场命中路径置 nil），规避其对 closed channel 的二次回调怪癖。
			st.sel.MarkConsumed(index)
		}
		chVal := types.NewValue("workflow.Channel", *ch) // 别名型名，见 executeWLang 的 BindType("workflow.Channel", ...)
		moreVal := types.NewValue(types.TBoolean, more)
		if _, err := e.activeWLExec.InvokeClosure(cl, []types.Value{chVal, moreVal}); err != nil {
			e.evalErr = err
		}
		return
	}
	// send / default case：0 参回调
	e.invokeSelectorClosure(cl, workflowContext{})
}

// invokeSelectorClosure 执行 0 参回调（send / default）。回调 nil 时静默跳过。
func (e *WorkflowExecutor) invokeSelectorClosure(cl *wlruntime.Closure, ctx workflowContext) {
	if cl == nil {
		return
	}
	if _, err := e.activeWLExec.InvokeClosure(cl, nil); err != nil {
		e.evalErr = err
	}
}

// mapFromAny 把 channel 交付的值归一为 map[string]any（与 activity/signal 的 out 类型一致，见
// ActivityFuture.Get 的 out *map[string]any）。channel 传的是 WLang 运行时值：已是 map 则原样，
// nil（closed 交付）则 nil，标量则包 {"value": v}（对齐 anyToMap 的标量兜底）。
// typed record 也走 registry：Send(ctx, v any) 接受任意值，用户可以直接塞一个
// 有名 DTO，裸 anyToMap 会让接收方拿到空 map。channel 不跨持久化边界，但
// "收到空 map 且无任何信号" 在内存里同样难查。
func mapFromAny(reg *wlregistry.Registry, v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return mustRecordToMap(reg, v)
}
