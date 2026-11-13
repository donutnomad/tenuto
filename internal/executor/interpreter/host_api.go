package interpreter

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
	wlast "github.com/donutnomad/wlang/ast"
	wlcompiler "github.com/donutnomad/wlang/compiler"
	wlregistry "github.com/donutnomad/wlang/registry"
	wlruntime "github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
	"github.com/donutnomad/wlang/wflang"

	"github.com/donutnomad/tenuto/internal/executor"
	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// parsedPrograms 缓存解析后的 WLang AST，键是 program JSON 的 sha256。
// 解析产出的 AST 在执行期只读，同一个 *ast.Program 可以被并发执行的多个 Run 共享；
// registry 绑定了每个 executor 自己的 host API 实例，仍然逐次构建。
// 条目数到达上限时整体清空，下一次使用重新解析。
var parsedPrograms = struct {
	sync.Mutex
	byDigest map[[sha256.Size]byte]*wlast.Program
}{byDigest: make(map[[sha256.Size]byte]*wlast.Program)}

const maxParsedPrograms = 256

func parsedProgram(source programpkg.WLangProgram) (*wlast.Program, error) {
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	parsedPrograms.Lock()
	program, ok := parsedPrograms.byDigest[digest]
	parsedPrograms.Unlock()
	if ok {
		return program, nil
	}
	program, err = wlcompiler.ParseProgram(raw)
	if err != nil {
		return nil, err
	}
	parsedPrograms.Lock()
	if len(parsedPrograms.byDigest) >= maxParsedPrograms {
		clear(parsedPrograms.byDigest)
	}
	parsedPrograms.byDigest[digest] = program
	parsedPrograms.Unlock()
	return program, nil
}

// hostTypeBindings 是 WLang registry 里 host 类型的绑定表，按顺序执行。
// alias 为空：AutoBindType，方法登记在 Go 类型的 auto host 型名下。
// alias 非空：BindType，把同一 Go 类型再登记到 go2wlang 产物使用的 SDK 型名下——
// 回调形参（如 `func(f workflow.ActivityFuture)`、`func(ch workflow.Channel, more bool)`）与
// 嵌套 struct 字段（ActivityOptions.RetryPolicy）按该型名做名义匹配和方法派发。
var hostTypeBindings = []struct {
	alias string
	value any
}{
	{value: workflow.Info{}},
	{alias: "workflow.Context", value: workflowContext{}},
	{alias: "workflow.Info", value: workflow.Info{}},
	{value: ActivityFuture{}},
	{value: SignalFuture{}},
	{value: SideEffectFuture{}},
	{value: ChildWorkflowFuture{}},
	{value: waitGroup{}},
	{value: channelHandle{}},
	{alias: "workflow.Channel", value: channelHandle{}},
	{alias: "workflow.ActivityFuture", value: ActivityFuture{}},
	{alias: "workflow.SignalFuture", value: SignalFuture{}},
	{alias: "workflow.ChildWorkflowFuture", value: ChildWorkflowFuture{}},
	{value: selectorHandle{}},
	{value: workflow.ActivityOptions{}},
	{alias: "workflow.ActivityOptions", value: workflow.ActivityOptions{}},
	{value: workflow.ChildWorkflowOptions{}},
	{alias: "workflow.ChildWorkflowOptions", value: workflow.ChildWorkflowOptions{}},
	{value: workflow.RetryPolicy{}},
	{alias: "workflow.RetryPolicy", value: workflow.RetryPolicy{}},
	{alias: "time.Time", value: workflowTime{}},
}

// hostSymbols 是 workflow 源码可直接引用的 SDK 常量。
var hostSymbols = map[string]any{
	"workflow.ParentClosePolicyTerminate":     workflow.ParentClosePolicyTerminate,
	"workflow.ParentClosePolicyRequestCancel": workflow.ParentClosePolicyRequestCancel,
	"workflow.ParentClosePolicyAbandon":       workflow.ParentClosePolicyAbandon,
	"workflow.TimeoutTypeScheduleToStart":     string(workflow.TimeoutTypeScheduleToStart),
	"workflow.TimeoutTypeStartToClose":        string(workflow.TimeoutTypeStartToClose),
	"workflow.TimeoutTypeScheduleToClose":     string(workflow.TimeoutTypeScheduleToClose),
	"workflow.TimeoutTypeHeartbeat":           string(workflow.TimeoutTypeHeartbeat),
	"workflow.ErrCanceled":                    types.NewValue(types.TError, workflow.ErrCanceled),
}

// executeWLang 在 main 协程上执行 Workflow 的 WLang 程序，记录 body 的返回值。
func (e *WorkflowExecutor) executeWLang(wf *programpkg.WorkflowConfig) error {
	if err := wf.Program.ValidateArtifact(); err != nil {
		return err
	}
	program, err := parsedProgram(wf.Program)
	if err != nil {
		return err
	}
	reg := wflang.DefaultRegistry()
	if err := reg.BindDescriptors(program.Types); err != nil {
		return err
	}
	api := &runtimeWorkflowAPI{executor: e, registry: reg}
	if err := reg.BindGoPackageAuto("workflow", api); err != nil {
		return err
	}
	for _, binding := range hostTypeBindings {
		var err error
		if binding.alias == "" {
			err = reg.AutoBindType(binding.value)
		} else {
			err = reg.BindType(binding.alias, reflect.TypeOf(binding.value), wlregistry.BindOptions{})
		}
		if err != nil {
			return err
		}
	}
	for symbol, value := range hostSymbols {
		if err := reg.BindSymbol(symbol, value); err != nil {
			return err
		}
	}
	for symbol, activityRef := range wf.SymbolRefs() {
		if err := reg.BindSymbol(symbol, activityRef.Type); err != nil {
			return err
		}
		api.registerActivityRoute(activityRef)
	}
	scope := wlruntime.NewScope()
	scope.LetReadOnly("ctx", types.NewValue("workflow.Context", workflowContext{scope: e.rootScope}))
	untypedInput := types.NewValue("map<string,any>", e.input)
	scope.LetReadOnly("input", untypedInput)
	// 入口参数按 Entry.InputVar（Go 形参名，如 `in`、`req`）绑定：翻译产物的 body 直接按形参名引用入口参数
	// （产物形如 {"var":"in.Amount"}），换个名字会让 body 找不到符号。有 descriptor 的入参按类型解码，
	// map[string]any 入参原样绑定；上面的 `input` 与之并存。
	entry := wf.Program.Entry
	if entry.InputVar != "" && entry.Input == "" {
		scope.LetReadOnly(entry.InputVar, untypedInput)
	}
	if entry.Input != "" && entry.InputVar != "" {
		rawInput, err := json.Marshal(e.input)
		if err != nil {
			return fmt.Errorf("encode workflow entry input: %w", err)
		}
		typedInput, err := reg.DecodeJSONValue(entry.Input, rawInput)
		if err != nil {
			return fmt.Errorf("decode workflow entry input %q: %w", entry.Input, err)
		}
		scope.LetReadOnly(entry.InputVar, typedInput)
	}
	wlExec := wlruntime.NewExecutor(e.ctx, scope, reg, reg.PackageNames(), workflowWLangBudget())
	if e.stepHook != nil {
		wlExec.OnStep = func(info wlruntime.StepInfo) error {
			if info.RoutineID == 0 && e.sched != nil {
				info.RoutineID = e.sched.CurrentRoutineID()
			}
			return e.stepHook(info)
		}
	}
	api.wlExec = wlExec     // handler/回调闭包经同一 executor 执行（闭包捕获的外层变量随之存活）
	e.activeWLExec = wlExec // 非结构化 lane 起协程（spawnLane / waitGroup.Go）用它 InvokeClosure 执行 lane 闭包
	e.activeAPI = api       // update handler 协程（批间 spawn）经它读 updateHandlers 注册表
	result, err := wlExec.RunProgram(program)
	if err != nil {
		return err
	}
	// 工作流 body 的 return 值：非 nil error → 记为工作流失败信号（收敛时产 CmdFailWorkflow，见 Execute）；
	// map 数据 → 记为完成输出（收敛时塞进 CmdCompleteWorkflow.Output）。nil error 两个分支都不命中，走正常完成。
	// Output 通道全引擎统一为 map[string]any（同 activity Output），标量返回值不支持。
	//
	// Go 侧 `(T, error)` 双返回值翻到 WLang 是一个 **tuple**，result.Go() 的动态类型是
	// []any ——三个 case 全部落空，输出会被静默丢弃。所以必须先解 tuple 再分派。
	// 实测：tuple TypeName="tuple<orders.PaymentOutput,null>" Go=[]interface{}。
	// update handler 的返回值同样先解 tuple（见 spawnUpdateHandlerCoroutine）。
	if vals, ok := types.UnpackTuple(result); ok && len(vals) == 2 {
		if errVal := vals[1]; errVal.TypeName() != types.TNull {
			if retErr, ok := errVal.Go().(error); ok && retErr != nil {
				e.setWorkflowReturnError(api.registryEncoder(), retErr)
				return nil
			}
		}
		if err := checkEntryOutputType(reg, wf.Program.Entry.Output, vals[0]); err != nil {
			return err
		}
		out, err := RecordToMap(reg, vals[0].Go())
		if err != nil {
			return err
		}
		e.workflowReturnOutput = out
		return nil
	}
	// 单返回值形态（error-only，或只返回数据不返回 error）：result 不是 tuple。
	switch ret := result.Go().(type) {
	case error:
		if ret != nil {
			e.setWorkflowReturnError(api.registryEncoder(), ret)
		}
	case map[string]any, *types.Record:
		out, err := RecordToMap(reg, ret)
		if err != nil {
			return err
		}
		e.workflowReturnOutput = out
	}
	return nil
}

type runtimeWorkflowAPI struct {
	executor       *WorkflowExecutor
	wlExec         *wlruntime.Executor // 活的 WLang executor（executeWLang 注入，供 handler 协程用 InvokeClosure 回调闭包）
	registry       *wlregistry.Registry
	updateHandlers map[string]registeredUpdateHandler
	// queryHandlers：name → query handler 闭包，由 SetQueryHandler host 方法登记。
	// 与 updateHandlers 隔离——query handler 不 spawn 协程、不进 spawnUpdateHandlers /
	// matchReplayWithHistory（query 只读、不产命令、不参与历史比对）。QueryWorkflow 在 ReplayForQuery
	// 收敛后按 name 查它、InvokeClosure 求值一次。
	queryHandlers map[string]*wlruntime.Closure
	activityQueue map[string]string
	signalReads   map[string]int

	// laneCounters 是 per-lane 确定键的计数器：activity/timer/child/random 的 ID = lanePath + lane 内
	// 局部序号。lane 身份在声明期固定、lane 内程序顺序固定，所以键在任意次 replay 中相同，与兄弟 lane
	// 推进快慢无关（对标 Temporal 协程作用域定序）。lanePath 由 workflowContext 逐协程携带（laneKey
	// 从 ctx 取），随协程栈冻结/恢复。计数器按 lanePath\x00kind 分桶，在 executor 生命周期内只增不重置：
	// 会话模式下它跨 Activation 累积，值与从头 replay 到同一位置时相同。
	laneCounters map[string]int
}

func (api *runtimeWorkflowAPI) GetInfo(workflowContext) workflow.Info {
	info := api.executor.activationInfo
	info.ContinueAsNewReasons = append([]workflow.ContinueAsNewReason(nil), info.ContinueAsNewReasons...)
	return info
}

// laneKey 生成 per-lane 确定键：kind 是原语类别前缀（a/t/c/ru/ri），返回 lanePath#<kind><局部序>。
// lanePath 从 ctx 取（对标 Temporal 从自身协程 ctx 取作用域）。
// 例：root 下第 1 个 activity → "#a1"；lane "/0" 内第 2 个 timer → "/0#t2"。
// ActivityOptions.ActivityID 为空的 Activity 以它作 ActivityID。
// 每类原语独立命名空间（同 lanePath 下按 kind 分桶），与旧 seq/timerSeq/childSeq 分开编号语义一致：
// 条件分支下不同类原语交错也不互相挤动（A 恒为该 lane 第 1 个 activity，与前面有无 timer 无关）。
func (api *runtimeWorkflowAPI) laneKey(ctx workflowContext, kind string) string {
	if api.laneCounters == nil {
		api.laneCounters = make(map[string]int)
	}
	lanePath := ctx.lanePath
	bucket := lanePath + "\x00" + kind // map 内部分桶键（\x00 不出现在返回的键里）
	api.laneCounters[bucket]++
	return fmt.Sprintf("%s#%s%d", lanePath, kind, api.laneCounters[bucket])
}

func (api *runtimeWorkflowAPI) registerActivityRoute(ref programpkg.ActivityRef) {
	if api.activityQueue == nil {
		api.activityQueue = make(map[string]string)
	}
	api.activityQueue[ref.Type] = ref.TaskQueue
}

// ExecuteActivity 立即调度：构造 future 后立即产 CmdScheduleActivity（发出即调度），不 Goexit、
// 继续执行下一行，让多个 Execute 累积多条调度命令 → 并发发起。Get 只等结果、不再产命令。
//
// 返回值带 SDK 型名 workflow.ActivityFuture：WLang 的类型匹配是名义的，`[]workflow.ActivityFuture`
// 的元素、`var f workflow.ActivityFuture` 的赋值都按这个名字检查。
func (api *runtimeWorkflowAPI) ExecuteActivity(ctx workflowContext, activityType string, input any, args ...any) types.Value {
	return types.NewValue("workflow.ActivityFuture", api.executeActivity(ctx, activityType, input, args...))
}

// activityCall 是一次 ExecuteActivity 调用，Future 经它找到自己的调度事件。重放时调用认领历史里第一个
// 尚未被认领、ActivityID 相同的 ActivityScheduled；live 时调用发出调度命令，调度事件到达时绑定到等待它的
// 调用。Server 拒绝与未关闭的 Activity 重复的 ActivityID，同一 ID 只在前一个关闭后再次调度，所以按历史
// 顺序认领与调用顺序一致。
type activityCall struct {
	id string
	// scheduledEventID 是绑定的 ActivityScheduled 事件 ID；0 表示调度事件还没有到达。
	scheduledEventID int64
	// canceled 表示 Future 已按取消结束：调度命令在提交前撤回，或取消请求已发出且不等待结果。
	canceled bool
	// cancelRequested 表示已发出 RequestCancelActivity。
	cancelRequested bool
	debug           *executor.DebugActivity
}

// claimActivityScheduled 认领历史里第一个尚未被调用认领、ActivityID 为 id 的调度事件。
func (e *WorkflowExecutor) claimActivityScheduled(id string) (int64, bool) {
	queue := e.unclaimedActivities[id]
	if len(queue) == 0 {
		return 0, false
	}
	if len(queue) == 1 {
		delete(e.unclaimedActivities, id)
	} else {
		e.unclaimedActivities[id] = queue[1:]
	}
	return queue[0], true
}

// bindActivityScheduled 登记灌入的 ActivityScheduled：有调用在等这个 ActivityID 的调度事件时绑定给它，
// 否则留给之后的调用认领。
func (e *WorkflowExecutor) bindActivityScheduled(id string, scheduledEventID int64) {
	if call := e.awaitingActivities[id]; call != nil {
		delete(e.awaitingActivities, id)
		call.scheduledEventID = scheduledEventID
		return
	}
	e.unclaimedActivities[id] = append(e.unclaimedActivities[id], scheduledEventID)
}

// activityClosed 报告调用的 Activity 在历史里是否已有终态（完成、失败、超时或取消）。
func (e *WorkflowExecutor) activityClosed(call *activityCall) bool {
	if call.scheduledEventID == 0 {
		return false
	}
	_, completed := e.completed[call.scheduledEventID]
	_, failed := e.failed[call.scheduledEventID]
	return completed || failed
}

func (api *runtimeWorkflowAPI) executeActivity(ctx workflowContext, activityType string, input any, args ...any) ActivityFuture {
	e := api.executor
	opts := activityCallOptions(callActivityOptions{taskQueue: api.activityQueue[activityType]}, args)
	// lane 计数器每次调用都前进：给某个调用指定 ActivityID 不改变其他调用的缺省 ID。
	call := &activityCall{id: cmp.Or(opts.activityID, api.laneKey(ctx, "a"))}
	defer api.registerActivityCancel(ctx, call, opts.waitForCancellation)
	taskQueue := opts.taskQueue
	future := ActivityFuture{
		executor:     e,
		call:         call,
		activityType: activityType,
		taskQueue:    taskQueue,
		input:        mustRecordToMap(api.registry, input),
	}
	if e.debugActivities != nil {
		call.debug = &executor.DebugActivity{
			ActivityID: call.id,
			Type:       activityType,
			Status:     "scheduled",
			Input:      future.input,
		}
		e.debugActivities = append(e.debugActivities, call.debug)
	}
	// 历史里已有这次调用的调度事件：不重复产命令，replay 批补产 shadow 与历史命令事件配对。
	if scheduledEventID, ok := e.claimActivityScheduled(call.id); ok {
		call.scheduledEventID = scheduledEventID
		e.emitDebugEffect(future.debugEffect("scheduled"))
		e.emitShadow(history.Command{Type: history.CmdScheduleActivity, ActivityID: call.id, ActivityType: activityType})
		return future
	}
	e.emitDebugEffect(future.debugEffect("scheduled"))
	// 历史无痕 → 立即产调度命令（发出即调度），不 Goexit、继续执行。per-call 超时/RetryPolicy 填进 Command。
	e.awaitingActivities[call.id] = call
	e.emitCommand(history.Command{
		Type:                   history.CmdScheduleActivity,
		ActivityID:             call.id,
		ActivityType:           activityType,
		TaskQueue:              taskQueue,
		Input:                  future.input,
		ScheduleToStartTimeout: opts.schedToStart,
		StartToCloseTimeout:    opts.startToClose,
		ScheduleToCloseTimeout: opts.schedToClose,
		HeartbeatTimeout:       opts.heartbeat,
		RetryPolicy:            opts.retryPolicy,
	})
	return future
}

// registerActivityCancel 在 ctx 的作用域上登记 Activity 的取消（对标 Temporal ExecuteActivity 的
// cancellationCallback 与 RequestCancelActivity）：调度命令还在本批、尚未提交时撤回命令，Future 以取消
// 错误返回；已提交时发 RequestCancelActivity，wait 为 false 时 Future 随即以取消错误返回，为 true 时等
// Activity 的结果（ActivityCancelled、完成或失败）。ctx 已取消时在调用点立即执行。
func (api *runtimeWorkflowAPI) registerActivityCancel(ctx workflowContext, call *activityCall, wait bool) {
	e := api.executor
	ctx.onCancel(cancelOp{
		finished: func() bool {
			return call.canceled || call.cancelRequested || e.activityClosed(call)
		},
		cancel: func() {
			if e.withdrawCommand(func(command history.Command) bool {
				return command.Type == history.CmdScheduleActivity && command.ActivityID == call.id
			}) {
				call.canceled = true
				if e.awaitingActivities[call.id] == call {
					delete(e.awaitingActivities, call.id)
				}
				return
			}
			e.emitCommand(history.Command{Type: history.CmdRequestCancelActivity, ActivityID: call.id})
			call.cancelRequested = true
			if !wait {
				call.canceled = true
			}
		},
	})
}

func (api *runtimeWorkflowAPI) WaitSignal(ctx workflowContext, name string) types.Value {
	if api.signalReads == nil {
		api.signalReads = make(map[string]int)
	}
	index := api.signalReads[name]
	api.signalReads[name] = index + 1
	return types.NewValue("workflow.SignalFuture", SignalFuture{
		executor: api.executor,
		name:     name,
		index:    index,
	})
}

// registry 返回当前 program 的 registry，活 executor 尚未注入时为 nil
// （activeAPI 在 main 首跑前可为 nil）。nil 时下游按非 typed 值处理。
func (e *WorkflowExecutor) registry() *wlregistry.Registry {
	if e == nil || e.activeAPI == nil {
		return nil
	}
	return e.activeAPI.registry
}

// Sleep 检查-yield 循环（照 Temporal 挂起原语）：
// 定时器已 fire → progressed()+return（越过 Sleep 继续）；否则确保只调度一次（首见产 CmdScheduleTimer），
// 未 fire → blocked+yield，被下轮 resume 后回循环顶重查 timersFired。
// park 会返回，所以用「产命令一次」的守卫 + 循环重查，避免 resume 后重复产 CmdScheduleTimer。
func (api *runtimeWorkflowAPI) Sleep(ctx workflowContext, delay any) error {
	d, err := durationFromAny(delay)
	if err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	e := api.executor
	timerID := api.laneKey(ctx, "t")
	// ctx 已取消时不调度 timer（对标 Temporal：创建即被取消的 timer 命令不发出）。
	if ctx.canceled() {
		return workflow.ErrCanceled
	}
	scheduled := false // 本次 Sleep 调用内是否已产过调度命令（防 resume 后重复产）
	shadowed := false  // 本次 Sleep 调用内是否已补过 shadow（fired/scheduled 两命中分支共用防重）
	registered := false
	for {
		if e.timerCanceled[timerID] {
			e.progressed()
			return workflow.ErrCanceled
		}
		if e.timersFired[timerID] {
			// fired 命中即历史有 TimerScheduled 事件（fire 时 timersScheduled 表项已被
			// applyBatch 删除，scheduled 分支到不了）→ replay 批补 shadow 配对。
			if !shadowed && !scheduled {
				e.emitShadow(history.Command{Type: history.CmdScheduleTimer, TimerID: timerID})
				shadowed = true
			}
			e.progressed()
			return nil
		}
		if _, ok := e.timersScheduled[timerID]; ok {
			// 历史已调度未 fire（跨批等待）→ replay 批补 shadow 配对（本调用一次）。
			if !shadowed && !scheduled {
				e.emitShadow(history.Command{Type: history.CmdScheduleTimer, TimerID: timerID})
				shadowed = true
			}
		} else if !scheduled {
			// 首见（历史未调度、本次未产）→ 产一次 CmdScheduleTimer（发出即调度）。
			e.emitCommand(history.Command{
				Type:    history.CmdScheduleTimer,
				TimerID: timerID,
				Delay:   d,
			})
			scheduled = true
		}
		if !registered {
			registered = true
			api.registerTimerCancel(ctx, timerID)
		}
		e.blocked = true
		e.park()
	}
}

// registerTimerCancel 在 ctx 的作用域上登记 timer 的取消（对标 Temporal NewTimer 的 cancellationCallback
// 与 RequestCancelTimer）：调度命令还在本批时撤回，已提交时发 CancelTimer；两种情况 Sleep 都立即返回取消错误。
func (api *runtimeWorkflowAPI) registerTimerCancel(ctx workflowContext, timerID string) {
	e := api.executor
	ctx.onCancel(cancelOp{
		finished: func() bool { return e.timersFired[timerID] || e.timerCanceled[timerID] },
		cancel: func() {
			if !e.withdrawCommand(func(command history.Command) bool {
				return command.Type == history.CmdScheduleTimer && command.TimerID == timerID
			}) {
				e.emitCommand(history.Command{Type: history.CmdCancelTimer, TimerID: timerID})
			}
			e.timerCanceled[timerID] = true
		},
	})
}

// Now 返回 event history 里的确定性时间，值的型名是 time.Time，方法见 workflowTime。
func (api *runtimeWorkflowAPI) Now(ctx workflowContext) types.Value {
	return newWorkflowTime(api.executor.currentTime)
}

// workflowTime 是 workflow 代码里 time.Time 值的运行时表示。
//
// WLang 的值没有 Go 的方法集，host 类型的方法要逐个绑定；time.Time 自己的方法返回
// time.Duration、time.Time 这些 WLang 不认识的 Go 类型，所以这里包一层，只暴露确定性的
// 运算，返回值都是 WLang 的内建类型（Duration 是 int64 纳秒）或 time.Time。
// Format 按 UTC 输出：结果与 worker 所在时区无关。
type workflowTime struct {
	t time.Time
}

func newWorkflowTime(t time.Time) types.Value {
	return types.NewValue("time.Time", workflowTime{t: t})
}

// Add 的参数是纳秒数（time.Duration 在 WLang 里是 int64）。参数不是时长时 panic，
// 由 Execute 的 recover 转成本轮 Workflow Task 失败。
func (w workflowTime) Add(delta any) types.Value {
	duration, err := durationFromAny(delta)
	if err != nil {
		panic(fmt.Errorf("time.Time.Add: %w", err))
	}
	return newWorkflowTime(w.t.Add(duration))
}

func (w workflowTime) Sub(other workflowTime) int64 { return int64(w.t.Sub(other.t)) }

func (w workflowTime) Before(other workflowTime) bool { return w.t.Before(other.t) }

func (w workflowTime) After(other workflowTime) bool { return w.t.After(other.t) }

func (w workflowTime) Equal(other workflowTime) bool { return w.t.Equal(other.t) }

func (w workflowTime) Unix() int64 { return w.t.Unix() }

func (w workflowTime) UnixMilli() int64 { return w.t.UnixMilli() }

func (w workflowTime) Format(layout string) string { return w.t.UTC().Format(layout) }

// MarshalJSON 让时间值放进 Activity 输入、workflow 输出等载荷时编码成 RFC 3339 字符串。
func (w workflowTime) MarshalJSON() ([]byte, error) { return w.t.MarshalJSON() }

func (w workflowTime) String() string { return w.t.UTC().Format(time.RFC3339Nano) }

// ExecuteChildWorkflow 的 args 是可选的 workflow.ChildWorkflowOptions；父关闭策略原样放进命令，由服务端校验。
func (api *runtimeWorkflowAPI) ExecuteChildWorkflow(ctx workflowContext, workflowID string, input any, args ...any) types.Value {
	childID := api.laneKey(ctx, "c")
	e := api.executor
	e.emitDebugEffect(executor.DebugEffect{
		Kind:            "child-workflow",
		Phase:           "scheduled",
		ChildID:         childID,
		ChildWorkflowID: workflowID,
	})
	// 发起时机上移：对齐 Temporal ExecuteChildWorkflow 调用即在
	// command 状态机产 StartChildWorkflow 命令，也对齐我们 ExecuteActivity 即调度的先例。否则
	// AddChildFuture(fut) 不 Get → 命令永不发出 → future 永不 ready。ExecuteChildWorkflow 是
	// body 内单次执行的语句（childID 按 laneKey 确定性），故发起恰好一次。
	// 三表任一命中（completed/failed/started）→ 历史已有 ChildWorkflowStarted 事件 → replay 批补
	// shadow 配对；全 miss → 首见发起 CmdStartChildWorkflow（live 批
	// 进 commands，replay 批进 replayCommands 与历史待配对）。
	shadow := history.Command{Type: history.CmdStartChildWorkflow, WorkflowID: workflowID, ChildID: childID}
	_, completed := e.childCompleted[childID]
	_, failed := e.childFailures[childID]
	_, started := e.childStarted[childID]
	wait := false
	for _, arg := range args {
		if opts, ok := arg.(workflow.ChildWorkflowOptions); ok {
			wait = wait || opts.WaitForCancellation
		}
	}
	if completed || failed || started {
		e.emitShadow(shadow)
	} else {
		command := history.Command{
			Type:       history.CmdStartChildWorkflow,
			WorkflowID: workflowID,
			ChildID:    childID,
			Input:      mustRecordToMap(api.registry, input),
		}
		for _, arg := range args {
			if opts, ok := arg.(workflow.ChildWorkflowOptions); ok && opts.ParentClosePolicy != "" {
				command.ParentClosePolicy = opts.ParentClosePolicy
			}
		}
		e.emitCommand(command)
	}
	api.registerChildCancel(ctx, childID, wait)
	return types.NewValue("workflow.ChildWorkflowFuture", ChildWorkflowFuture{
		executor:   e,
		childID:    childID,
		workflowID: workflowID,
		input:      mustRecordToMap(api.registry, input),
	})
}

// registerChildCancel 在 ctx 的作用域上登记子 Workflow 的取消（对标 Temporal ExecuteChildWorkflow 的
// cancellationCallback 与 RequestCancelChildWorkflow）：子 Workflow 已发起即发 RequestCancelChildWorkflow，
// 启动命令与取消在同一批时两条命令依次提交。wait 为 false 时 Future 随即以取消错误返回，为 true 时等子
// Workflow 结束。
func (api *runtimeWorkflowAPI) registerChildCancel(ctx workflowContext, childID string, wait bool) {
	e := api.executor
	ctx.onCancel(cancelOp{
		finished: func() bool {
			_, completed := e.childCompleted[childID]
			_, failed := e.childFailures[childID]
			return completed || failed || e.childCanceled[childID] || e.childCancelRequested[childID]
		},
		cancel: func() {
			e.emitCommand(history.Command{Type: history.CmdRequestCancelChildWorkflow, ChildID: childID})
			e.childCancelRequested[childID] = true
			if !wait {
				e.childCanceled[childID] = true
			}
		},
	})
}

func (api *runtimeWorkflowAPI) ContinueAsNew(ctx workflowContext, workflowID string, input any) error {
	// replay 批产生的 CAN 进 replayCommands → canInReplay() 豁免比对（CAN 是唯一真实可达的
	// 「replay 中跑到终态」形态）。
	api.executor.emitCommand(history.Command{
		Type:       history.CmdContinueAsNew,
		WorkflowID: workflowID,
		Input:      mustRecordToMap(api.registry, input),
	})
	runtime.Goexit()
	return nil
}

// callActivityOptions 是一次 ExecuteActivity 调用的路由、超时与重试策略。
type callActivityOptions struct {
	activityID, taskQueue                               string
	schedToStart, startToClose, schedToClose, heartbeat time.Duration
	retryPolicy                                         *protocol.ParsedRetryPolicy
	waitForCancellation                                 bool
}

// activityCallOptions 把调用点的 workflow.ActivityOptions 叠加到符号的缺省路由上。
// 重试策略原样带给 Server：未设置的字段由 Server 按缺省策略补齐。
func activityCallOptions(base callActivityOptions, args []any) callActivityOptions {
	for _, arg := range args {
		v, ok := arg.(workflow.ActivityOptions)
		if !ok {
			continue
		}
		base.activityID = cmp.Or(v.ActivityID, base.activityID)
		if v.TaskQueue != "" {
			base.taskQueue = v.TaskQueue
		}
		base.schedToStart = cmp.Or(v.ScheduleToStartTimeout, base.schedToStart)
		base.startToClose = cmp.Or(v.StartToCloseTimeout, base.startToClose)
		base.schedToClose = cmp.Or(v.ScheduleToCloseTimeout, base.schedToClose)
		base.heartbeat = cmp.Or(v.HeartbeatTimeout, base.heartbeat)
		if rp := parsedRetryPolicy(v.RetryPolicy); rp != nil {
			base.retryPolicy = rp
		}
		base.waitForCancellation = base.waitForCancellation || v.WaitForCancellation
	}
	return base
}

// parsedRetryPolicy 把调用点的 workflow.RetryPolicy 转成调度命令携带的策略；零值策略返回 nil。
func parsedRetryPolicy(rp workflow.RetryPolicy) *protocol.ParsedRetryPolicy {
	if rp.MaxAttempts == 0 && rp.InitialInterval == 0 && rp.BackoffCoefficient == 0 &&
		rp.MaxInterval == 0 && len(rp.NonRetryableErrors) == 0 && len(rp.NonRetryableErrorTypes) == 0 {
		return nil
	}
	return &protocol.ParsedRetryPolicy{
		MaxAttempts:            rp.MaxAttempts,
		InitialInterval:        rp.InitialInterval,
		BackoffCoefficient:     rp.BackoffCoefficient,
		MaxInterval:            rp.MaxInterval,
		NonRetryableErrors:     rp.NonRetryableErrors,
		NonRetryableErrorTypes: rp.NonRetryableErrorTypes,
	}
}

// checkEntryOutputType 校验 body 实际返回的类型与 artifact 声明的 Entry.Output 一致。
//
// 挡的是「artifact 声明返回 A、body 实际返回 B」——这类不一致会让下游按 A 解码一份
// B 的数据，错在消费端而非产生端，很难追。
//
// 不能直接字符串比较：Entry.Output 存的是 descriptor ID（Types 的 key，指针类型带
// "*" 前缀），而 value.TypeName() 返回的是 LanguageName，两者不相等。
// DescriptorID 对两种拼写都返回 canonical ID，用它归一后再比。
//
// declared 为空表示这个 workflow 没有 typed 出参（error-only、标量返回），不校验。
func checkEntryOutputType(reg *wlregistry.Registry, declared string, actual types.Value) error {
	if declared == "" || reg == nil {
		return nil
	}
	// null 出参（如 tuple 里业务位为空）无类型可比。
	if actual.TypeName() == types.TNull {
		return nil
	}
	actualID, ok := reg.DescriptorID(actual.TypeName())
	if !ok {
		return fmt.Errorf("workflow entry output %q has no type descriptor", actual.TypeName())
	}
	declaredID, ok := reg.DescriptorID(declared)
	if !ok {
		return fmt.Errorf("workflow entry output descriptor %q is not bound", declared)
	}
	if actualID != declaredID {
		return fmt.Errorf("workflow entry output type mismatch: artifact declares %q, body returned %q",
			declaredID, actualID)
	}
	return nil
}

// workflowMaxStringBytes 是 Workflow Program 里单个字符串的上限（字节）。
const workflowMaxStringBytes = 16 << 20

// workflowWLangBudget 是一次执行（从 History 开头重放到当前）的 WLang 预算。超出时解释器返回 E_BUDGET，
// Workflow Task 以这个错误让 Run 失败，副本照常运行。
//
// 大小上限在分配之前检查：字符串拼接、bigInt 运算，以及 strings.Repeat、Replace、Join、Split、fmt.Sprintf
// 等结果可能远大于参数的标准库函数，由参数算出结果大小；其余宿主函数（含 workflow 包）在返回后检查结果。
// 上限针对单个值；值的个数由步数、数组长度与键数的上限约束。
func workflowWLangBudget() wlruntime.Budget {
	return wlruntime.Budget{
		MaxSteps:          1_000_000,
		MaxLoopIterations: 1_000_000,
		MaxRoutines:       1024,
		MaxCallDepth:      128,
		MaxArrayLength:    100_000,
		MaxObjectKeys:     100_000,
		MaxStringBytes:    workflowMaxStringBytes,
	}
}

// setWorkflowReturnError 记录 body 返回的错误：字符串镜像用于既有路径与比对，结构化 Failure
// 用于终态事件落库。Encode 失败时退化为 message-only，不掩盖原始失败。
//
// 返回的错误是取消错误（IsCanceledError 为 true）时 Workflow 以 Cancelled 关闭，与有没有收到取消请求
// 无关（对标 Temporal：SDK 对 CanceledError 产 CancelWorkflowExecution，Server 不检查取消请求）；
// 结构化载荷取链上的取消错误，WorkflowCancelled 带它的 details。
func (e *WorkflowExecutor) setWorkflowReturnError(encode apperror.Encoder, err error) {
	e.workflowReturnErr = err.Error()
	e.workflowReturnFailed = true
	if canceled := apperror.CanceledIn(err); canceled != nil {
		e.workflowReturnCanceled = true
		err = canceled
	}
	failure, encodeErr := apperror.Encode(err, encode)
	if encodeErr != nil {
		return
	}
	e.workflowReturnFailure = &failure
}
