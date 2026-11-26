package interpreter

import (
	"context"
	"runtime"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	coroutine2 "github.com/donutnomad/tenuto/internal/executor/coroutine"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	wlruntime "github.com/donutnomad/wlang/runtime"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// WorkflowExecutor 解释执行一个 Workflow：把 Event History 按决策批灌入事实表，在协作式
// Scheduler 上推进 Workflow 协程，产出下一批 Command。
//
// 两种使用方式共用同一套驱动（restore + activate）：
//   - 一次性：Execute / ReplayForQuery 处理构造时给定的完整历史，结束后回收协程栈。
//   - session：workflowExecutionSession 持有 executor 跨多个 Workflow Task，协程栈保留在内存里，
//     每次 Activate 只灌入增量事件。
//
// 不变式：executor 的全部状态是「已应用历史前缀」的确定性函数。事实表、游标和协程栈随应用的
// 事件单调累积，从不按 Workflow Task 重置；一次性重放到某个位置与 session 增量推进到同一位置，
// 得到的状态相同。只有 commands / evalErr / execErr 属于单次 activation。
type WorkflowExecutor struct {
	ctx      context.Context
	workflow *program.WorkflowConfig

	// 事实表：由 applyBatch 从事件灌入，协程只读。Activity 的结果以调度事件 ID 为键。
	completed map[int64]map[string]any
	failed    map[int64]*protocol.Failure
	// unclaimedActivities 是历史里还没有对应调用的 ActivityScheduled（ActivityID → 调度事件 ID，按历史顺序）；
	// awaitingActivities 是已发出调度命令、调度事件尚未到达的调用。二者把调度事件绑定到 ExecuteActivity
	// 调用（见 activityCall）。
	unclaimedActivities map[string][]int64
	awaitingActivities  map[string]*activityCall
	signals             map[string][]map[string]any
	// signalsConsumed 是每个 Signal 名称已被 SignalFuture 消费的条数，见 SignalFuture。
	signalsConsumed map[string]int
	updateAccepted  map[string][]history.WorkflowUpdateRecord
	updateCompleted map[string]map[string]any
	timersScheduled map[string]history.Event
	timersFired     map[string]bool
	// rootScope 是根 Context 的取消作用域：WorkflowCancellationRequested 灌入后、本批协程推进前取消。
	rootScope *cancelScope
	// timerCanceled / childCanceled 记录 Workflow 代码一侧已按取消结束的操作：取消请求发出即返回的
	// 子 Workflow Future 与被取消的 timer。它们随取消的执行点确定性地写入，重放到同一位置时相同。
	// Activity 的同类状态在它自己的 activityCall 上。
	timerCanceled map[string]bool
	childCanceled map[string]bool
	// childCancelRequested 记录已经发出取消请求的子 Workflow。
	childCancelRequested map[string]bool
	markers              map[string]map[string]any
	// changeVersions 记录每个 changeID 在本 Run 已确定的版本，包含 replay 首见时返回的
	// DefaultVersion；history 里的 version marker 到达时覆盖。
	changeVersions map[string]int64
	// shadowedMarkers 按 marker key 去重 shadowMarker：同一个 marker 在历史里只有一条
	// MarkerRecorded，多个读回调用点只补一条 shadow 命令。
	shadowedMarkers map[string]bool
	// MutableSideEffect 状态。
	// mseCallCounter：每个 id 的调用计数，每次 MutableSideEffect(id) 递增。
	// mseHistory：id → callCount → 已记录的 marker 值。去重后只有值变化的 callCount 有条目，
	// 查找取不大于当前 callCount 的最近一条。
	mseCallCounter map[string]int
	mseHistory     map[string]map[int]map[string]any
	childStarted   map[string]history.Event
	childCompleted map[string]map[string]any
	childFailures  map[string]*protocol.Failure
	currentTime    time.Time
	input          map[string]any
	seqCounter     int

	// commands 是当前 live 批产出、要上报给 Server 的命令。
	commands []history.Command
	// replayCommands 是 replay 批产出的命令（含幂等命中补产的 shadow），只用于和历史命令事件
	// 比对，从不上报。replayBatchStart 是当前重放批的第一条命令在其中的位置。
	replayCommands   []history.Command
	replayBatchStart int
	// spawnedUpdates 是已经起过 handler 协程的 UpdateID 集合，保证每个 update 只起一个协程。
	spawnedUpdates map[string]bool
	evalErr        error
	// workflowReturnErr 是 main body 返回非 nil error 时的错误消息。
	workflowReturnErr string
	// workflowReturnFailed 标记 body 返回了非 nil error。失败判定用它，
	// typed 应用错误的 message 允许为空。
	workflowReturnFailed bool
	// workflowReturnFailure 是 body 返回结构化错误时的完整载荷；普通 error 只有 message。
	workflowReturnFailure *protocol.Failure
	// workflowReturnCanceled 标记 body 返回了取消错误：收敛时产 CmdCancelWorkflow，
	// workflowReturnFailure 带取消错误的 details。
	workflowReturnCanceled bool
	// workflowReturnOutput 是 body 返回的数据值，收敛时写入 CmdCompleteWorkflow.Output。
	workflowReturnOutput map[string]any
	// mainReturned 表示 main 协程已正常返回（程序 body 执行完）。Workflow 在这一刻结束：
	// emitConvergedCommand 据此产出终态命令，仍在运行的 workflow.Go 协程与 update handler 被放弃。
	mainReturned bool
	// blocked 表示最近一轮调度里仍有协程在等待外部事件；挂起原语 park 前置位，
	// 命中条件时经 progressed 清除。
	blocked               bool
	terminated            bool // 事件历史中已存在终态事件
	cancellationRequested bool // 历史里已有 WorkflowCancellationRequested
	// events 是一次性入口（Execute / ReplayForQuery）要处理的完整历史；session 模式下为空，
	// 事件由 Restore / Activate 分批送入。
	events []history.Event
	// isReplay 按决策批设置：重放已提交的批为 true，live 批为 false。
	// replay 批里命令进 replayCommands；SideEffect / RandomUUID / RandomInt 缺 marker 视为
	// 非确定性并 panic，GetVersion 缺 marker 返回 DefaultVersion。
	isReplay bool

	// queryHandler 在 query handler 求值期间为 true：handler 在调用方 goroutine 上执行一次，
	// 阻塞原语与 workflow.Go 以 errQueryHandlerBlocked 拒绝。
	queryHandler bool

	// sched 是顶层 Scheduler，由 startScheduler 创建，生命周期与 executor 相同。
	// main、workflow.Go lane 和 update handler 都是它的协程。
	sched *coroutine2.Scheduler

	// spawnSeq 是 workflow.Go 调用序游标：每个 Go 调用点消耗一个递增值，
	// childLane = parentLane(ctx) + "/" + spawnSeq。Go 调用点的程序顺序固定，
	// 所以同一调用点在任何一次执行里得到同一个 childLane。
	spawnSeq int
	// activeWLExec 是当前 WLang program 的 executor（executeWLang 注入）。lane 与 update handler
	// 协程用它 InvokeClosure；与 runtimeWorkflowAPI.wlExec 是同一个实例。
	activeWLExec *wlruntime.Executor
	// activeAPI 是当前 program 的 runtimeWorkflowAPI（executeWLang 注入）。update handler 协程
	// 经它读 handler 注册表；协程可能早于 main 首次运行，此时为 nil。
	activeAPI              *runtimeWorkflowAPI
	stepHook               wlruntime.StepHook
	debugEffectHook        executor.DebugEffectHook
	debugWaitingActivities map[uint64]executor.DebugWaitingActivity
	// debugActivities 按调用顺序记录调试回放里的每次 ExecuteActivity；为 nil 时不记录。
	debugActivities []*executor.DebugActivity

	// WaitGroup 状态：waitGroup 值只携带 id，计数器存在这里。
	// waitGroupSeq 按 NewWaitGroup 调用顺序分配 id；waitGroupN 是 id → 计数。
	// 读写都发生在协作式调度的单一执行线上，普通 int 即可。
	waitGroupSeq int
	waitGroupN   map[int]int

	// Selector 状态：selectorHandle 值只携带 id（registry 要求值接收者），case 与回调存在这里。
	// selectorSeq 按 NewSelector 调用顺序分配 id；selectors 是 id → selectorState。
	selectorSeq int
	selectors   map[int]*selectorState

	// pendingFutureBridges 是 Selector.AddFuture 时尚未 ready 的 future。
	// 每批事实灌入后 flushFutureBridges 逐个 poll，ready 的 Set 后移出。
	pendingFutureBridges []futureBridge
	execErr              error
	// historyMetrics 是历史规模的累计值；unmeasuredEvents 是已应用但尚未计入它的事件。
	// activationInfo 是当前 Workflow Task 的 GetInfo 快照。
	historyMetrics   history.HistoryMetrics
	unmeasuredEvents []history.Event
	activationInfo   workflow.Info
}

func (e *WorkflowExecutor) emitDebugEffect(effect executor.DebugEffect) {
	if e.debugEffectHook == nil {
		return
	}
	if e.sched != nil {
		effect.RoutineID = e.sched.CurrentRoutineID()
	}
	e.debugEffectHook(effect)
}

// park 挂起当前协程，是所有挂起原语的唯一出口：把控制权交还 Scheduler，下一次 RunBatch
// 恢复后从这里返回，调用方回到自己的「检查-park」循环重查条件。
// 不在 Scheduler 的协程上下文里调用时，直接结束当前 goroutine。
func (e *WorkflowExecutor) park() {
	if e.queryHandler {
		panic(errQueryHandlerBlocked)
	}
	if e.sched != nil {
		if yield := e.sched.CurrentYield(); yield != nil {
			// WLang Executor 的 scope 是所有协程共享的单个可变字段。协程 park 时栈冻结在
			// InvokeClosure 内部，scope 的恢复 defer 尚未执行；这里保存并在恢复后还原，
			// 其他协程交错执行时才不会在本协程的作用域链上解析变量。
			var saved *wlruntime.Scope
			if e.activeWLExec != nil {
				saved = e.activeWLExec.Scope()
			}
			yield()
			if e.activeWLExec != nil {
				e.activeWLExec.SetScope(saved)
			}
			return
		}
	}
	runtime.Goexit()
}

// progressed 由挂起原语在条件命中、即将返回时调用：清除 blocked，并告诉 Scheduler 当前协程
// 本轮取得了进展，RunBatch 因此再转一轮，让等待它的其他协程有机会重查。
// 不变式：每个 park 循环未命中时重新置 blocked=true，所以收敛时 blocked 的值等于
// 最后一轮是否仍有协程在等待。
func (e *WorkflowExecutor) progressed() {
	e.blocked = false
	if e.sched != nil {
		e.sched.Unblocked()
	}
}

// NewWorkflowExecutor 创建执行 workflow 的 executor；workflow 为 nil 时 main 不执行任何程序。
func NewWorkflowExecutor(workflow *program.WorkflowConfig, events []history.Event) *WorkflowExecutor {
	return NewWorkflowExecutorWithContext(context.Background(), workflow, events)
}

func NewWorkflowExecutorWithContext(ctx context.Context, workflow *program.WorkflowConfig, events []history.Event) *WorkflowExecutor {
	if ctx == nil {
		ctx = context.Background()
	}
	executor := &WorkflowExecutor{
		ctx:                 ctx,
		workflow:            workflow,
		completed:           make(map[int64]map[string]any),
		failed:              make(map[int64]*protocol.Failure),
		unclaimedActivities: make(map[string][]int64),
		awaitingActivities:  make(map[string]*activityCall),
		signals:             make(map[string][]map[string]any),
		signalsConsumed:     make(map[string]int),
		updateAccepted:      make(map[string][]history.WorkflowUpdateRecord),
		updateCompleted:     make(map[string]map[string]any),
		timersScheduled:     make(map[string]history.Event),
		timersFired:         make(map[string]bool),
		markers:             make(map[string]map[string]any),
		changeVersions:      make(map[string]int64),
		shadowedMarkers:     make(map[string]bool),
		mseCallCounter:      make(map[string]int),
		mseHistory:          make(map[string]map[int]map[string]any),
		childStarted:        make(map[string]history.Event),
		childCompleted:      make(map[string]map[string]any),
		childFailures:       make(map[string]*protocol.Failure),

		timerCanceled:        make(map[string]bool),
		childCanceled:        make(map[string]bool),
		childCancelRequested: make(map[string]bool),
	}
	executor.rootScope = executor.newCancelScope(nil)
	executor.events = events
	// 终态是 run 级事实：构造时扫描全量历史，Execute 入口据此短路。
	// 其余事实（含取消请求）按决策批由 applyBatch 灌入。
	for _, event := range events {
		if event.WorkflowCompleted != nil || event.WorkflowFailed != nil ||
			event.WorkflowCancelled != nil || event.WorkflowTerminated != nil {
			executor.terminated = true
		}
	}
	return executor
}

// emitCommand 是命令的统一出口：replay 批的命令进 replayCommands（只用于比对），
// live 批的命令进 commands（上报 Server）。
func (e *WorkflowExecutor) emitCommand(cmd history.Command) {
	if e.isReplay {
		e.replayCommands = append(e.replayCommands, cmd)
		return
	}
	e.commands = append(e.commands, cmd)
}

// emitShadow 在幂等命中点补一条命令：事实表已有结果、不需要真命令的调用点，在 replay 批仍然
// 产出当年那条命令，使 replayCommands 与历史命令事件一一对应。live 批不产。
func (e *WorkflowExecutor) emitShadow(cmd history.Command) {
	if e.isReplay {
		e.replayCommands = append(e.replayCommands, cmd)
	}
}

// Execute 对构造时给定的完整历史做一次性决策：重放已提交的决策批，再把末批作为 live 批执行，
// 返回 live 批产出的命令。它与 execution session 走同一套驱动（restore + activate），
// 区别只在于一次调用内完成、结束时回收全部协程栈。
func (e *WorkflowExecutor) Execute() ([]history.Command, error) {
	if e.terminated {
		return nil, nil
	}
	batches := executor.SplitBatches(e.events)
	live := len(batches) - 1
	e.startScheduler()
	defer e.sched.Reclaim()
	if err := e.restore(batches[:live]); err != nil {
		return nil, err
	}
	if err := e.activate(batches[live]); err != nil {
		return nil, err
	}
	return e.commands, nil
}

// ReplayForQuery 把历史只读重放到最后一个已完成的 Workflow Task，返回活的 runtimeWorkflowAPI
// （query 注册表）与 WLang executor，调用方在其上求值 query 闭包。
//
// 末批（最后一个 WorkflowTaskCompleted 之后的事件）还没有被任何 Workflow Task 处理：只灌事实，
// 协程停在已提交的位置。让协程越过这个位置会执行到尚未落 marker 的 SideEffect / RandomUUID /
// RandomInt / GetVersion，得到与在途 Workflow Task 不一致的结果。
// 一个 Workflow Task 都没完成的 Run，main 尚未运行，返回的 api 为 nil。
func (e *WorkflowExecutor) ReplayForQuery() (*runtimeWorkflowAPI, *wlruntime.Executor, error) {
	return e.replayReadOnly(false)
}

// replayReadOnly 只读重放历史：已提交的批按 replay 批执行，命令只进 replayCommands 并被丢弃；
// 不做历史比对，不产收敛命令；终态 Run 也完整重放，以得到最终内存态。
// advanceTrailing 决定末批的处理：query 传 false，只灌事实；调试回放传 true，
// 让协程按 replay 批继续推进到当前历史能到达的位置，用于逐步展示执行过程。
func (e *WorkflowExecutor) replayReadOnly(advanceTrailing bool) (*runtimeWorkflowAPI, *wlruntime.Executor, error) {
	e.startScheduler()
	batches := executor.SplitBatches(e.events)
	committed := len(batches) - 1
	for _, batch := range batches[:committed] {
		e.runDecisionBatch(batch, true)
	}
	if advanceTrailing {
		e.runDecisionBatch(batches[committed], true)
	} else {
		e.isReplay = true
		e.applyBatch(batches[committed])
	}
	e.sched.Reclaim()
	if err := e.executionError(); err != nil {
		return nil, nil, err
	}
	return e.activeAPI, e.activeWLExec, nil
}

// startScheduler 创建顶层 Scheduler 并登记 main 协程；main 在第一次 RunBatch 时才开始执行。
func (e *WorkflowExecutor) startScheduler() {
	e.execErr = nil
	e.sched = coroutine2.NewScheduler()
	e.sched.Spawn("main", func(yield func()) {
		defer func() {
			if r := recover(); r != nil {
				// 非确定性 panic 原样透传，使 Workflow Task 以可辨识的原因失败；其余 panic 统一包装。
				if nd, ok := r.(executor.NonDeterministicError); ok {
					e.execErr = nd
				} else {
					e.execErr = executor.WorkflowPanicError{Value: r}
				}
			}
		}()
		e.execErr = e.executeWorkflow()
		// 经 runtime.Goexit 结束的 main（ContinueAsNew、等待 marker 落库）到不了这里。
		e.mainReturned = e.execErr == nil
	})
}

// restore 重放已提交的决策批，并把重放产出的命令序列与这些批里的历史命令事件比对。
// 每个批对应一个已完成的 Workflow Task：它看到的外部事件加上它产出的命令事件。
// 调用前必须已 startScheduler。返回错误时 executor 不可继续使用。
func (e *WorkflowExecutor) restore(committed [][]history.Event) error {
	var historyCommands []history.Event
	for _, batch := range committed {
		e.runDecisionBatch(batch, true)
		if err := e.executionError(); err != nil {
			return err
		}
		for _, event := range batch {
			if executor.IsCommandEvent(event) {
				historyCommands = append(historyCommands, event)
			}
		}
	}
	// 重放中执行到 ContinueAsNew 的 Run 已经结束，其后的命令序列不参与比对。
	if e.canInReplay() {
		return nil
	}
	return executor.MatchReplayWithHistory(e.replayCommands, historyCommands)
}

// activate 执行 live 决策批并在 e.commands 中留下本次 Workflow Task 的命令。
// 已终态的 Run 不产命令；其余情况在协程全部阻塞后由 emitConvergedCommand 按 main 是否已返回
// 判定是否结束 Workflow。
func (e *WorkflowExecutor) activate(batch []history.Event) error {
	e.runDecisionBatch(batch, false)
	if err := e.executionError(); err != nil {
		return err
	}
	if !e.terminated {
		e.emitConvergedCommand()
	}
	return nil
}

// runDecisionBatch 灌入一个决策批的事实，并把所有协程推进到全部阻塞。
// replay 决定本批命令的去向（见 emitCommand）以及缺 marker 时的处理。
// live 批遇到终态事件时只灌事实，协程不再前进。
//
// 取消请求的送达点固定在这里：本批灌入 WorkflowCancellationRequested 之后、协程推进之前取消根作用域，
// 对标 Temporal 处理 WorkflowExecutionCancelRequested 事件时调用 cancelHandler。取消产生的命令
// 排在本批其余命令之前，重放同一批时在同一位置产生。
//
// 取消按事件顺序送达（对标 Temporal 按序处理事件）：本批先灌入取消请求及之前的事件，取消根作用域，再灌入其余事件。
// 取消请求之后才完成的 Activity、触发的 timer、关闭的子 Workflow 在取消时仍算未结束，按取消处理；
// 不等待取消的 Future 以取消错误返回，之后到达的结果不改变它。
func (e *WorkflowExecutor) runDecisionBatch(batch []history.Event, replay bool) {
	e.isReplay = replay
	e.replayBatchStart = len(e.replayCommands)
	cut := len(batch)
	if !e.cancellationRequested {
		if i := slices.IndexFunc(batch, func(event history.Event) bool { return event.WorkflowCancellationRequested != nil }); i >= 0 {
			cut = i + 1
		}
	}
	e.applyBatch(batch[:cut])
	if e.cancellationRequested && (replay || !e.terminated) {
		e.rootScope.cancel()
	}
	e.applyBatch(batch[cut:])
	if now, ok := batchTime(batch); ok {
		e.currentTime = now
	}
	// 没有 WorkflowTaskStarted 的批（裸历史、空的 live 末批）在批尾取 GetInfo 快照。
	if !slices.ContainsFunc(batch, func(event history.Event) bool { return event.WorkflowTaskStarted != nil }) {
		e.captureActivationInfo()
	}
	if !replay && e.terminated {
		return
	}
	e.flushFutureBridges()
	e.spawnUpdateHandlers()
	e.sched.RunBatch()
}

func (e *WorkflowExecutor) executionError() error {
	if e.evalErr != nil {
		return e.evalErr
	}
	if e.execErr != nil {
		return e.execErr
	}
	return nil
}

// emitConvergedCommand 在本批协程全部阻塞后判定 Workflow 是否结束：main 已返回即结束，
// 按 main 的返回值产 CmdFailWorkflow 或 CmdCompleteWorkflow，排在本批其余命令之后。
// 此时仍在运行的 workflow.Go 协程与 update handler 被放弃，它们未完成的 Activity、timer、
// 子 Workflow 的结果不再被消费（与 Temporal 一致）。main 仍阻塞时不产终态命令。
func (e *WorkflowExecutor) emitConvergedCommand() {
	if !e.mainReturned {
		return
	}
	// 协程里的 ContinueAsNew 与 main 的返回落在同一批时，以先产出的终态命令为准。
	if slices.ContainsFunc(e.commands, func(command history.Command) bool {
		return command.Type == history.CmdContinueAsNew || command.Type == history.CmdFailWorkflow
	}) {
		return
	}
	if e.workflowReturnCanceled {
		command := history.Command{Type: history.CmdCancelWorkflow}
		if e.workflowReturnFailure != nil {
			command.Details = e.workflowReturnFailure.Details
		}
		e.emitCommand(command)
		return
	}
	if e.workflowReturnFailed {
		e.emitCommand(history.Command{
			Type:            history.CmdFailWorkflow,
			Error:           e.workflowReturnErr,
			WorkflowFailure: e.workflowReturnFailure,
		})
		return
	}
	e.emitCommand(history.Command{
		Type:   history.CmdCompleteWorkflow,
		Output: e.workflowReturnOutput,
	})
}

// executeWorkflow 是 main 协程的 body。
func (e *WorkflowExecutor) executeWorkflow() error {
	if e.workflow == nil {
		return nil
	}
	if err := e.ctx.Err(); err != nil {
		return err
	}
	return e.executeWLang(e.workflow)
}

// recordCoroutinePanic 把 lane / update handler 协程里 recover 到的 Go panic 记为 evalErr，
// 使本次 Workflow Task 失败而进程继续运行。非确定性 panic 原样保留类型，其余统一包装。
// 调用方保证 r 非 nil。
func (e *WorkflowExecutor) recordCoroutinePanic(r any) {
	if nd, ok := r.(executor.NonDeterministicError); ok {
		e.evalErr = nd
	} else {
		e.evalErr = executor.WorkflowPanicError{Value: r}
	}
}

// panicNonDeterministic 以 nonDeterministicError panic；协程入口的 recover 把它转成执行错误。
// 用于 replay 批缺少 marker 等非确定性情形。
func panicNonDeterministic(msg string) {
	panic(executor.NewNonDeterministicError(msg))
}
