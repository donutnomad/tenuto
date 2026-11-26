package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/google/uuid"
)

// InputValidator 是无状态输入校验：只看 name+args，返回 error=拒绝。
// 无状态 → 无需 replay → 拒绝零 history → 天然无重放问题（对标 Temporal replay-skip validator）。
// 有状态校验（读 workflow 状态）明确不做：那需先 replay，replay 需先有 accepted 事件，就不"零 history"了；
// 按状态拒绝的需求用 handler 返 error 表达（多一条 history，零架构负债）。
type InputValidator func(name string, args protocol.Payloads) error

// updateSoftTimeout 对标 Temporal LongPollExpirationInterval（≈20s）：Completed 等待的软超时。
const updateSoftTimeout = 20 * time.Second

// RegisterUpdateValidator 按 update name 注册无状态输入校验器。未注册则该 name 的 update 全通过。
func (s *Server) RegisterUpdateValidator(name string, v InputValidator) {
	s.updateValidatorsMu.Lock()
	defer s.updateValidatorsMu.Unlock()
	s.updateValidators[name] = v
}

// lookupUpdateValidator 读锁取指定 name 的校验器（与 RegisterUpdateValidator 并发安全）。
func (s *Server) lookupUpdateValidator(name string) (InputValidator, bool) {
	s.updateValidatorsMu.RLock()
	defer s.updateValidatorsMu.RUnlock()
	v, ok := s.updateValidators[name]
	return v, ok
}

func (s *Server) UpsertSearchAttributes(ctx context.Context, namespace, workflowID, runID string, attrs map[string]string) error {
	if runID == "" && workflowID == "" {
		return protocol.InvalidRequestf("workflow run id is empty")
	}
	// 原子写收进 lease 的 fn（fn 本就在 RunInTx 内）。Upsert 不动 current、不调度 WFT（只更新索引字段）。
	return s.withWorkflowLease(ctx, namespace, workflowID, runID, func(tx execution.ExecutionTx, current *execution.Run) (LeaseAction, error) {
		// runBranchInfoFromRun 是纯内存版（不走 mgr.Get），避免在 RunInTx 内重入存储导致死锁。
		tid, bid, err := runBranchInfoFromRun(current)
		if err != nil {
			return LeaseAction{}, err
		}
		if e := execution.UpsertSearchAttributes(tx, current, tid, bid, attrs); e != nil {
			return LeaseAction{}, e
		}
		return LeaseAction{}, nil
	})
}

// SignalWorkflow 向 Run 发送 Signal，返回 WorkflowSignaled 的事件 ID。requestID 非空时同一个 Run 上同一个
// requestID 只记一条 WorkflowSignaled，重发的请求返回第一次记下的事件 ID；为空时不去重。
func (s *Server) SignalWorkflow(ctx context.Context, namespace, workflowID, runID, name, requestID string, input map[string]any) (int64, error) {
	if err := execution.ValidateRequestID(requestID); err != nil {
		return 0, err
	}
	if name == "" {
		return 0, protocol.InvalidRequestf("workflow interaction name is empty")
	}
	if runID == "" && workflowID == "" {
		return 0, protocol.InvalidRequestf("workflow run id is empty")
	}
	// 给了 runID 时在 lease 外校验 Run 未结束。只给 workflowID 时 current 在 lease 内解析，终态判定用
	// run.Status（RunInTx 内不重入 mgr.GetEvents/Update，避免死锁），scheduleWorkflowTaskIfRunning 不为终态 Run 调度。
	if runID != "" {
		if err := s.validateInteraction(namespace, workflowID, runID, name); err != nil {
			return 0, err
		}
	}
	var eventID int64
	err := s.withWorkflowLease(ctx, namespace, workflowID, runID, func(tx execution.ExecutionTx, current *execution.Run) (LeaseAction, error) {
		// runBranchInfoFromRun 是纯内存版（不走 mgr.Get），避免在 RunInTx 内重入存储导致死锁。
		tid, bid, err := runBranchInfoFromRun(current)
		if err != nil {
			return LeaseAction{}, err
		}
		id, e := execution.RecordSignal(tx, current, tid, bid, name, requestID, input)
		if e != nil {
			return LeaseAction{}, e
		}
		eventID = id // 闭包捕获传出（fn 只返回 LeaseAction）
		return LeaseAction{ScheduleWFT: true, AfterSignal: true}, nil
	})
	if err != nil {
		return 0, err
	}
	return eventID, nil
}

func (s *Server) QueryWorkflow(ctx context.Context, namespace, workflowID, runID, name string, input map[string]any) (execution.WorkflowQueryResult, error) {
	workflowRun, state, err := s.view.GetRunWithState(namespace, workflowID, runID)
	if err != nil {
		return execution.WorkflowQueryResult{}, err
	}
	runID = workflowRun.ID
	events, err := s.GetEvents(runID)
	if err != nil {
		return execution.WorkflowQueryResult{}, err
	}
	result := execution.ProjectWorkflowQuery(workflowRun, name, state, events)

	// B1 动态 Query Handler：name 非空时就地重放到最新态、对 query 闭包求值一次（server 与 executor
	// 同进程，不投远端 worker）。name 为空时只返回上面的历史投影。
	if name != "" {
		qres, qrejected, qerr := s.evaluateQuery(ctx, runID, name, input, state, events)
		result.QueryResult = qres
		result.QueryRejected = qrejected
		result.QueryError = qerr
	}
	return result, nil
}

// evaluateQuery 就地构造 query 专用只读 executor、ReplayForQuery 重放到最新态、按 name 求值 query
// 闭包一次（B1）。返回 (结果 map, 是否被拒, 错误详情)。绝不裸 panic、不返回 500——任何失败都归一到
// QueryRejected + 可辨识 QueryError（对齐 Temporal QueryFailedError）。
//
// 失败归类：
//   - name=="__stack_trace" → 内置路径（不查 queryHandlers），返回当前 blocked lane 栈（M5 单协程）。
//   - 读取配置失败 → rejected（无法重放）。
//   - ReplayForQuery 非确定性 err（S2：blocked run 尾部 WFTFailed 串重放复现）→ rejected + err 详情。
//   - 未注册 name（S4）→ rejected + "unknown queryType: <name>"（含旧产物无 SetQueryHandler，S5）。
//   - handler 求值 err → rejected + err.Error()。
//   - handler 返回 map 含非空 "__query_error"（F1）→ rejected + 该值。
func (s *Server) evaluateQuery(ctx context.Context, runID, name string, input map[string]any, state *execution.WorkflowExecutionState, events []history.Event) (map[string]any, bool, string) {
	// __stack_trace 内置路径（选项 2 第一步）：返回 workflow【逻辑执行位置】——步骤进度取自事件历史，当前等待点取自 execution state
	// （不重放、不抓 Go 解释器栈），即便旧产物无 SetQueryHandler 也可用（S5）。
	// 放在重放之前：逻辑位置不依赖 ReplayForQuery，更快更稳（避开重放期抓栈的中途 park 复杂性）。
	if name == stackTraceQueryType {
		return execution.StackTraceProgress(state, events), false, ""
	}

	wfConfig, err := s.GetWorkflowConfig(ctx, runID, "")
	if err != nil {
		return nil, true, "load workflow config: " + err.Error()
	}
	return s.queryEvaluator.EvaluateQuery(ctx, wfConfig, events, name, input)
}

// UpdateWorkflow 接受一次 Update：args 是按 handler 业务参数顺序排列的 payload。
func (s *Server) UpdateWorkflow(ctx context.Context, namespace, workflowID, runID, name, updateID string, stage protocol.UpdateWaitStage, args protocol.Payloads) (protocol.WorkflowUpdateResult, error) {
	args, err := normalizeUpdateArgs(args)
	if err != nil {
		return protocol.WorkflowUpdateResult{}, err
	}
	if name == "" {
		return protocol.WorkflowUpdateResult{}, protocol.InvalidRequestf("workflow interaction name is empty")
	}
	if runID == "" && workflowID == "" {
		return protocol.WorkflowUpdateResult{}, protocol.InvalidRequestf("workflow run id is empty")
	}
	stage, err = protocol.NormalizeUpdateWaitStage(stage)
	if err != nil {
		return protocol.WorkflowUpdateResult{}, err
	}
	if updateID == "" {
		updateID = uuid.NewString()
	}
	// 无状态 args 校验：在 Register waiter、进 lease 之前。拒绝时零 history、不注册 waiter、不 ScheduleWFT，
	// 直接返回 error（对标 Temporal reject）。只看 name+args，不读 workflow 状态 → 无副作用泄漏、无需 replay。
	if v, ok := s.lookupUpdateValidator(name); ok {
		if err := v(name, args); err != nil {
			return protocol.WorkflowUpdateResult{}, newUpdateRejectedError(err)
		}
	}
	// ① updateID 幂等——lease 前预取历史快照（锁外，可安全调 GetEvents，绝不在 RunInTx 内重入）。
	//    ExecutionTx 无"读本 run 全部事件"的方法，无法在 lease 内去重，故走"lease 前快照 + lease 内判定"。
	//    C2 边界（诚实标注）：本引擎无 rejected 事件，被拒的 updateID 不落任何历史，因此对被拒 update
	//    没有幂等——重复请求会被当作全新 update 重新走一遍。此处不引入"负 registry"存被拒记录（避免负债）。
	existing, hasExisting := s.lookupUpdateFromHistory(namespace, workflowID, runID, updateID)

	// 已记录的 Update 在 Run 结束之后仍返回记录的结果；新的 Update 要求 Run 未结束。
	// runID 非空：lease 外读 execution state 判终态。runID 空（按 workflowID 解析 current）：
	// 终态判定在 lease 内用 run.Status 做。
	if runID != "" && !hasExisting {
		if err := s.validateInteraction(namespace, workflowID, runID, name); err != nil {
			return protocol.WorkflowUpdateResult{}, err
		}
	}

	// ② 写在锁内：按快照三态判定 Append accepted。withWorkflowLease 全程持 keyed lock，fn 内绝不 Wait。
	var result protocol.WorkflowUpdateResult
	var updateRunID string
	err = s.withWorkflowLease(ctx, namespace, workflowID, runID, func(tx execution.ExecutionTx, current *execution.Run) (LeaseAction, error) {
		tid, bid, err := runBranchInfoFromRun(current)
		if err != nil {
			return LeaseAction{}, err
		}
		var scheduled bool
		var e error
		result, scheduled, e = execution.AcceptOrReuseUpdate(tx, current, tid, bid, name, updateID, args, existing, hasExisting)
		if e != nil {
			return LeaseAction{}, e
		}
		updateRunID = current.ID
		return LeaseAction{ScheduleWFT: scheduled}, nil
	})
	if err != nil {
		return protocol.WorkflowUpdateResult{}, err
	}

	if stage == protocol.UpdateWaitStageEnums.Accepted {
		return result, nil // accept-only 立即返回
	}

	// 幂等命中已完成快照：result 已带 Completed+Output，无需再等 waiter（否则会空等到软超时）。
	if result.Stage == protocol.UpdateReachedStageEnums.Completed {
		return result, nil
	}

	// ③ 等在锁外：long-poll。withWorkflowLease 已返回、keyed lock 已释放。
	return s.awaitUpdateResult(ctx, namespace, updateRunID, updateID, result), nil
}

// awaitUpdateResult 等待 runID 上已接受的 Update 完成，结果总从 History 读取：Update 完成时是 handler 的结果，
// Run 先结束时是 History 里记下的失败终态（见 execution.UpdateRecordsFromEvents）。
// 每一轮先读 Run 的 NextEventID 再读 History，未完成时经 runWatch 等 Run 变化，Run 在哪个副本上变化都会唤醒。
// 软超时时仍未完成则返回 Accepted，不报错，让 client 再来（对标 Temporal）。
func (s *Server) awaitUpdateResult(ctx context.Context, namespace, runID, updateID string, result protocol.WorkflowUpdateResult) protocol.WorkflowUpdateResult {
	deadline := s.clock.Now().Add(s.updateSoftTimeout)
	for {
		state, err := s.mgr.GetWorkflowExecutionState(runID)
		if err != nil {
			return result
		}
		if rec, ok := s.lookupUpdateFromHistory(namespace, "", runID, updateID); ok && rec.CompletedEventID != 0 {
			return execution.CompleteUpdateResult(result, rec.Outcome)
		}
		if history.IsTerminalStatus(state.Status) {
			return result
		}
		remaining := deadline.Sub(s.clock.Now())
		if remaining <= 0 || !s.runWatch.Wait(ctx, runID, state.NextEventID, remaining) {
			return result
		}
	}
}

// UpdateWithStart 原子地「确保该 Workflow ID 有进行中的 Run」+「发送 Update 并等结果」。
//   - 没有 Run，或 current Run 已结束 → 按 workflowType 的 active 配置启动新 Run，再接收 Update；
//   - 已有进行中的 Run → 复用它接收 Update，startInput 不生效；它的类型与 workflowType 不同时报错。
//
// workflowID 为空时服务端生成，等同于每次都启动新 Run。start 与 UpdateAccepted 在同一把
// per-(namespace, workflowID) keyed lock、同一个事务里提交；等待结果的 long-poll 在锁外。
func (s *Server) UpdateWithStart(
	ctx context.Context,
	namespace, workflowType, workflowID string,
	startInput map[string]any,
	updateName, updateID string,
	args protocol.Payloads,
	stage protocol.UpdateWaitStage,
) (runID string, result protocol.WorkflowUpdateResult, err error) {
	args, err = normalizeUpdateArgs(args)
	if err != nil {
		return "", protocol.WorkflowUpdateResult{}, err
	}
	if updateName == "" {
		return "", protocol.WorkflowUpdateResult{}, protocol.InvalidRequestf("workflow interaction name is empty")
	}
	if workflowType == "" {
		return "", protocol.WorkflowUpdateResult{}, protocol.InvalidRequestf("workflow type is empty")
	}
	if workflowID == "" {
		workflowID = execution.NewID()
	}
	namespace = protocol.NormalizeNamespace(namespace)
	if updateID == "" {
		updateID = uuid.NewString()
	}
	stage, err = protocol.NormalizeUpdateWaitStage(stage)
	if err != nil {
		return "", protocol.WorkflowUpdateResult{}, err
	}
	// 无状态 args 校验（与 UpdateWorkflow 一致：拒绝时零副作用——不 start、不 accept、不注册 waiter）。
	if v, ok := s.lookupUpdateValidator(updateName); ok {
		if e := v(updateName, args); e != nil {
			return "", protocol.WorkflowUpdateResult{}, newUpdateRejectedError(e)
		}
	}

	// updateID 幂等——lease 前预取历史快照（锁外，可安全调 GetEvents）。workflow 不存在时解析失败返回
	// (_, false)，即无既有 update（首次 start 场景）；已存在则可命中已 accepted/completed 的快照。
	existing, hasExisting := s.lookupUpdateFromHistory(namespace, workflowID, "", updateID)

	// start + accept 在同一 keyed lock 内原子完成（ensureStartedThenAcceptUpdate）。
	runID, err = s.ensureStartedThenAcceptUpdate(
		ctx, namespace, workflowType, workflowID, startInput, updateName, updateID, args, existing, hasExisting, &result)
	if err != nil {
		return "", protocol.WorkflowUpdateResult{}, err
	}

	if stage == protocol.UpdateWaitStageEnums.Accepted {
		return runID, result, nil // accept-only 立即返回
	}
	if result.Stage == protocol.UpdateReachedStageEnums.Completed {
		return runID, result, nil // 幂等命中已完成快照
	}

	// long-poll 等 handler 回传（与 UpdateWorkflow 相同）。keyed lock 已在 ensureStartedThenAcceptUpdate 内释放。
	return runID, s.awaitUpdateResult(ctx, namespace, runID, updateID, result), nil
}

// SignalWithStart 原子地「确保该 Workflow ID 有进行中的 Run」+「发送 Signal」。启动判定与 UpdateWithStart
// 相同；Signal 发出即返回，返回 Signal 的 eventID。requestID 是启动与 Signal 的幂等键：启动时记进
// WorkflowStarted.RequestID；重发时 current Run（含已结束）或它的链首已记过这个 requestID 的 Signal，
// 返回那个 Run 与第一次的事件 ID，不启动新 Run、不再记 Signal。
func (s *Server) SignalWithStart(
	ctx context.Context, namespace, workflowType, workflowID string,
	startInput map[string]any, signalName, requestID string, signalInput map[string]any,
) (runID string, eventID int64, err error) {
	if err := execution.ValidateRequestID(requestID); err != nil {
		return "", 0, err
	}
	if signalName == "" {
		return "", 0, protocol.InvalidRequestf("workflow interaction name is empty")
	}
	if workflowType == "" {
		return "", 0, protocol.InvalidRequestf("workflow type is empty")
	}
	if workflowID == "" {
		workflowID = execution.NewID()
	}
	namespace = protocol.NormalizeNamespace(namespace)
	return s.ensureStartedThenSignal(ctx, namespace, workflowType, workflowID, startInput, signalName, requestID, signalInput)
}

// normalizeUpdateArgs 把 Update 入参规范化：nil 记为零个参数，每个 payload 走统一的规范形式。
func normalizeUpdateArgs(args protocol.Payloads) (protocol.Payloads, error) {
	if args == nil {
		args = protocol.Payloads{}
	}
	args, err := protocol.NormalizePayloads(args)
	if err != nil {
		return nil, protocol.InvalidRequestf("update args: %w", err)
	}
	return args, nil
}

// lookupUpdateFromHistory 从该 run 历史查指定 updateID 的 update 记录。
// 复用 updateRecordsFromEvents 的组装逻辑，不复制代码。
func (s *Server) lookupUpdateFromHistory(namespace, workflowID, runID, updateID string) (history.WorkflowUpdateRecord, bool) {
	run, err := s.resolveRun(namespace, workflowID, runID)
	if err != nil {
		return history.WorkflowUpdateRecord{}, false
	}
	events, err := s.GetEvents(run.ID)
	if err != nil {
		return history.WorkflowUpdateRecord{}, false
	}
	for _, rec := range execution.UpdateRecordsFromEvents(events) {
		if rec.UpdateID == updateID {
			return rec, true
		}
	}
	return history.WorkflowUpdateRecord{}, false
}

// validateInteraction 要求请求寻址的 Run 存在且未结束。
func (s *Server) validateInteraction(namespace, workflowID, runID, name string) error {
	if name == "" {
		return protocol.InvalidRequestf("workflow interaction name is empty")
	}
	run, err := s.resolveRun(namespace, workflowID, runID)
	if err != nil {
		return err
	}
	status, err := s.GetRunStatus(run.ID)
	if err != nil {
		return err
	}
	if history.IsTerminalStatus(status) {
		return protocol.InvalidRequestf("workflow %s is %s", runID, status)
	}
	return nil
}

// closeEventScan 是读取结束事件时向前查看的事件数：结束事件之后不再追加事件，留出余量即可。
const closeEventScan = 8

// WaitRunClosed 等待请求寻址的 Run 结束，返回结束后的 Run 与它的结束事件；ctx 先结束时返回 ctx 的错误。
// 每一轮先读 Run 的 execution state，未结束时经 runWatch 等 Run 变化：Run 在哪个副本上结束都会唤醒。
// 只等这一个 Run，continue-as-new 与重试产生的下一个 Run 由调用方按结束事件跟随。
func (s *Server) WaitRunClosed(ctx context.Context, namespace, workflowID, runID string) (*execution.Run, history.Event, error) {
	ref, err := s.resolveRunRef(namespace, workflowID, runID)
	if err != nil {
		return nil, history.Event{}, err
	}
	for {
		state, err := s.mgr.GetWorkflowExecutionState(ref.ID)
		if err != nil {
			return nil, history.Event{}, err
		}
		if history.IsTerminalStatus(state.Status) {
			return s.closedRun(ref.ID, state.LastEventID)
		}
		if !s.runWatch.Wait(ctx, ref.ID, state.NextEventID, 0) {
			return nil, history.Event{}, ctx.Err()
		}
	}
}

// closedRun 读取已结束的 Run 与它的结束事件。
func (s *Server) closedRun(runID string, lastEventID int64) (*execution.Run, history.Event, error) {
	run, err := s.mgr.Get(runID)
	if err != nil {
		return nil, history.Event{}, err
	}
	events, err := s.mgr.GetEventsRange(runID, max(0, lastEventID-closeEventScan), lastEventID)
	if err != nil {
		return nil, history.Event{}, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		if history.IsWorkflowTerminalEventType(events[i].EventType()) {
			return run, events[i], nil
		}
	}
	return nil, history.Event{}, fmt.Errorf("run %s is %s but its last %d events have no closing event", runID, run.Status, closeEventScan)
}
