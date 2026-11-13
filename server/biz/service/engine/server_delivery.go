package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/donutnomad/tenuto/protocol"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// PollWorkflowTask 取一个 workflow task lease。不再属于 open Workflow Task 的陈旧投递由 Server Ack 丢弃，
// 继续下一轮 poll；用循环而非递归，队列里堆积大量陈旧投递时栈深度恒定。
//
// 取到 lease 之后的准备、领取与 Ack/Nack 不随 ctx 取消（见 pollWorkflowTaskOnce）。
func (s *Server) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	for {
		polled, matched, err := s.pollWorkflowTaskOnce(ctx)
		if err != nil {
			return protocol.TaskLease{}, err
		}
		if !matched {
			// 暂停 Run 的 task 已被 Ack，继续下一轮 poll。
			continue
		}
		leaseCtx := context.WithoutCancel(ctx)
		lease, started, err := s.startWorkflowTask(leaseCtx, polled)
		if err != nil {
			if err := s.settlePrepareFailure(leaseCtx, polled.lease, err); err != nil {
				return protocol.TaskLease{}, err
			}
			continue
		}
		if started {
			return lease, nil
		}
		// 这份投递没有开始（WFT 已关闭、另一份投递正在执行、Run 已暂停或选版已变），已被 Ack 或 Nack，继续下一轮 poll。
	}
}

// settlePrepareFailure 处理准备 lease 时的失败，分类见 execution.ClassifyWorkflowTaskPrepareError。确定性失败（Run 的配置或
// 数据决定、每次准备都会重现）时 Run 以 ConfigError 暂停，Run 已不存在时直接丢弃任务；这两种情况都确认这份投递并返回 nil，
// 调用方继续下一轮 poll。暂时性失败原样返回 cause：投递不确认，由队列重投。
func (s *Server) settlePrepareFailure(ctx context.Context, lease protocol.TaskLease, cause error) error {
	switch execution.ClassifyWorkflowTaskPrepareError(cause) {
	case execution.PrepareFailurePauseRun:
		if err := s.pauseRunForPrepareFailure(ctx, lease.Task.RunID, cause); err != nil {
			return errors.Join(cause, err)
		}
	case execution.PrepareFailureDropTask:
		s.logger.Warn("workflow task dropped: run not found",
			slog.String("run_id", lease.Task.RunID),
			slog.Int64("scheduled_event_id", lease.Task.ScheduledEventID),
			slog.Any("err", cause),
		)
	default:
		return cause
	}
	if err := s.ackPausedWorkflowTask(ctx, lease); err != nil {
		return errors.Join(cause, err)
	}
	return nil
}

// pauseRunForPrepareFailure 在 workflow 的 keyed lock 下把准备失败的原因写成 Run 的 ConfigError。
func (s *Server) pauseRunForPrepareFailure(ctx context.Context, runID string, cause error) error {
	_, release, err := s.lockRunWorkflow(ctx, runID)
	if err != nil {
		return err
	}
	defer release()
	return s.pauseRun(runID, "workflow task cannot be prepared: "+cause.Error())
}

// polledWorkflowTask 是取到并补全了任务字段的 Workflow Task lease，以及它的 Program 版本所依据的升级候选。
//
// 领取沿用准备 lease 时在事务外读到的结果，不再重读：branch 取自 Run 创建后不变的 branch token；config 是
// 这次 lease 选中的 Program 版本的配置引用；state 是 execution state，两次读取之间没有本次领取自己的写入，沿用等价于
// 两次读取之间恰好没有并发写入，领取的事务按锁下读到的状态重新校验。state 为 nil 时领取前重新读取（准备时选版的
// 事务写过 Run，见 pollWorkflowTaskOnce）。
type polledWorkflowTask struct {
	lease      protocol.TaskLease
	workerInfo protocol.WorkerInfo
	candidate  execution.UpgradeCandidate
	branch     runBranch
	config     definition.WorkflowConfigRef
	state      *execution.WorkflowExecutionState
}

// pollWorkflowTaskOnce 取一个 lease 并补全其任务字段。matched=false 表示该 lease 属于暂停的 Run，
// 或准备时遇到确定性失败（都已 Ack），调用方应继续下一轮 poll。
//
// 任务字段按事务外读到的 Run 准备，Program 版本是 proposeWorkflowTaskProgram 的建议；领取的事务按锁下读到的 Run
// 重新确定（startWorkflowTask）。
func (s *Server) pollWorkflowTaskOnce(ctx context.Context) (polledWorkflowTask, bool, error) {
	workerInfo, _ := protocol.WorkerInfoFrom(ctx)
	s.recordWorkflowWorker(workerInfo, s.clock.Now())
	lease, err := s.dequeueWorkflowTask(ctx, workerInfo.Identity)
	if err != nil {
		return polledWorkflowTask{}, false, err
	}
	// lease 已在手：poll 的期限可能正好到期，此后的读取、Ack 与 Nack 都不能因此失败。Ack/Nack 失败时这份投递
	// 既不确认也不放回，要等队列的可见性超时（TaskQ 默认 60s）才再出现。领取成功的 lease 照常返回：Worker 的
	// HTTP 请求比服务端的 poll 期限活得久，仍会收到它。
	ctx = context.WithoutCancel(ctx)
	// 取到 lease 之后的失败经 settlePrepareFailure 分类：确定性失败不让同一个任务无限重投。
	unprepared := func(err error) (polledWorkflowTask, bool, error) {
		return polledWorkflowTask{}, false, s.settlePrepareFailure(ctx, lease, err)
	}
	run, runErr := s.mgr.Get(lease.Task.RunID)
	if runErr != nil {
		return unprepared(runErr)
	}
	taskExecutionState, stateErr := s.mgr.GetWorkflowExecutionState(run.ID)
	if stateErr != nil {
		return unprepared(stateErr)
	}
	pollState := &dispatch.WorkflowTaskState{
		ExecutionGeneration:  taskExecutionState.ExecutionGeneration,
		StickyGeneration:     taskExecutionState.StickyGeneration,
		StickyWorkerIdentity: taskExecutionState.StickyWorkerIdentity,
	}
	// 已 started 的 Workflow Task 被重新投递时沿用 started 时选中的 Program 版本：
	// WorkflowTaskStarted 先到先得，提交时按它记录的版本校验。
	polled := polledWorkflowTask{workerInfo: workerInfo, state: taskExecutionState}
	proposal := execution.ProgramVersionChoice{
		Version:     taskExecutionState.WorkflowTaskProgramVersion(lease.Task.ScheduledEventID),
		ConfigError: run.ConfigError,
	}
	if proposal.Version == "" {
		polled.candidate, proposal = s.proposeWorkflowTaskProgram(run)
	}
	// ConfigError 是确定性配置/契约/target 冲突：任务不派发，暂停推进等待人工修复。UpgradeError 只是最近一次
	// 升级尝试的诊断，不阻塞重试。暂停在事务内按锁下读到的 Run 确认（选版判定的暂停随这个事务写入）；确认时 Run
	// 已被恢复，按事务内的选版继续准备。
	if proposal.ConfigError != "" {
		choice, err := s.chooseWorkflowTaskProgram(ctx, run.ID, lease.Task.ScheduledEventID, polled.candidate)
		if err != nil {
			return unprepared(err)
		}
		if choice.ConfigError != "" {
			if ackErr := s.ackPausedWorkflowTask(ctx, lease); ackErr != nil {
				return polledWorkflowTask{}, false, ackErr
			}
			return polledWorkflowTask{}, false, nil
		}
		proposal = choice
		// 选版的事务可能改过 Run 与 execution state：领取前重新读取。
		polled.state = nil
		if run, runErr = s.mgr.Get(run.ID); runErr != nil {
			return unprepared(runErr)
		}
	}
	polled.branch.treeID, polled.branch.branchID, polled.branch.err = runBranchInfoFromRun(run)
	lease.Task = dispatch.PrepareWorkflowTask(lease.Task, dispatch.WorkflowTaskRun{
		Namespace: run.Namespace, WorkflowID: run.WorkflowID, WorkflowType: run.WorkflowType,
		ProgramVersion: proposal.Version,
	}, dispatch.WorkflowTaskWorker{Identity: workerInfo.Identity, BuildID: workerInfo.BuildID}, pollState, history.ExecutorABIVersionV1)
	// 记在这里而非 dequeue 之后：暂停 Run 的 task 走上面的 Ack 分支直接丢弃，
	// 它们从未真正开始执行，不该算进 schedule-to-start 分布。
	s.telemetry.recordScheduleToStart(ctx, lease.Task, s.clock.Now())
	polled.lease = lease
	polled.config = runConfigRef(run, lease.Task.ProgramVersion)
	return polled, true, nil
}

// startWorkflowTask 完成 token 签发、fence 校验与 WorkflowTaskStarted 落库，started 为 true 时调用方把 lease
// 交给 Worker。Run 已终态或该 WFT 已关闭时这份投递已经陈旧：Server 直接 Ack 丢弃，返回 started=false。交给
// Worker 的陈旧投递在上报时被激活 fence 拒绝，Worker Nack 后队列无限重投。领取的事务按锁下读到的 Run
// 确定 Program 版本：Run 已暂停时 Server 确认丢弃这份投递，版本与 lease 准备的不同时 Nack 它重新准备，都返回
// started=false。同一个 open WFT 的另一份投递正在执行时，这份投递被激活 fence 拒绝：Server 直接 Ack 它，返回
// started=false。
func (s *Server) startWorkflowTask(ctx context.Context, polled polledWorkflowTask) (protocol.TaskLease, bool, error) {
	lease, workerInfo := polled.lease, polled.workerInfo
	state := polled.state
	if state == nil {
		var err error
		if state, err = s.mgr.GetWorkflowExecutionState(lease.Task.RunID); err != nil {
			return protocol.TaskLease{}, false, err
		}
	}
	scheduledEventID := lease.Task.ScheduledEventID
	var err error
	lease.Task.ActivationID = history.WorkflowActivationID(lease.Task)
	lease.Task, err = s.tokens.Attach(lease.Task)
	if err != nil {
		return protocol.TaskLease{}, false, err
	}
	if state.IsTerminal() || !state.WorkflowTaskOpen(scheduledEventID) {
		return protocol.TaskLease{}, false, s.wfQueue.Ack(ctx, lease)
	}
	if err := s.validateWorkflowTaskWorkerCapabilities(ctx, polled.config, lease, workerInfo); err != nil {
		return protocol.TaskLease{}, false, err
	}
	if polled.branch.err != nil {
		return protocol.TaskLease{}, false, polled.branch.err
	}
	tid, bid := polled.branch.treeID, polled.branch.branchID
	timeout := execution.WorkflowTaskStartToCloseTimeout(workerInfo.WorkflowTaskTimeout)
	now := s.clock.Now()
	var result execution.WorkflowTaskStartResult
	// 选版、激活与 WorkflowTaskStarted 同一事务提交：两者要么都写入，要么都不写入。
	if err := s.telemetry.runInTx(ctx, "start_workflow_task", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.StartWorkflowTask(tx, execution.WorkflowTaskStart{
			RunID: lease.Task.RunID, TreeID: tid, BranchID: bid, ScheduledEventID: scheduledEventID,
			ExecutionGeneration: lease.Task.ExecutionGeneration, StickyGeneration: lease.Task.StickyGeneration,
			ActivationID:   lease.Task.ActivationID,
			WorkerIdentity: workerInfo.Identity, WorkerBuildID: workerInfo.BuildID,
			ProgramVersion: lease.Task.ProgramVersion, Candidate: polled.candidate,
			Timeout: timeout, Now: now,
		})
		return err
	}); err != nil {
		return protocol.TaskLease{}, false, err
	}
	if result.Choice.ConfigError != "" {
		// Run 已暂停（另一个事务暂停了它，或本次选版判定为配置错误）：这份投递不派发。
		if err := s.ackPausedWorkflowTask(ctx, lease); err != nil {
			return protocol.TaskLease{}, false, err
		}
		return protocol.TaskLease{}, false, nil
	}
	if result.Choice.Version != lease.Task.ProgramVersion {
		// 准备 lease 之后 Run 的策略或版本被另一个事务改过：放回队列，下一次领取按新的 Run 重新准备。
		if err := s.wfQueue.Nack(ctx, lease); err != nil {
			return protocol.TaskLease{}, false, err
		}
		return protocol.TaskLease{}, false, nil
	}
	if !result.Started {
		// 上面读到的 state 之后，这个 WFT 可能已被另一份投递完成：此时 Ack 丢弃这份投递。
		current, err := s.mgr.GetWorkflowExecutionState(lease.Task.RunID)
		if err != nil {
			return protocol.TaskLease{}, false, err
		}
		if current.IsTerminal() || !current.WorkflowTaskOpen(scheduledEventID) {
			return protocol.TaskLease{}, false, s.wfQueue.Ack(ctx, lease)
		}
		if current.CurrentActivationID == "" || current.CurrentActivationID == lease.Task.ActivationID {
			// 准备 lease 之后 sticky 或 execution 代数变了（例如 sticky 期限扫描清除了会话）：放回队列，下一次领取
			// 按当前代数重新准备。不确认也不放回时，这份投递要等队列的可见性超时（TaskQ 默认 60s）才再出现。
			return protocol.TaskLease{}, false, s.wfQueue.Nack(ctx, lease)
		}
		// 另一份投递正在执行这个 WFT：交给 Worker 只会在完成时被激活 fence 拒绝后重投。由 Server 确认
		// 丢弃这份投递；正在执行的那份若没有完成，由 Server 侧的 start-to-close 超时重新调度。
		if err := s.wfQueue.Ack(ctx, lease); err != nil {
			return protocol.TaskLease{}, false, err
		}
		return protocol.TaskLease{}, false, nil
	}
	s.armWorkflowTaskTimeout(lease.Task.RunID, scheduledEventID, now.Add(timeout))
	return lease, true, nil
}

func (s *Server) validateWorkflowTaskWorkerCapabilities(ctx context.Context, config definition.WorkflowConfigRef, lease protocol.TaskLease, workerInfo protocol.WorkerInfo) error {
	// 校验本次 lease 实际选中的内容版本，而不是 Run 的 current：AutoUpgrade 候选若需要
	// 更高 wlang capability，必须在派发前 Nack 等有能力 worker，不能放行后让 WFT 失败重试。
	if err := ctx.Err(); err != nil {
		return err
	}
	wf, err := s.workflowConfigs.GetWorkflowConfig(config)
	if err != nil {
		return err
	}
	if err := wf.Program.ValidateCapabilities(workerInfo.WorkflowCapabilities); err != nil {
		if nackErr := s.wfQueue.Nack(ctx, lease); nackErr != nil {
			return errors.Join(err, fmt.Errorf("requeue incompatible workflow task: %w", nackErr))
		}
		return err
	}
	return nil
}

func (s *Server) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	lease, err := s.actQueue.Dequeue(ctx, taskQueue)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	s.telemetry.recordScheduleToStart(ctx, lease.Task, s.clock.Now())
	return lease, nil
}

// ackPausedWorkflowTask 丢弃暂停 run 的 lease 前先释放 dispatch 记录。
// dispatch 记录处于 Dispatched（非租约）状态，不会自动过期；若只 Ack 不释放，
// set-policy 恢复复用的同一 open WFT 会在 AcquireTaskDispatch 被去重门控挡住，
// 恢复静默 no-op，直到进程重启的 force release 才恢复。
func (s *Server) ackPausedWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	if err := s.dispatches.ReleaseTaskDispatch(lease.Task, "run paused by config error"); err != nil {
		return err
	}
	return s.wfQueue.Ack(ctx, lease)
}

// AckWorkflowTask 确认 Worker 处理完的 WFT 投递，见 ackSettledLease。
func (s *Server) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	s.ackSettledLease(ctx, s.wfQueue, lease)
	return nil
}

func (s *Server) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return s.wfQueue.Nack(ctx, lease)
}

// AckActivityTask 确认 Worker 处理完的 Activity 投递，见 ackSettledLease。
func (s *Server) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	s.ackSettledLease(ctx, s.actQueue, lease)
	return nil
}

// ackSettledLease 确认一份结局已经落库的投递；队列的确认失败只记 WARN，不返回给 Worker。
//
// Worker 只在这两种时刻确认：Server 已记下这份投递的结局（Activity 的 attempt started、WFT 的完成或失败上报、
// Server 交还 WFT 时的 WorkflowTaskTimedOut），或 Server 按已落库的状态判定这份投递不执行（BeginActivityTask
// 的 Execute=false）。确认只是清理队列：没有送达时（TaskQ 分区换主期间重试用完、租约属于旧的 ownership epoch）
// 任务在可见性超时后重新投递，Server 按落库的状态丢弃它——Activity 由 BeginActivityTask 按 stamp、attempt 与
// started 判定不执行，WFT 由 startWorkflowTask 按 WFT 是否仍 open 与激活 fence 判定。确认失败不改变结局，Worker
// 也没有可做的补救，应答 5xx 只会让 Worker 记一条错误。
func (s *Server) ackSettledLease(ctx context.Context, queue interface {
	Ack(context.Context, protocol.TaskLease) error
}, lease protocol.TaskLease) {
	if err := queue.Ack(ctx, lease); err != nil {
		s.logger.Warn("task queue ack failed; the queue redelivers the task after its visibility timeout and the server drops the redelivery",
			"task_type", lease.Task.Type, "run_id", lease.Task.RunID, "scheduled_event_id", lease.Task.ScheduledEventID,
			"attempt", lease.Task.Attempt, "err", err)
	}
}

func (s *Server) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return s.actQueue.Nack(ctx, lease)
}

func (s *Server) runExists(runID string) bool {
	run, err := s.mgr.Get(runID)
	return err == nil && run != nil
}

func (s *Server) runNamespace(runID string) string {
	run, err := s.mgr.Get(runID)
	if err != nil || run == nil {
		return protocol.DefaultNamespace
	}
	return protocol.NormalizeNamespace(run.Namespace)
}

// validateWorkflowTaskToken 校验 ctx 里的 task token 属于 run 的这个 WFT；fence 按 states 读到的 execution state 判定。
func (s *Server) validateWorkflowTaskToken(ctx context.Context, run *execution.Run, scheduledEventID int64, states interface {
	GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error)
}) error {
	token := protocol.WorkflowTaskTokenFrom(ctx)
	if token == "" {
		return protocol.InvalidRequestf("workflow task token is required")
	}
	task, err := s.tokens.Decode(token)
	if err != nil {
		return err
	}
	if err := execution.ValidateWorkflowTaskToken(task, run.ID, scheduledEventID, states); err != nil {
		return err
	}
	return dispatch.ValidateTaskNamespace(task, run.Namespace)
}

// DispatchPendingTasks 是周期 reconciler 的入口：对每个 Running 的 run 做一次补偿（见 reconcileRun），
// 返回本轮补投的任务数。单个 run 失败计入返回的 error，不影响其余 run。
func (s *Server) DispatchPendingTasks(ctx context.Context) (dispatch.WorkflowDispatchResult, error) {
	result, err := s.reconcileRunningRuns(ctx, false)
	// 在引擎层记录而非调用方：HTTP 触发与 reconciler 周期补投都经由此处。
	s.telemetry.recordBacklog(ctx, result.WorkflowTasks, result.ActivityTasks)
	return result, err
}

func (s *Server) scheduleWorkflowTask(ctx context.Context, runID string) error {
	// ConfigError 暂停推进：等待人工修复后显式 set-policy 恢复。期间到达的
	// signal/activity 完成只留在 history，恢复后由新的 WFT 统一 replay。
	// UpgradeError 不阻塞调度，失败重试由 WFT 退避机制负责重调度。
	// Run 读一次：ConfigError 判定、History 分支与 namespace 都取自它（后两者在 Run 创建后不变）。
	current, err := s.mgr.Get(runID)
	if err != nil {
		return err
	}
	if current.ConfigError != "" {
		return nil
	}
	treeID, branchID, err := runBranchInfoFromRun(current)
	if err != nil {
		return err
	}
	event := history.NewWorkflowTaskScheduledEvent(protocol.DefaultWorkflowTaskTimeout.Milliseconds())
	event.WorkflowTaskScheduled.TraceContext = s.telemetry.inject(ctx)
	eventID, inserted, err := s.appendWorkflowTaskScheduled(runID, treeID, branchID, event)
	if err != nil {
		return err
	}
	traceContext := event.WorkflowTaskScheduled.TraceContext
	if !inserted {
		persisted, ok, err := s.mgr.GetWorkflowTaskScheduledEvent(runID, eventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("workflow task scheduled event %d missing for run %s", eventID, runID)
		}
		traceContext = persisted.WorkflowTaskScheduled.TraceContext
	}
	// dispatched=false 且 err=nil 是去重门控命中（该 WFT 已在队列或已派发），属正常路径，
	// 不记录。非 nil err 才是真故障：本函数被多处 post-commit 路径调用，改为返回错误会把
	// 已提交的状态变更误报为失败，且 reconciler 有兜底重投，故只记日志不改控制流。
	if _, err := s.dispatchWorkflowTask(ctx, protocol.Task{
		Namespace:        protocol.NormalizeNamespace(current.Namespace),
		Type:             protocol.TaskTypeWorkflow,
		RunID:            runID,
		ScheduledEventID: eventID,
		TraceContext:     cloneStringMap(traceContext),
	}); err != nil {
		s.logger.Error("workflow task dispatch failed; reconciler will redispatch",
			slog.String("run_id", runID),
			slog.Int64("scheduled_event_id", eventID),
			slog.Any("err", err),
		)
	}
	return nil
}

func (s *Server) scheduleWorkflowTaskIfRunning(ctx context.Context, runID string) error {
	status, err := s.GetRunStatus(runID)
	if err != nil {
		return err
	}
	if status != history.StatusRunning {
		return nil
	}
	return s.scheduleWorkflowTask(ctx, runID)
}

func (s *Server) dispatchWorkflowTask(ctx context.Context, task protocol.Task) (dispatched bool, err error) {
	task.Type = protocol.TaskTypeWorkflow
	if task.Namespace == "" {
		task.Namespace = s.runNamespace(task.RunID)
	}
	task = dispatch.NormalizeTask(task)
	startedAt := s.clock.Now()
	ctx, span := s.telemetry.start(ctx, "DispatchWorkflowTask", tenutotel.TaskSpanAttributes(task)...)
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		} else if !dispatched {
			outcome = "skipped"
		}
		s.telemetry.recordDispatch(ctx, task, outcome, s.clock.Now().Sub(startedAt))
		tenutotel.FinishSpan(span, err)
	}()
	return s.dispatcher.DispatchWorkflow(ctx, task)
}

func (s *Server) dispatchActivityTask(ctx context.Context, task protocol.Task) (dispatched bool, err error) {
	task.Type = protocol.TaskTypeActivity
	if task.Namespace == "" {
		task.Namespace = s.runNamespace(task.RunID)
	}
	task = dispatch.NormalizeTask(task)
	startedAt := s.clock.Now()
	ctx, span := s.telemetry.start(ctx, "DispatchActivityTask", tenutotel.TaskSpanAttributes(task)...)
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		} else if !dispatched {
			outcome = "skipped"
		}
		s.telemetry.recordDispatch(ctx, task, outcome, s.clock.Now().Sub(startedAt))
		tenutotel.FinishSpan(span, err)
	}()
	return s.dispatcher.DispatchActivity(ctx, task)
}

// redispatchActivityTask 先释放该 (run, scheduledEventID, attempt) 的旧 dispatch 租约再重派。
// dispatch key 含 attempt 但不含 stamp（task_dispatch.go），故 reset（attempt 归 1）与 reset 后的
// retry（attempt 复用旧值）会撞到 pre-reset 的同 attempt 租约——必须先 Release 否则 Acquire 失败、
// 任务永不入队。
func (s *Server) redispatchActivityTask(ctx context.Context, task protocol.Task) (bool, error) {
	_ = s.dispatches.ReleaseTaskDispatch(task, "redispatch")
	return s.dispatchActivityTask(ctx, task)
}
