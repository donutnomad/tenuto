package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RecoverRunningWorkflows 是启动恢复：对每个 Running 的 run 做一次 force 补偿。
func (s *Server) RecoverRunningWorkflows(ctx context.Context) error {
	_, err := s.reconcileRunningRuns(ctx, true)
	return err
}

// reconcileRunningRuns 逐个补偿全部 Running 的 run，供启动恢复与低频全量补偿（DispatchPendingTasks）使用。
// 单个 run 失败不影响其余 run，错误汇总返回，未完成的补偿留给下一轮；ctx 取消时停止。
//
// 周期补偿（Reconcile）只处理 ListRecoveryRunIDs 选出的有到期或待处理事项的 run；没有到期索引的崩溃窗口
// 由这里的全量扫描兜底：子 run 未建或终态未传播、终态未写回 Run、父关闭策略未做完、子 Run 的取消请求未发出。
// 接续 run（continue-as-new / 重试）与旧 run 的终态在同一个事务里提交，没有补建窗口。
//
// 只扫 Running 就能覆盖终态后续动作未完成的 run：History 已终态而后续动作未做完时，Run 的 Status 列
// 保持 Running（见 reconcileClosedRun）；已落终态的子 run 未传播给父 run 时，
// 由 Running 的父 run 凭 open ChildWorkflowState 补传播（见 recoverChildWorkflows）；父 run 已关闭而
// 父关闭策略没有做完时，由 Running 的子 run 凭父 run 的 execution state 与子关联补做（见 recoverParentClosePolicy）。
func (s *Server) reconcileRunningRuns(ctx context.Context, force bool) (dispatch.WorkflowDispatchResult, error) {
	var result dispatch.WorkflowDispatchResult
	var errs []error
	err := s.forEachRunningRunPage(ctx, func(runIDs []string) {
		for _, runID := range runIDs {
			if ctx.Err() != nil {
				return
			}
			dispatched, err := s.reconcileRun(ctx, runID, force)
			if err != nil {
				errs = append(errs, fmt.Errorf("reconcile run %s: %w", runID, err))
			}
			result.WorkflowTasks += dispatched.WorkflowTasks
			result.ActivityTasks += dispatched.ActivityTasks
		}
	})
	return result, errors.Join(append(errs, err)...)
}

// forEachRunningRunPage 按 (created_at, id) 游标逐页列出 Running 的 run 交给 visit，不做计数。游标位置与
// 处理无关：处理让 run 离开 Running 时后续页不漂移。ctx 取消时返回它的错误。
func (s *Server) forEachRunningRunPage(ctx context.Context, visit func(runIDs []string)) error {
	filter := execution.StoreListFilter{Statuses: []history.Status{history.StatusRunning}, Limit: runScanPageSize, SkipTotal: true}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := s.mgr.List(filter)
		if err != nil {
			return err
		}
		if len(page.Runs) == 0 {
			return nil
		}
		runIDs := make([]string, len(page.Runs))
		for i, run := range page.Runs {
			runIDs[i] = run.ID
		}
		visit(runIDs)
		cursor := execution.CursorOf(page.Runs[len(page.Runs)-1])
		filter.After = &cursor
		if len(page.Runs) < runScanPageSize {
			return ctx.Err()
		}
	}
}

// reconcileRun 把一个 run 的可恢复副作用补齐到与持久化状态一致，启动恢复与周期 reconciler 共用，
// 每一步都幂等。当前状态读 execution state；History 只在补齐已终态 run 的后续动作时读取。
//
// force=true 是启动恢复：进程内没有任何定时器，已过期的超时立即触发。队列不持久（Durable 为 false）时
// 队列里的任务随进程丢失，派发记录释放后重投；持久队列里的任务仍在，派发记录保持原状，
// 已派发的任务不再入队，租约过期或派发失败的记录由派发本身重新取得。
// force=false 是周期补偿与低频全量补偿：进程内定时器仍在，已过期的超时交给 FireOverdueTimers 按宽限期接管。
func (s *Server) reconcileRun(ctx context.Context, runID string, force bool) (dispatch.WorkflowDispatchResult, error) {
	var result dispatch.WorkflowDispatchResult
	run, err := s.mgr.Get(runID)
	if err != nil {
		return result, err
	}
	state, err := s.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		return result, err
	}
	if state.IsTerminal() {
		return result, s.reconcileClosedRun(ctx, run)
	}
	if run.Status != history.StatusRunning {
		return result, nil
	}
	// Run 与它的 WorkflowStarted 同事务写入。
	if !state.HasWorkflowStarted {
		return result, fmt.Errorf("run %s has no WorkflowStarted event", runID)
	}
	// 父 Run 已关闭而父关闭策略没有做完：先补做，被终止的子 Run 没有其余副作用要恢复。
	if terminated, err := s.recoverParentClosePolicy(ctx, run); err != nil || terminated {
		return result, err
	}
	switch execution.RecoveryActionFor(run, s.clock.Now()) {
	case execution.RunRecoveryWorkflowTimeout:
		if force {
			s.handleWorkflowTimeout(runID)
		}
		return result, nil
	case execution.RunRecoveryRunTimeout:
		if force {
			s.handleRunTimeout(runID)
		}
		return result, nil
	}
	if force {
		timed := run
		if run.DeadlineAt == nil || run.RunDeadlineAt == nil {
			restored, err := s.restoreRunDeadlines(runID)
			if err != nil {
				return result, err
			}
			if restored != nil {
				timed = restored
			}
		}
		s.armRunTimers(timed)
	}
	// 依赖 state 快照的两步紧跟在读取之后，缩短与并发 WFT 处理之间的窗口。
	if result.WorkflowTasks, err = s.recoverWorkflowTasks(ctx, run, state, force); err != nil {
		return result, err
	}
	s.restoreWorkflowTaskTimeout(runID, state, force)
	if err := s.recoverWorkflowActivation(ctx, runID, state, force); err != nil {
		return result, err
	}
	// 子 Workflow 的恢复失败（例如子 Run 启动失败）留给下一轮，本轮继续恢复这个 run 的 Activity 与 timer。
	childErr := s.recoverChildWorkflows(ctx, runID)
	if result.ActivityTasks, err = s.recoverActivityRetries(ctx, runID, force); err != nil {
		return result, errors.Join(childErr, err)
	}
	return result, errors.Join(childErr, s.recoverDurableTimers(ctx, runID, force))
}

// reconcileClosedRun 补齐 history 已终态的 run 的后续动作，最后才把终态写回 Run：
// Status 在此之前保持 Running，后续动作失败时下一轮扫描还能找到它。
// 终态事件的载荷（输出）取自 History。
func (s *Server) reconcileClosedRun(ctx context.Context, run *execution.Run) error {
	event, status, found, err := s.runtime.StatusEventFromHistory(run.ID)
	if err != nil {
		return err
	}
	if err := s.recoverClosedChildWorkflow(ctx, run, status); err != nil {
		return err
	}
	if !found {
		return nil
	}
	if history.IsTerminalStatus(status) {
		s.runClosed(run.ID)
	}
	return s.reconcileRunStatus(ctx, run.ID, event, status)
}

// reconcileRunStatus 在事务内按锁下读到的 Run 写回 History 里最后一个生命周期事件的状态。
func (s *Server) reconcileRunStatus(ctx context.Context, runID string, event history.Event, status history.Status) error {
	return s.telemetry.runInTx(ctx, "reconcile_run_status", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		return execution.ReconcileRunStatus(tx, run, event, status)
	})
}

// recoverWorkflowActivation 为「有新事实、没有 open WFT」的 run 补调度 WFT：
// 覆盖事件已提交、随后调度 WFT 失败的窗口（activity/timer/child 关闭、signal 等）。
func (s *Server) recoverWorkflowActivation(ctx context.Context, runID string, state *execution.WorkflowExecutionState, force bool) error {
	if state.NeedsWorkflowTask() {
		// 重试 run 的首个 WFT 由 RetryBackoff timer 到期时调度，退避期内不提前补调度。
		timers, err := s.mgr.ListTimerStates(runID)
		if err != nil {
			return err
		}
		if slices.ContainsFunc(timers, func(timer *execution.TimerState) bool {
			return timer.Kind == execution.TimerKindEnums.RetryBackoff && !timer.IsTerminal()
		}) {
			return nil
		}
		return s.scheduleWorkflowTask(ctx, runID)
	}
	// Block 后 run 处于「Running 且无 open WFT，末个 WFT lifecycle 是 WorkflowTaskFailed」，
	// 没有待处理的新事实。启动恢复在此按退避重挂延迟 timer；
	// 周期补偿时进程内 timer 仍在，退避到期后由 FireOverdueTimers 接管。
	if force && state.Blocked.Blocked {
		ctx = s.telemetry.extract(ctx, state.WorkflowTask.LastFailureTraceContext)
		s.scheduleWorkflowTaskAfter(ctx, runID, state.WorkflowTaskFailureBackoff(s.wftBackoffBase, s.wftBackoffCap))
	}
	return nil
}

// recoverChildWorkflows 崩溃恢复直读 ChildWorkflowState 求仍 open 的子关联（不扫父 history）。
// 双路径（保留现状语义）：
//
//	A. 子 Run 不存在（崩溃在建子 Run 前）→ 用 state 重建子 Run（startChildWorkflowRunWithRef）。
//	B. 子 Run 已存在且终态（崩溃在 propagate 前）→ recoverClosedChildWorkflow 补发父 close。
//
// 终态的 ChildWorkflowState 跳过（已 close，父已收到）。单个子关联失败不影响其余子关联，错误汇总返回。
func (s *Server) recoverChildWorkflows(ctx context.Context, parentRunID string) error {
	states, err := s.mgr.ListChildWorkflowStates(parentRunID)
	if err != nil {
		return err
	}
	var errs []error
	for _, st := range states {
		if err := s.recoverChildWorkflow(ctx, parentRunID, st); err != nil {
			errs = append(errs, fmt.Errorf("recover child workflow %s: %w", st.ChildID, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Server) recoverChildWorkflow(ctx context.Context, parentRunID string, st *execution.ChildWorkflowState) error {
	var child *execution.Run
	var childStatus history.Status
	var err error
	childExists := st.ChildRunID != "" && s.runExists(st.ChildRunID)
	if childExists {
		child, err = s.mgr.Get(st.ChildRunID)
		if err != nil {
			return err
		}
		childStatus, err = s.GetRunStatus(child.ID)
		if err != nil {
			return err
		}
	}
	switch execution.ChildRecoveryFor(st, childExists, childStatus) {
	case execution.ChildRecoverySkip:
		return nil
	case execution.ChildRecoveryPropagateClose:
		return s.recoverClosedChildWorkflow(ctx, child, childStatus)
	}
	// Run selected restart; engine resolves the pinned definition and starts it.
	configRef, err := s.workflowConfigRef(s.runNamespace(parentRunID), st.WorkflowType, st.WorkflowConfigVersion)
	if err != nil {
		return err
	}
	return s.startChildWorkflowRunWithRef(ctx, parentChildRunRequest{
		parentRunID:      parentRunID,
		childID:          st.ChildID,
		childRunID:       st.ChildRunID,
		scheduledEventID: st.ScheduledEventID,
		input:            st.Input,
		configRef:        configRef,
	})
}

func (s *Server) recoverClosedChildWorkflow(ctx context.Context, run *execution.Run, status history.Status) error {
	action, needed, err := s.runtime.ChildCloseRecoveryActionFor(run, status)
	if err != nil || !needed {
		return err
	}
	return s.propagateChildWorkflowClose(ctx, run.ID, run.ParentRunID, action.EventType, action.Output, action.Reason, action.Failure)
}

// recoverWorkflowTasks 重投 execution state 记录的 open WFT，trace context 取自它的调度事件。
func (s *Server) recoverWorkflowTasks(ctx context.Context, run *execution.Run, state *execution.WorkflowExecutionState, force bool) (int, error) {
	if state.OpenWorkflowTaskEventID == 0 {
		return 0, nil
	}
	task, err := s.openWorkflowTask(run, state.OpenWorkflowTaskEventID)
	if err != nil {
		return 0, err
	}
	if force && !s.wfQueue.Durable() {
		_ = s.dispatches.ReleaseTaskDispatch(task, "recovery")
	}
	dispatched, err := s.dispatchWorkflowTask(ctx, task)
	if err != nil || !dispatched {
		return 0, err
	}
	return 1, nil
}

// openWorkflowTask 构造 open WFT 的派发任务，trace context 取自它的调度事件。
func (s *Server) openWorkflowTask(run *execution.Run, scheduledEventID int64) (protocol.Task, error) {
	task := protocol.Task{
		Namespace:        protocol.NormalizeNamespace(run.Namespace),
		Type:             protocol.TaskTypeWorkflow,
		RunID:            run.ID,
		ScheduledEventID: scheduledEventID,
	}
	scheduled, ok, err := s.mgr.GetWorkflowTaskScheduledEvent(run.ID, scheduledEventID)
	if err != nil {
		return protocol.Task{}, err
	}
	if ok {
		task.TraceContext = cloneStringMap(scheduled.WorkflowTaskScheduled.TraceContext)
	}
	return task, nil
}

// recoverDurableTimers 崩溃恢复直读 TimerState 重建内存 timer 句柄（不扫历史、不需 workflow config）。
// 只重建 scheduled（非终态）的 timer；fired/cancelled 跳过。按 TimerState.Deadline 算剩余时长
// 重排（已过期的 deadline 立即到期）。
//
// fireOverdue 区分两条路径：启动恢复（true）立即触发已过期 timer；周期 reconciler（false）
// 只重建未来的 timer，已过期的交给 FireOverdueTimers 按宽限期接管。
func (s *Server) recoverDurableTimers(ctx context.Context, runID string, fireOverdue bool) error {
	states, err := s.mgr.ListTimerStates(runID)
	if err != nil {
		return err
	}
	for _, st := range states {
		if !execution.TimerRecoveryNeeded(st) {
			continue // 已 fired / cancelled，无需重建
		}
		if !fireOverdue && !s.clock.Now().Before(st.Deadline) {
			continue
		}
		s.startDurableTimer(runID, st.ScheduledEventID, st.Deadline)
	}
	return nil
}

// recoverActivityRetries 崩溃恢复直读 ActivityState 重建内存 timer 句柄（不扫历史、不需 workflow config）。
// 非终态的 state 重建超时 timer；Scheduled 的重派（携带 state 的 attempt+stamp），Started 的不重派
// （worker 可能仍在执行，靠超时 timer 兜底）。重试退避未到期的只按 NextAttemptAt 重挂退避 timer。
// force 为 true 表示启动恢复：队列不持久时释放旧派发记录，并立即触发已过期的超时；周期 reconciler 传 false，
// 已过期的超时交给 FireOverdueTimers 按宽限期接管。
func (s *Server) recoverActivityRetries(ctx context.Context, runID string, force bool) (int, error) {
	states, err := s.mgr.ListActivityStates(runID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, st := range states {
		disposition := execution.ActivityRecoveryFor(st, s.clock.Now())
		if disposition == execution.ActivityRecoverySkip {
			continue
		}
		s.armActivityTimeouts(st, force)
		if disposition == execution.ActivityRecoveryAwaitRetry {
			s.timers.Arm(activityTimerRef(timerActivityRetry, runID, st.ScheduledEventID), *st.NextAttemptAt)
			continue
		}

		if disposition == execution.ActivityRecoveryRedispatch {
			// 未 started 或 reset/retry 后待派发：重新入队（携带 state 的 attempt+stamp），
			// 待到期的重试退避 timer 由这次派发取代。
			s.timers.Stop(activityTimerRef(timerActivityRetry, runID, st.ScheduledEventID))
			if force && !s.actQueue.Durable() {
				_ = s.dispatches.ReleaseTaskDispatch(s.activityTaskFromState(st), "recovery")
			}
			dispatched, err := s.dispatchActivityTask(ctx, s.activityTaskFromState(st))
			if err != nil {
				return 0, err
			}
			if dispatched {
				count++
			}
		}
	}
	return count, nil
}
