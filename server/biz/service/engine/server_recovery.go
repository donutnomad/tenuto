package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RecoverRunningWorkflows 是启动恢复：对 ListRecoveryRunIDs(now+lookahead) 选出的 Run 做 force 补偿（补投任务、
// 把 lookahead 内到期的定时器挂到本进程、立即触发已过期的超时），再做一轮全量兜底（reconcileRunningRuns）。
// 代价与到期或待处理的事项数、Running Run 的页数成正比，不逐个读空闲的 Run。截止时间、timer 与 Activity 超时
// 晚于 lookahead 的，由周期补偿（Reconcile）在进入 lookahead 时挂上。Run 缺少配置声明的截止时间时（升级到声明
// 超时的 Program）只有候选 Run 在这里补写（restoreRunDeadlines）；与 Temporal 相同，执行超时在 Run 启动时确定，
// 升级不回填空闲 Run。
func (s *Server) RecoverRunningWorkflows(ctx context.Context, lookahead time.Duration) error {
	_, err := s.reconcileCandidates(ctx, s.clock.Now().Add(lookahead), true)
	return errors.Join(err, s.reconcileRunningRuns(ctx))
}

// reconcileCandidates 对 ListRecoveryRunIDs(horizon) 选出的 Run 逐个做 reconcileRun。单个 Run 失败不影响其余 Run，
// 错误汇总返回；ctx 取消时停止。
func (s *Server) reconcileCandidates(ctx context.Context, horizon time.Time, force bool) (dispatch.WorkflowDispatchResult, error) {
	var result dispatch.WorkflowDispatchResult
	runIDs, err := s.mgr.ListRecoveryRunIDs(horizon)
	errs := []error{err}
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		dispatched, err := s.reconcileRun(ctx, runID, force, horizon)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile run %s: %w", runID, err))
		}
		result.WorkflowTasks += dispatched.WorkflowTasks
		result.ActivityTasks += dispatched.ActivityTasks
	}
	return result, errors.Join(errs...)
}

// reconcileRunningRuns 是全量兜底，供启动恢复与低频全量补偿（DispatchPendingTasks）使用：按游标逐页检查全部
// Running Run，只处理没有到期索引的崩溃窗口。有到期索引的事项（截止时间、timer、Activity、open 或待调度的 WFT、
// blocked）由 reconcileCandidates 处理。
//
//   - History 已终态而后续动作没做完：Run 的 Status 列保持 Running（见 reconcileClosedRun），execution state 已终态。
//   - 父 Run 已关闭而父关闭策略没有做完、父 Run 已请求取消而子 Run 的取消请求没有发出：由 Running 的子 Run 凭父 Run
//     的 execution state 与子关联补做（见 recoverParentLink）。
//   - 子 Run 未建、已落终态的子 Run 未传播给父 Run：由 Running 的父 Run 凭 open ChildWorkflowState 补做（见
//     recoverChildWorkflows）。
//
// 每页是固定的几条批量读取（Run 列表、页内 execution state、父 Run 的 execution state 与子关联、open 子关联与子 Run
// 的 execution state），与页内 Run 有没有待处理事项无关；只有需要补做的 Run 才逐个处理。接续 run（continue-as-new /
// 重试）与旧 run 的终态在同一个事务里提交，没有补建窗口。单个 run 失败不影响其余 run，错误汇总返回，未完成的补偿
// 留给下一轮；ctx 取消时停止。
func (s *Server) reconcileRunningRuns(ctx context.Context) error {
	var errs []error
	err := s.forEachRunningRunPage(ctx, func(runs []*execution.Run) {
		errs = append(errs, s.reconcileRunPage(ctx, runs))
	})
	return errors.Join(append(errs, err)...)
}

// reconcileRunPage 是 reconcileRunningRuns 的一页。
func (s *Server) reconcileRunPage(ctx context.Context, runs []*execution.Run) error {
	runIDs := make([]string, len(runs))
	for i, run := range runs {
		runIDs[i] = run.ID
	}
	states, err := s.mgr.ListWorkflowExecutionStates(runIDs)
	if err != nil {
		return err
	}
	var errs []error
	open := make([]*execution.Run, 0, len(runs))
	for _, run := range runs {
		state := states[run.ID]
		switch {
		case state == nil:
			errs = append(errs, fmt.Errorf("reconcile run %s: %w", run.ID, execution.ErrRunNotFound))
		case state.IsTerminal():
			if err := s.reconcileClosedRun(ctx, run); err != nil {
				errs = append(errs, fmt.Errorf("reconcile run %s: %w", run.ID, err))
			}
		case !state.HasWorkflowStarted:
			// Run 与它的 WorkflowStarted 同事务写入。
			errs = append(errs, fmt.Errorf("reconcile run %s: run has no WorkflowStarted event", run.ID))
		default:
			open = append(open, run)
		}
	}
	open, err = s.recoverParentLinks(ctx, open, states)
	errs = append(errs, err)
	openIDs := make([]string, len(open))
	for i, run := range open {
		openIDs[i] = run.ID
	}
	return errors.Join(append(errs, s.recoverChildWorkflows(ctx, openIDs...))...)
}

// recoverParentLinks 对 runs 里的子 Run 补做父侧处理（见 recoverParentLink），父 Run 的 execution state 与子关联
// 各一条批量读取；states 是 runs 自己的 execution state。返回没有被父关闭策略终止的 Run。父 Run 已被保留期删除时
// 子关联随之删除，不再处理。
func (s *Server) recoverParentLinks(ctx context.Context, runs []*execution.Run, states map[string]*execution.WorkflowExecutionState) ([]*execution.Run, error) {
	var parentIDs []string
	var keys []execution.ChildWorkflowKey
	for _, run := range runs {
		if run.ParentRunID != "" && run.ParentScheduledEventID != 0 {
			parentIDs = append(parentIDs, run.ParentRunID)
			keys = append(keys, execution.ChildWorkflowKey{RunID: run.ParentRunID, ScheduledEventID: run.ParentScheduledEventID})
		}
	}
	if len(keys) == 0 {
		return runs, nil
	}
	parents, err := s.mgr.ListWorkflowExecutionStates(parentIDs)
	if err != nil {
		return runs, err
	}
	links, err := s.mgr.GetChildWorkflowStates(keys)
	if err != nil {
		return runs, err
	}
	var errs []error
	remaining := make([]*execution.Run, 0, len(runs))
	for _, run := range runs {
		parent := parents[run.ParentRunID]
		link := links[execution.ChildWorkflowKey{RunID: run.ParentRunID, ScheduledEventID: run.ParentScheduledEventID}]
		if parent == nil || link == nil {
			remaining = append(remaining, run)
			continue
		}
		terminated, err := s.recoverParentLink(ctx, run, states[run.ID], parent, link)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile run %s: %w", run.ID, err))
		}
		if !terminated {
			remaining = append(remaining, run)
		}
	}
	return remaining, errors.Join(errs...)
}

// forEachRunningRunPage 按 (created_at, id) 游标逐页列出 Running 的 run 交给 visit，不做计数。游标位置与
// 处理无关：处理让 run 离开 Running 时后续页不漂移。ctx 取消时返回它的错误。
func (s *Server) forEachRunningRunPage(ctx context.Context, visit func(runs []*execution.Run)) error {
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
		visit(page.Runs)
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
// force=false 是周期补偿与 DispatchPendingTasks：进程内定时器仍在，已过期的超时交给 FireOverdueTimers 按宽限期接管。
// 两者都只把 horizon 之前到期的定时器（Run 截止时间、durable timer、Activity 超时）挂到本进程，更晚的
// 由之后某一轮周期补偿在进入 lookahead 时挂上。
func (s *Server) reconcileRun(ctx context.Context, runID string, force bool, horizon time.Time) (dispatch.WorkflowDispatchResult, error) {
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
	timed := run
	if force && (run.DeadlineAt == nil || run.RunDeadlineAt == nil) {
		restored, err := s.restoreRunDeadlines(runID)
		if err != nil {
			return result, err
		}
		if restored != nil {
			timed = restored
		}
	}
	s.recoverRunTimers(timed, horizon, force)
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
	if result.ActivityTasks, err = s.recoverActivityRetries(ctx, runID, force, horizon); err != nil {
		return result, errors.Join(childErr, err)
	}
	return result, errors.Join(childErr, s.recoverDurableTimers(ctx, runID, force, horizon))
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
	return s.runInTx(ctx, "reconcile_run_status", func(tx execution.ExecutionTx) error {
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
	retryAt, blocked := state.BlockedRetryAt(s.wftBackoffBase, s.wftBackoffCap)
	if state.NeedsWorkflowTask() && (!blocked || !retryAt.After(s.clock.Now())) {
		// 重试 run 的首个 WFT 由 RetryBackoff timer 到期时调度，退避期内不提前补调度。
		if pending, err := s.pendingOnWorkflowTaskBackoff(state); err != nil || pending {
			return err
		}
		return s.scheduleWorkflowTask(ctx, runID)
	}
	// Block 后 run 处于「Running 且无 open WFT，末个 WFT lifecycle 是 WorkflowTaskFailed」，退避期内到达的事实
	// 也等这次重试。启动恢复在此按退避重挂延迟 timer；
	// 周期补偿时进程内 timer 仍在，退避到期后由 FireOverdueTimers 接管。
	if force && blocked {
		ctx = s.telemetry.extract(ctx, state.WorkflowTask.LastFailureTraceContext)
		s.scheduleWorkflowTaskAfter(ctx, runID, state.WorkflowTaskFailureBackoff(s.wftBackoffBase, s.wftBackoffCap))
	}
	return nil
}

// recoverChildWorkflows 崩溃恢复直读 ChildWorkflowState 求这些父 Run 仍 open 的子关联（不扫父 history），子 Run 的
// 存在与状态取自它的 execution state（与 Run 同事务创建）。无论几个父 Run 都是两条批量读取：
//
//	A. 子 Run 不存在（崩溃在建子 Run 前）→ 用 state 重建子 Run（startChildWorkflowRunWithRef）。
//	B. 子 Run 已存在且终态（崩溃在 propagate 前）→ recoverClosedChildWorkflow 补发父 close。
//
// 终态的 ChildWorkflowState 跳过（已 close，父已收到）。单个子关联失败不影响其余子关联，错误汇总返回。
func (s *Server) recoverChildWorkflows(ctx context.Context, parentRunIDs ...string) error {
	links, err := s.mgr.ListOpenChildWorkflowStates(parentRunIDs)
	if err != nil || len(links) == 0 {
		return err
	}
	childRunIDs := make([]string, 0, len(links))
	for _, link := range links {
		if link.ChildRunID != "" {
			childRunIDs = append(childRunIDs, link.ChildRunID)
		}
	}
	children, err := s.mgr.ListWorkflowExecutionStates(childRunIDs)
	if err != nil {
		return err
	}
	var errs []error
	for _, link := range links {
		if err := s.recoverChildWorkflow(ctx, link, children[link.ChildRunID]); err != nil {
			errs = append(errs, fmt.Errorf("recover child workflow %s of run %s: %w", link.ChildID, link.RunID, err))
		}
	}
	return errors.Join(errs...)
}

// recoverChildWorkflow 恢复一个子关联；child 是子 Run 的 execution state，子 Run 不存在时为 nil。
func (s *Server) recoverChildWorkflow(ctx context.Context, st *execution.ChildWorkflowState, child *execution.WorkflowExecutionState) error {
	var childStatus history.Status
	if child != nil {
		childStatus = child.Status
	}
	switch execution.ChildRecoveryFor(st, child != nil, childStatus) {
	case execution.ChildRecoverySkip:
		return nil
	case execution.ChildRecoveryPropagateClose:
		// continue-as-new 的子 Run 由接续 Run 的终态传播，不读 Run。
		if childStatus == history.StatusContinuedAsNew {
			return nil
		}
		run, err := s.mgr.Get(st.ChildRunID)
		if err != nil {
			return err
		}
		return s.recoverClosedChildWorkflow(ctx, run, childStatus)
	}
	// Run selected restart; engine resolves the pinned definition and starts it.
	configRef, err := s.workflowConfigRef(s.runNamespace(st.RunID), st.WorkflowType, st.WorkflowConfigVersion)
	if err != nil {
		return err
	}
	return s.startChildWorkflowRunWithRef(ctx, parentChildRunRequest{
		parentRunID:      st.RunID,
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
// 只重建未来的 timer，已过期的交给 FireOverdueTimers 按宽限期接管。两者都只重建 horizon 之前到期的 timer。
func (s *Server) recoverDurableTimers(ctx context.Context, runID string, fireOverdue bool, horizon time.Time) error {
	states, err := s.mgr.ListTimerStates(runID)
	if err != nil {
		return err
	}
	for _, st := range states {
		if !execution.TimerRecoveryNeeded(st) {
			continue // 已 fired / cancelled，无需重建
		}
		if !s.recoveryArmable(st.Deadline, horizon, fireOverdue) {
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
// 已过期的超时交给 FireOverdueTimers 按宽限期接管。超时只挂 horizon 之前到期的。
func (s *Server) recoverActivityRetries(ctx context.Context, runID string, force bool, horizon time.Time) (int, error) {
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
		for kind, reason := range activityTimeoutKinds {
			if deadline, ok := execution.ActivityTimeoutDeadline(st, reason); ok && s.recoveryArmable(deadline, horizon, force) {
				s.timers.Arm(activityTimerRef(kind, runID, st.ScheduledEventID), deadline)
			}
		}
		if disposition == execution.ActivityRecoveryAwaitRetry {
			// Scheduled 的 Activity 每轮都是候选（读它不省查询），重试退避不受 horizon 限制。
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
