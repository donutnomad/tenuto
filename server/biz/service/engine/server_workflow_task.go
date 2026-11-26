package engine

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"go.opentelemetry.io/otel/attribute"
)

func (s *Server) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) (err error) {
	ctx, span := s.telemetry.start(ctx, "CompleteWorkflowTask",
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.Int64(tenutotel.AttrScheduledEventID, scheduledEventID),
	)
	defer func() { tenutotel.FinishSpan(span, err) }()
	// WFT 处理内部的 retry/CAN 派生 Run 随 WFT 事务提交，子 Run 走不拿 keyed lock 的 createStartedRun，
	// 故同 workflowID 锁不会重入。
	open, release, err := s.lockOpenWorkflowTask(ctx, runID, scheduledEventID)
	if err != nil {
		return err
	}
	defer release()
	span.SetAttributes(workflowSpanAttributes(open.ref.Namespace, open.ref.WorkflowID, open.ref.ID, open.ref.WorkflowType)...)
	if open.scheduledEventID == 0 {
		return nil
	}
	return s.completeWorkflowTaskWithStore(ctx, open, commands, workerIdentity)
}

// openWorkflowTask 是 WFT 关闭入口在 keyed lock 下解析出的 open WFT。ref 与 branch 取自取锁之前读到的 Run 创建后
// 不变的字段；state 是取锁之后读到的 execution state，token 的 fence、open 判定与 started 时选中的 Program 版本都取自
// 这一次读取。scheduledEventID 为 0 表示 run 已终态或该 WFT 已关闭。
type openWorkflowTask struct {
	ref              runRef
	branch           runBranch
	state            *execution.WorkflowExecutionState
	scheduledEventID int64
}

// programVersion 返回本次 Workflow Task 在 started 时选中的 Program 版本。
func (o openWorkflowTask) programVersion() string {
	return o.state.WorkflowTaskProgramVersion(o.scheduledEventID)
}

// lockOpenWorkflowTask 是三个 WFT 关闭入口的公共前段：拿 run 所属 workflow 的 keyed lock
// （按 (namespace, workflowID) 串行化同 workflow 的 WFT 处理），校验 task token，
// 解析要关闭的 WorkflowTaskScheduled 事件 ID。err 为 nil 时调用方负责 release。
//
// Run 与 execution state 各读一次：Run 只取不变的字段；取锁之后的 token 校验、open 判定与选中版本此前各读一次
// execution state，三次读取之间没有本次请求的写入，合成一次等价于三次读取之间恰好没有并发写入。
func (s *Server) lockOpenWorkflowTask(ctx context.Context, runID string, scheduledEventID int64) (openWorkflowTask, func(), error) {
	run, err := s.mgr.Get(runID)
	if err != nil {
		return openWorkflowTask{}, nil, err
	}
	release, err := s.keyedMu.Lock(ctx, workflowLockKey(run.Namespace, run.WorkflowID))
	if err != nil {
		return openWorkflowTask{}, nil, err
	}
	open, err := s.resolveOpenWorkflowTask(ctx, run, scheduledEventID)
	if err != nil {
		release()
		return openWorkflowTask{}, nil, err
	}
	return open, release, nil
}

// resolveOpenWorkflowTask 校验 task token 并读 execution state 解析本次要关闭的 open WFT。
func (s *Server) resolveOpenWorkflowTask(ctx context.Context, run *execution.Run, scheduledEventID int64) (openWorkflowTask, error) {
	ref := refOf(run)
	open := openWorkflowTask{ref: ref, branch: ref.branch}
	states := &onceStateReader{mgr: s.mgr}
	if err := s.validateWorkflowTaskToken(ctx, run, scheduledEventID, states); err != nil {
		return openWorkflowTask{}, err
	}
	state, err := states.GetWorkflowExecutionState(run.ID)
	if err != nil {
		return openWorkflowTask{}, err
	}
	open.state = state
	if !state.IsTerminal() && state.WorkflowTaskOpen(scheduledEventID) {
		open.scheduledEventID = scheduledEventID
	}
	return open, nil
}

// onceStateReader 第一次调用时读 execution state，之后返回同一份结果。
type onceStateReader struct {
	mgr   execution.ExecutionManager
	state *execution.WorkflowExecutionState
}

func (r *onceStateReader) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	if r.state != nil {
		return r.state, nil
	}
	state, err := r.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		return nil, err
	}
	r.state = state
	return state, nil
}

func (s *Server) completeWorkflowTaskWithStore(ctx context.Context, open openWorkflowTask, commands []history.Command, workerIdentity string) error {
	runID, workflowTaskScheduledID := open.ref.ID, open.scheduledEventID
	traceContext := s.telemetry.inject(ctx)
	programVersion := open.programVersion()
	prepared, err := execution.PrepareWorkflowTask(runID, commands, traceContext, workflowCommandResolver{server: s, programVersion: programVersion})
	if errors.Is(err, dispatch.ErrActivityRouteUnavailable) {
		// Activity Worker 尚未上线：关闭本次 WFT 并退避重试，Run 显示为 Blocked。
		return s.recordWorkflowTaskFailure(ctx, runID, workflowTaskScheduledID, WorkflowTaskFailedCauseActivityRouteUnavailable, err.Error())
	}
	if cause, ok := execution.BadCommandAttributesCause(err); ok {
		// Workflow 代码的确定性错误：关闭本次 WFT 并退避重试，Run 显示为 Blocked，修复代码后恢复。
		return s.recordWorkflowTaskFailure(ctx, runID, workflowTaskScheduledID, cause, err.Error())
	}
	if err != nil {
		return err
	}
	commands = prepared.Commands
	commandEvents := prepared.CommandEvents

	if open.branch.err != nil {
		return open.branch.err
	}
	treeID, branchID := open.branch.treeID, open.branch.branchID
	closeEvent := history.NewWorkflowTaskCompletedEvent(workflowTaskScheduledID, workerIdentity)

	// 终态命令派生的接续 Run 在事务外备好，与终态事件、旧 Run 的终态状态、current 切换同事务提交：
	// History 写下终态之后，同 Workflow ID 的启动、Signal 与 Update 看到的只有接续 Run。
	//   - CmdContinueAsNew：接续 Run 经 execution.CommitContinuedRun 提交；
	//   - CmdFailWorkflow：没有命令事件，重试还是终结在提交前决定；重试 run 与它的 backoff timer 经
	//     execution.CommitRetryRun 提交，不重试时 Run 的 Failed 状态随 WorkflowFailed 提交。
	var continued *startPlan
	if i := slices.IndexFunc(commands, func(command history.Command) bool { return command.Type == history.CmdContinueAsNew }); i >= 0 {
		if continued, err = s.prepareContinuedRun(runID, prepared.ContinuedRunIDs[i], commands[i].Input, definition.WorkflowConfigRef(prepared.ContinuedRefs[i])); err != nil {
			return err
		}
	}
	var workflowFailed *history.Event
	var retry *retryRunStart
	if i := slices.IndexFunc(commands, func(command history.Command) bool { return command.Type == history.CmdFailWorkflow }); i >= 0 {
		failed, retryRunID, err := s.prepareWorkflowFailure(runID, workflowFailure(commands[i]), "")
		if err != nil {
			return err
		}
		workflowFailed = &failed
		if retryRunID != "" {
			start, err := s.prepareRetryRun(ctx, runID, retryRunID)
			if err != nil {
				return err
			}
			retry = &start
		}
	}

	now := s.clock.Now()
	var (
		run                    *execution.Run
		promoted               bool
		result                 execution.WorkflowTaskCompletionResult
		continuedInserted      bool
		workflowFailedInserted bool
	)
	if txErr := s.telemetry.runInTx(ctx, "complete_workflow_task", s.mgr, func(tx execution.ExecutionTx) error {
		// 死锁重放时上一次尝试的结果作废。
		run, promoted, result, continuedInserted, workflowFailedInserted = nil, false, execution.WorkflowTaskCompletionResult{}, false, false
		// 锁下读取 Run：提交用它把 current 提升到本次采用的 Program 版本，终态命令在它上面关闭 Run。
		var err error
		if run, err = tx.GetRunForUpdate(runID); err != nil {
			return err
		}
		promoted = programVersion != "" && programVersion != run.WorkflowConfigVersion
		result, err = execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
			Run: run, TreeID: treeID, BranchID: branchID,
			CloseEvent: closeEvent, CommandEvents: commandEvents, Commands: commands,
			ChildTargets: prepared.ChildTargets, TraceContext: traceContext,
			SeenThroughEventID: activatedThroughEventID(ctx),
			ProgramVersion:     programVersion, Now: now,
		})
		if err != nil || !result.ClosedInserted {
			return err
		}
		if continued != nil {
			if continuedInserted, err = continueAsNewInserted(commands, result); err != nil || !continuedInserted {
				return err
			}
			return execution.CommitContinuedRun(tx, runID, continued.run, continued.treeID, continued.branchID, continued.started)
		}
		if workflowFailed == nil {
			return nil
		}
		workflowFailedInserted, err = execution.AppendWorkflowFailed(tx, run, treeID, branchID, *workflowFailed)
		if err != nil || !workflowFailedInserted || retry == nil {
			return err
		}
		_, err = execution.CommitRetryRun(tx, retry.commit)
		return err
	}); txErr != nil {
		var limitErr *history.HistoryLimitExceededError
		if errors.As(txErr, &limitErr) {
			return s.closeWorkflowForHistoryLimit(ctx, runID, treeID, branchID, closeEvent, limitErr)
		}
		if errors.Is(txErr, execution.ErrScheduleActivityDuplicateID) {
			// 与 BadCommandAttributesCause 同类的确定性错误：关闭本次 WFT 并退避重试，Run 显示为 Blocked。
			return s.recordWorkflowTaskFailure(ctx, runID, workflowTaskScheduledID, execution.WorkflowTaskFailedCauseScheduleActivityDuplicateID, txErr.Error())
		}
		return txErr
	}
	if result.Superseded {
		// started 之后有新事实：本次完成没有提交，立即重做。调度失败由 reconcileRun 按 ActivationPending 补上。
		s.stopWorkflowTaskTimeout(runID, workflowTaskScheduledID)
		return s.scheduleWorkflowTaskIfRunning(context.WithoutCancel(ctx), runID)
	}
	if !result.ClosedInserted {
		return nil
	}
	s.stopWorkflowTaskTimeout(runID, workflowTaskScheduledID)
	// current 的提升与 UpgradeError 的清除已随 WFT 完成同事务提交。
	if promoted {
		s.logger.Info("workflow content upgraded at workflow task boundary",
			slog.String("run_id", runID),
			slog.String("from_version", run.OriginalWorkflowConfigVersion),
			slog.String("to_version", programVersion),
			slog.String("policy", run.Policy),
			slog.Int64("scheduled_event_id", workflowTaskScheduledID),
		)
	}
	// 提交后的动作（派发 Activity、挂 timer、唤醒 Update 等待方、终态的后续动作）先于激活回执执行：
	// 回执出错只让 Worker 丢弃本地缓存，已提交的决策照常推进。
	if err := s.runWorkflowTaskPostCommit(context.WithoutCancel(ctx), run, commands, result, prepared, traceContext, workflowFailedInserted, continued, retry, now); err != nil {
		return err
	}
	return s.captureWorkflowTaskCompletion(ctx, result, closeEvent, commandEvents, commands)
}

// continueAsNewInserted 报告本次提交是否插入了 CmdContinueAsNew 的 WorkflowContinuedAsNew 事件。终态事件先到先得，
// 同一批里更早的终态命令或别处已写入的终态让它不插入。
func continueAsNewInserted(commands []history.Command, result execution.WorkflowTaskCompletionResult) (bool, error) {
	actions, err := execution.WorkflowTaskActions(commands, result)
	if err != nil {
		return false, err
	}
	for _, action := range actions {
		if action.Command.Type == history.CmdContinueAsNew {
			return action.Event.Inserted, nil
		}
	}
	return false, nil
}

// runWorkflowTaskPostCommit 执行已提交的 WFT 决策的事务外动作。每一步失败都可由 reconcileRun 按持久化状态补做。
func (s *Server) runWorkflowTaskPostCommit(
	postCommitCtx context.Context,
	run *execution.Run,
	commands []history.Command,
	result execution.WorkflowTaskCompletionResult,
	prepared execution.PreparedWorkflowTask,
	traceContext map[string]string,
	workflowFailedInserted bool,
	continued *startPlan,
	retry *retryRunStart,
	now time.Time,
) error {
	runID := run.ID
	actions, err := execution.WorkflowTaskActions(commands, result)
	if err != nil {
		return err
	}
	scheduleNextWorkflowTask := false
	// 取消时尚未开始的 Activity 已随提交以 ActivityCancelled 结束：停掉它们的 timer，新的事实需要一次 Workflow Task。
	for _, scheduledEventID := range result.CancelledActivities {
		s.stopActivityTimers(runID, scheduledEventID)
		scheduleNextWorkflowTask = true
	}
	for _, action := range actions {
		cmd := action.Command
		eventResult := action.Event
		switch cmd.Type {
		case history.CmdScheduleActivity:
			if !eventResult.Inserted {
				continue
			}
			task := protocol.Task{
				Namespace:              protocol.NormalizeNamespace(run.Namespace),
				Type:                   protocol.TaskTypeActivity,
				WorkflowID:             run.WorkflowID,
				WorkflowType:           run.WorkflowType,
				RunID:                  runID,
				ScheduledTime:          now,
				ActivityID:             cmd.ActivityID,
				ScheduledEventID:       eventResult.EventID,
				Attempt:                1,
				Stamp:                  1, // 与 schedule 时落库的 ActivityState.Stamp 一致，worker start 时 stamp 校验通过
				ActivityType:           cmd.ActivityType,
				TaskQueue:              dispatch.NormalizeActivityTaskQueue(cmd.TaskQueue),
				TargetWorkerIdentity:   cmd.TargetWorkerIdentity,
				Input:                  cmd.Input,
				ScheduleToStartTimeout: cmd.ScheduleToStartTimeout,
				StartToCloseTimeout:    cmd.StartToCloseTimeout,
				ScheduleToCloseTimeout: cmd.ScheduleToCloseTimeout,
				HeartbeatTimeout:       cmd.HeartbeatTimeout,
				TraceContext:           cloneStringMap(traceContext),
			}
			s.startActivityTimers(runID, cmd, eventResult.EventID, now)
			// post-commit：事务已提交、ActivityState 已落库，失败由 reconciler
			// (DispatchPendingTasks) 重投，状态可恢复，故不返回错误（返回会把已提交的
			// WFT 完成误报为失败）。但完全静默会掩盖队列故障，故记日志。
			if _, err := s.dispatchActivityTask(postCommitCtx, task); err != nil {
				s.logger.Error("post-commit activity dispatch failed; reconciler will redispatch",
					slog.String("run_id", runID),
					slog.Int64("scheduled_event_id", eventResult.EventID),
					slog.String("activity_type", cmd.ActivityType),
					slog.Any("err", err),
				)
			}
		case history.CmdScheduleTimer:
			if !eventResult.Inserted {
				continue
			}
			s.startDurableTimer(runID, eventResult.EventID, now.Add(cmd.Delay))
		case history.CmdCancelTimer:
			if eventResult.Inserted {
				s.stopDurableTimer(runID, prepared.CommandEvents[action.EventIndex].TimerCancelled.ScheduledEventID)
			}
		case history.CmdRequestCancelChildWorkflow:
			if !eventResult.Inserted {
				continue
			}
			// 子 Run 的取消请求在提交后发出；失败时子 Run 的 reconcileRun 按 ChildWorkflowState.CancelRequested 补发。
			requested := prepared.CommandEvents[action.EventIndex].ChildWorkflowCancelRequested
			if err := s.requestChildCancellation(postCommitCtx, runID, requested.ScheduledEventID); err != nil {
				s.logger.Error("post-commit child workflow cancel request failed; reconciler will retry it",
					slog.String("run_id", runID),
					slog.String("child_id", requested.ChildID),
					slog.Any("err", err),
				)
			}
		case history.CmdRecordMarker:
			if eventResult.Inserted {
				scheduleNextWorkflowTask = true
			}
		case history.CmdStartChildWorkflow:
			if !eventResult.Inserted {
				continue
			}
			if err := s.startChildWorkflowRunWithRef(postCommitCtx, parentChildRunRequest{
				parentRunID:      runID,
				childID:          cmd.ChildID,
				childRunID:       prepared.ChildTargets[action.Index].RunID,
				scheduledEventID: eventResult.EventID,
				input:            cmd.Input,
				configRef:        definition.WorkflowConfigRef(prepared.ChildRefs[action.Index]),
			}); err != nil {
				// post-commit：ChildWorkflowState 已落库，reconcileRun 会按它补建子 run；
				// 这里继续处理其余命令。
				s.logger.Error("post-commit child workflow start failed; reconciler will restart it",
					slog.String("run_id", runID),
					slog.Int64("scheduled_event_id", eventResult.EventID),
					slog.String("child_id", cmd.ChildID),
					slog.Any("err", err),
				)
			}
		case history.CmdContinueAsNew:
			// 终态事件先到先得；未插入表示终态已由别处写入，由写入方落状态。接续 Run 已随终态事件提交。
			if !eventResult.Inserted {
				return nil
			}
			err := s.activateStartedRun(postCommitCtx, continued.run)
			s.runClosed(runID)
			return err
		case history.CmdCompleteWorkflowUpdate:
			if eventResult.Inserted {
				scheduleNextWorkflowTask = true
				// 事务已提交、completed 事件已落库：唤醒本副本上等待这个 Run 的调用方，它们从 History 读取结果。
				// 仍在 keyed lock 内，但 Notify 只争 runWatch 自己的锁，不阻塞。
				s.runWatch.Notify(runID)
			}
		case history.CmdCompleteWorkflow:
			if !eventResult.Inserted {
				return nil
			}
			return s.workflowCompletedAfterCommit(run, cmd.Output)
		case history.CmdFailWorkflow:
			// WorkflowFailed 已随 WFT 完成同事务落库；未插入表示终态已由别处写入。
			if !workflowFailedInserted {
				return nil
			}
			if retry != nil {
				s.activateRetryRun(*retry)
				return nil
			}
			return s.workflowFailedAfterCommit(run, workflowFailure(cmd))
		case history.CmdCancelWorkflow:
			if !eventResult.Inserted {
				return nil
			}
			return s.workflowCancelledAfterCommit(run)
		}
	}
	if scheduleNextWorkflowTask {
		return s.scheduleWorkflowTaskIfRunning(postCommitCtx, runID)
	}
	return nil
}

func (s *Server) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	open, release, err := s.lockOpenWorkflowTask(ctx, runID, scheduledEventID)
	if err != nil {
		return err
	}
	defer release()
	if open.scheduledEventID == 0 {
		return nil
	}
	return s.timeoutWorkflowTask(ctx, runID, open.scheduledEventID, errMsg, false)
}

// HandleWorkflowTaskFailed 镜像 HandleWorkflowTaskTimedOut：落 WorkflowTaskFailed 事件（非终态）、
// backoff 后重调度 WFT。实现 BlockWorkflow（照 Temporal applyWorkflowPanicPolicy 默认分支）——
// workflow 不终结、不产终态事件；修复代码部署后下一次 replay 通过即正常推进。
func (s *Server) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	open, release, err := s.lockOpenWorkflowTask(ctx, runID, scheduledEventID)
	if err != nil {
		return err
	}
	defer release()
	if open.scheduledEventID == 0 {
		return nil
	}
	return s.recordWorkflowTaskFailure(ctx, runID, open.scheduledEventID, cause, reason)
}

// recordWorkflowTaskFailure 写入 WorkflowTaskFailed 并按退避重调度。调用方持有 workflow 的 keyed lock。
// 失败的 WFT 若运行在 AutoUpgrade 候选版本上，Run 的 UpgradeError 诊断与 WorkflowTaskFailed 同事务写入。
func (s *Server) recordWorkflowTaskFailure(ctx context.Context, runID string, workflowTaskScheduledID int64, cause, reason string) error {
	treeID, branchID, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	failed := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: workflowTaskScheduledID,
		Cause:            cause,
		Reason:           reason,
		TraceContext:     s.telemetry.inject(ctx),
	})
	// AutoUpgrade 候选重放失败：只记录可查询诊断，保留 current/original/target 与业务 Activity history，
	// 然后走统一的 WFT 失败退避重试。下一次 poll 会重新选择 latest，因此发布兼容内容后无需
	// set-policy/signal 即可继续推进。
	inserted := false
	var state *execution.WorkflowExecutionState
	if err := s.telemetry.runInTx(ctx, "fail_workflow_task", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		inserted, state, err = execution.RecordWorkflowTaskFailure(tx, run, treeID, branchID, failed)
		return err
	}); err != nil {
		return err
	}
	// 幂等：!inserted（同 scheduledEventID 二次调用）→ 不重复调度，防重复 WFT。
	if !inserted {
		return nil
	}
	s.stopWorkflowTaskTimeout(runID, workflowTaskScheduledID)
	s.scheduleWorkflowTaskAfter(ctx, runID, state.WorkflowTaskFailureBackoff(s.wftBackoffBase, s.wftBackoffCap))
	return nil
}

// scheduleWorkflowTaskAfter 延迟重调度 WFT（Block 的节流）。每个 run 至多一个待触发的延迟 timer，
// 由 runClosed / Stop 停掉；到期后 handleWorkflowTaskRetryFired 从 execution state 取 trace 接续。
// delay 为 0 时同步调度，WithoutCancel 保留调用链的 trace carrier 并脱离请求取消信号。
// 进程重启丢失的 timer 由 recoverWorkflowActivation 与 FireOverdueTimers 兜底。
func (s *Server) scheduleWorkflowTaskAfter(ctx context.Context, runID string, delay time.Duration) {
	if delay <= 0 {
		if err := s.scheduleWorkflowTaskIfRunning(context.WithoutCancel(ctx), runID); err != nil {
			s.logger.Error("reschedule workflow task after workflow task failure failed", "run_id", runID, "err", err)
		}
		return
	}
	s.timers.Arm(timerRef{Kind: timerWorkflowTaskRetry, RunID: runID}, s.clock.Now().Add(delay))
}
