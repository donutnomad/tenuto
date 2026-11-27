package engine

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"go.opentelemetry.io/otel/attribute"
)

// startDurableTimer 让 durable timer 在 deadline 到期；deadline 已过时立即到期。
// 到期处理用 context.Background()：触发时机由 deadline 决定，与调用方请求的生命周期无关。
func (s *Server) startDurableTimer(runID string, scheduledEventID int64, deadline time.Time) {
	s.timers.Arm(timerRef{Kind: timerDurable, RunID: runID, ScheduledEventID: scheduledEventID}, deadline)
}

// handleTimerFired 按 TimerState.Kind 把触发分发给对应的 handler，每个 Kind 是完整独立的路径。
func (s *Server) handleTimerFired(ctx context.Context, runID string, scheduledEventID int64) {
	st, ok, err := s.mgr.GetTimerState(runID, scheduledEventID)
	if err != nil || !ok || st.IsTerminal() {
		return
	}
	ctx = s.telemetry.extract(ctx, st.TraceContext)
	switch st.Kind {
	case execution.TimerKindEnums.RetryBackoff:
		s.handleRetryBackoffFired(ctx, runID, scheduledEventID)
	case execution.TimerKindEnums.UserTimer:
		s.handleUserTimerFired(ctx, runID, scheduledEventID, st.TimerID)
	default:
		s.logger.Error("unknown timer kind", "kind", st.Kind, "run_id", runID, "scheduled_event_id", scheduledEventID)
	}
}

// handleUserTimerFired 处理用户通过 CmdScheduleTimer 命令创建的 durable timer 到期。
// 落 TimerFired 事件 → 推进状态到 Fired → 调度下一轮 WFT。
func (s *Server) handleUserTimerFired(ctx context.Context, runID string, scheduledEventID int64, timerID string) {
	ctx, span := s.telemetry.start(ctx, "FireTimer",
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.Int64(tenutotel.AttrScheduledEventID, scheduledEventID),
		attribute.String("tenuto.timer.id", timerID),
	)
	var spanErr error
	defer func() { tenutotel.FinishSpan(span, spanErr) }()
	// timer goroutine 写 TimerFired 事件并调度 WFT，须与并发 WFT/signal 同 workflowID 串行。
	// 下游 scheduleWorkflowTask 仅异步入队 WFT，不同步回调拿锁函数，同 workflowID 不重入。
	ref, release, err := s.lockRunWorkflow(ctx, runID)
	if err != nil {
		spanErr = err
		return
	}
	defer release()
	span.SetAttributes(workflowSpanAttributes(ref.Namespace, ref.WorkflowID, ref.ID, ref.WorkflowType)...)

	if status, err := s.GetRunStatus(runID); err != nil || status != history.StatusRunning {
		return
	}

	treeID, branchID, err := ref.branch.treeID, ref.branch.branchID, ref.branch.err
	if err != nil {
		spanErr = err
		return
	}
	now := s.clock.Now()
	fired := false
	if txErr := s.runInTx(ctx, "fire_timer", func(tx execution.ExecutionTx) error {
		var err error
		fired, err = execution.FireTimer(tx, execution.TimerFire{
			RunID: runID, TreeID: treeID, BranchID: branchID,
			ScheduledEventID: scheduledEventID, Kind: execution.TimerKindEnums.UserTimer, Now: now,
		})
		return err
	}); txErr != nil {
		spanErr = txErr
		s.logger.Error("fire durable timer failed", "run_id", runID, "timer_id", timerID, "err", txErr)
		return
	}
	if !fired {
		return // 已终态，无新进展
	}
	s.stopDurableTimer(runID, scheduledEventID)
	spanErr = s.scheduleWorkflowTask(ctx, runID)
}

// handleRetryBackoffFired 处理 workflow retry 的 backoff 延迟 timer 到期。
// 到期后调度新 run（即 retryBackoff timer 所在的 run）的首个 WFT。
// 不落 TimerFired 事件（RetryBackoff 是引擎内部 timer，不暴露给 workflow history）。
func (s *Server) handleRetryBackoffFired(ctx context.Context, runID string, scheduledEventID int64) {
	// timer goroutine 推进 RetryBackoff timer 到 Fired 并调度新 run 首个 WFT，须拿 per-(namespace, workflowID)
	// keyed lock 与并发 WFT/signal 串行（retry 新 run 继承同 namespace 同 workflowID）。下游 scheduleWorkflowTask
	// 仅异步入队，不同步回调拿锁函数，不重入。
	_, release, err := s.lockRunWorkflow(ctx, runID)
	if err != nil {
		return
	}
	defer release()

	now := s.clock.Now()
	fired := false
	if txErr := s.runInTx(ctx, "fire_retry_timer", func(tx execution.ExecutionTx) error {
		var err error
		fired, err = execution.FireTimer(tx, execution.TimerFire{
			RunID: runID, ScheduledEventID: scheduledEventID,
			Kind: execution.TimerKindEnums.RetryBackoff, Now: now,
		})
		return err
	}); txErr != nil {
		s.logger.Error("fire retry backoff timer failed", "run_id", runID, "err", txErr)
		return
	}
	if !fired {
		return
	}
	s.stopDurableTimer(runID, scheduledEventID)
	// 退避期内的取消请求或 Update 已经调度过 WFT 时，Run 已经开始，到期不再调度（同 Temporal 的
	// workflowBackoffTimer 对 HadOrHasWorkflowTask 的检查）。
	state, err := s.mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		s.logger.Error("read execution state after retry backoff failed", "run_id", runID, "err", err)
		return
	}
	if state.HadOrHasWorkflowTask() {
		return
	}
	// 调度新 run 的首个 WFT（幂等：若 run 已非 Running 则跳过）
	if err := s.scheduleWorkflowTask(ctx, runID); err != nil {
		s.logger.Error("schedule first workflow task after retry backoff failed", "run_id", runID, "err", err)
	}
}

func (s *Server) stopDurableTimer(runID string, scheduledEventID int64) {
	s.timers.Stop(timerRef{Kind: timerDurable, RunID: runID, ScheduledEventID: scheduledEventID})
}
