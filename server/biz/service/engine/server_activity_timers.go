package engine

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	tenutotel "github.com/donutnomad/tenuto/telemetry"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"go.opentelemetry.io/otel/attribute"
)

// scheduleActivityRetryTimer 在 state 已递增 attempt/stamp 后换上新 attempt 的 timer：按持久化的 NextAttemptAt
// 排定重试退避 timer，新 attempt 的 ScheduleToStart 从同一时刻起算。
func (s *Server) scheduleActivityRetryTimer(st *execution.ActivityState) {
	s.stopActivityAttemptTimers(st.RunID, st.ScheduledEventID)
	s.armActivityTimeouts(st, true)
	s.timers.Arm(activityTimerRef(timerActivityRetry, st.RunID, st.ScheduledEventID), *st.NextAttemptAt)
}

func (s *Server) activityTaskFromState(st *execution.ActivityState) protocol.Task {
	run, err := s.mgr.Get(st.RunID)
	if err != nil || run == nil {
		run = &execution.Run{ID: st.RunID}
	}
	task := execution.ActivityTaskFromState(run, st)
	task.TaskQueue = dispatch.NormalizeActivityTaskQueue(task.TaskQueue)
	return task
}

// startActivityTimers 为刚调度的 Activity 的首个 attempt 挂 ScheduleToClose 与 ScheduleToStart，now 是调度时刻。
func (s *Server) startActivityTimers(runID string, cmd history.Command, scheduledEventID int64, now time.Time) {
	if cmd.ScheduleToCloseTimeout > 0 {
		s.timers.Arm(activityTimerRef(timerActivityScheduleToClose, runID, scheduledEventID), now.Add(cmd.ScheduleToCloseTimeout))
	}
	if cmd.ScheduleToStartTimeout > 0 {
		s.timers.Arm(activityTimerRef(timerActivityScheduleToStart, runID, scheduledEventID), now.Add(cmd.ScheduleToStartTimeout))
	}
}

// handleActivityTimeout 按持久化状态的当前 attempt 处理一次超时。
func (s *Server) handleActivityTimeout(runID string, scheduledEventID int64, reason protocol.TimeoutType) {
	st, ok, err := s.mgr.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok {
		return
	}
	s.handleActivityTimeoutAttempt(runID, scheduledEventID, st.Attempt, reason)
}

func (s *Server) handleActivityTimeoutAttempt(runID string, scheduledEventID int64, attempt int, reason protocol.TimeoutType) {
	// activity 超时 timer goroutine 写 ActivityTimedOut 事件并经 HandleActivityFailed 改 run 事件/state、
	// 可能调度 WFT，须拿 per-(namespace, workflowID) keyed lock 与并发 WFT/signal 串行。所有 activity 超时入口
	// （s2c/hb/s2s/total timer）都汇聚到本函数，在此统一加锁；HandleActivityFailed/scheduleWorkflowTask
	// 不拿 keyed lock、派 WFT 仅异步入队，同 workflowID 不重入。timer 无请求 ctx，用 Background。
	ref, release, err := s.lockRunWorkflow(context.Background(), runID)
	if err != nil {
		return
	}
	defer release()

	attempt = history.NormalizeActivityAttempt(attempt)
	if status, err := s.GetRunStatus(runID); err != nil || status != history.StatusRunning {
		return
	}
	// 前置校验读 ActivityState：已终态或 attempt 已变化的超时是过期的。
	st, ok, err := s.mgr.GetActivityState(runID, scheduledEventID)
	if err != nil || !ok || st.IsTerminal() || st.Attempt != attempt {
		return // stale 超时
	}
	ctx := s.telemetry.extract(context.Background(), st.TraceContext)
	ctx, span := s.telemetry.start(ctx, "FailActivity",
		attribute.String(tenutotel.AttrWorkflowRunID, runID),
		attribute.Int64(tenutotel.AttrScheduledEventID, scheduledEventID),
		attribute.Int(tenutotel.AttrAttempt, history.NormalizeActivityAttempt(attempt)),
	)
	defer tenutotel.FinishSpan(span, nil)
	tid, bid, err := ref.branch.treeID, ref.branch.branchID, ref.branch.err
	if err != nil {
		return
	}
	timeoutCtx := ctx
	var failure execution.ActivityFailureResult
	if err := s.telemetry.runInTx(timeoutCtx, "timeout_activity", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		failure, err = execution.TimeoutActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: tid, BranchID: bid,
			ScheduledEventID: scheduledEventID, Attempt: attempt, Stamp: st.Stamp, Now: s.clock.Now(),
		}, reason)
		return err
	}); err != nil {
		s.logger.Error("mark activity timed out failed", "run_id", runID, "scheduled_event_id", scheduledEventID, "attempt", attempt, "reason", reason, "err", err)
		return
	}
	if !failure.Applied {
		return
	}
	if err := s.settleActivityFailure(timeoutCtx, runID, scheduledEventID, failure); err != nil {
		s.logger.Error("settle activity timeout failed", "run_id", runID, "scheduled_event_id", scheduledEventID, "attempt", attempt, "reason", reason, "err", err)
	}
}
