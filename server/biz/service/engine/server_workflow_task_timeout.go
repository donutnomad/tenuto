package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// workflowTaskTimeoutMessage 是 Server 计时到期时 WorkflowTaskTimedOut 的错误文本。
const workflowTaskTimeoutMessage = "workflow task start-to-close timeout"

func workflowTaskTimeoutRef(runID string, scheduledEventID int64) timerRef {
	return timerRef{Kind: timerWorkflowTaskTimeout, RunID: runID, ScheduledEventID: scheduledEventID}
}

// armWorkflowTaskTimeout 让已 started 的 WFT 在 deadline 到期；同一个 WFT 再次被领取时替换旧的 timer。
func (s *Server) armWorkflowTaskTimeout(runID string, scheduledEventID int64, deadline time.Time) {
	s.timers.Arm(workflowTaskTimeoutRef(runID, scheduledEventID), deadline)
}

func (s *Server) stopWorkflowTaskTimeout(runID string, scheduledEventID int64) {
	s.timers.Stop(workflowTaskTimeoutRef(runID, scheduledEventID))
}

// restoreWorkflowTaskTimeout 按 execution state 记录的截止时间重挂已 started 的 open WFT 的超时 timer。
// armOverdue 为 false 时跳过已过期的截止时间，留给 FireOverdueTimers 按宽限期接管。
func (s *Server) restoreWorkflowTaskTimeout(runID string, state *execution.WorkflowExecutionState, armOverdue bool) {
	scheduledEventID, deadline, ok := state.WorkflowTaskTimeoutDeadline()
	if !ok || (!armOverdue && !deadline.After(s.clock.Now())) {
		return
	}
	s.armWorkflowTaskTimeout(runID, scheduledEventID, deadline)
}

// handleWorkflowTaskTimeoutFired 是 Server 侧 WFT start-to-close 超时的到期入口：领取它的 Worker 没有在
// 超时内关闭它（例如领取后崩溃）时记为超时并调度新的 WFT。
func (s *Server) handleWorkflowTaskTimeoutFired(ctx context.Context, runID string, scheduledEventID int64) {
	_, release, err := s.lockRunWorkflow(ctx, runID)
	if err != nil {
		return
	}
	defer release()
	if err := s.timeoutWorkflowTask(ctx, runID, scheduledEventID, workflowTaskTimeoutMessage, true); err != nil {
		s.logger.Error("time out workflow task failed; the overdue scan times it out again",
			slog.String("run_id", runID),
			slog.Int64("scheduled_event_id", scheduledEventID),
			slog.Any("err", err),
		)
	}
}

// timeoutWorkflowTask 记 WorkflowTaskTimedOut 并立即调度下一个 WFT，Worker 上报的超时与 Server 计时到期
// 共用这一条路径。调用方持有 workflow 的 keyed lock。两条路径对同一个 WFT 只有一方写入关闭事件。
// WorkflowTaskTimedOut 让 ActivationPending 保持为 true：提交之后调度失败（或进程退出）时只记日志，
// 由周期补偿与启动恢复按 NeedsWorkflowTask 补上调度，不把已提交的超时报成失败。
func (s *Server) timeoutWorkflowTask(ctx context.Context, runID string, scheduledEventID int64, message string, serverTimer bool) error {
	treeID, branchID, err := s.runBranchInfo(runID)
	if err != nil {
		return err
	}
	var result execution.WorkflowTaskCloseResult
	if err := s.telemetry.runInTx(ctx, "timeout_workflow_task", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutWorkflowTask(tx, execution.WorkflowTaskTimeout{
			RunID: runID, TreeID: treeID, BranchID: branchID, ScheduledEventID: scheduledEventID,
			Message: message, Now: s.clock.Now(), ServerTimer: serverTimer,
		})
		return err
	}); err != nil || !result.Inserted {
		return err
	}
	s.stopWorkflowTaskTimeout(runID, scheduledEventID)
	if err := s.scheduleWorkflowTask(context.WithoutCancel(ctx), runID); err != nil {
		s.logger.Error("schedule workflow task after timeout failed; reconcile schedules it",
			slog.String("run_id", runID),
			slog.Int64("scheduled_event_id", scheduledEventID),
			slog.Any("err", err),
		)
	}
	return nil
}
