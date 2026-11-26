package executionrepo

import (
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func (p *RunPO) ToPO(run *execution.Run) *RunPO {
	if run == nil {
		return nil
	}
	return &RunPO{
		ID:                     run.ID,
		Namespace:              run.Namespace,
		WorkflowID:             run.WorkflowID,
		WorkflowType:           run.WorkflowType,
		WorkflowConfigVersion:  run.WorkflowConfigVersion,
		OriginalConfigVersion:  run.OriginalWorkflowConfigVersion,
		Policy:                 run.Policy,
		PolicySource:           run.PolicySource,
		TargetWorkerIdentity:   run.TargetWorkerIdentity,
		UpgradeError:           run.UpgradeError,
		ConfigError:            run.ConfigError,
		ParentRunID:            run.ParentRunID,
		ParentChildID:          run.ParentChildID,
		ParentScheduledEventID: run.ParentScheduledEventID,
		FirstRunID:             run.FirstRunID,
		ContinuedRunID:         run.ContinuedRunID,
		ResetRunID:             run.ResetRunID,
		BranchTokenJSON:        run.BranchToken,
		Status:                 string(run.Status),
		CreatedAt:              run.CreatedAt,
		DeadlineAt:             run.DeadlineAt,
		RunDeadlineAt:          run.RunDeadlineAt,
		Attempt:                run.Attempt,
		Initiator:              run.Initiator,
		IsCurrent:              run.IsCurrent,
	}
}

func (p *WorkflowExecutionStatePO) ToPO(state *execution.WorkflowExecutionState) *WorkflowExecutionStatePO {
	if state == nil {
		return nil
	}
	return &WorkflowExecutionStatePO{
		RunID:                    state.RunID,
		NextEventID:              state.NextEventID,
		LastEventID:              state.LastEventID,
		HistoryEventCount:        state.HistoryEventCount,
		HistorySizeBytes:         state.HistorySizeBytes,
		Status:                   string(state.Status),
		ClosedAt:                 state.ClosedAt,
		FailureReason:            state.FailureReason,
		IsReset:                  state.IsReset,
		Blocked:                  state.Blocked.Blocked,
		OpenWorkflowTaskEventID:  state.OpenWorkflowTaskEventID,
		ExecutionGeneration:      state.ExecutionGeneration,
		CurrentActivationID:      state.CurrentActivationID,
		CurrentActivationAttempt: state.CurrentActivationAttempt,
		StickyWorkerIdentity:     state.StickyWorkerIdentity,
		StickyWorkerBuildID:      state.StickyWorkerBuildID,
		StickyGeneration:         state.StickyGeneration,
		StickyExpiresAt:          state.StickyExpiresAt,
		StateVersion:             state.StateVersion,
		UpdatedAt:                state.UpdatedAt,
	}
}

// truncateMillis 把时间截断到毫秒。activity state 的时间列是 DATETIME(3)，MySQL 写入时把亚毫秒部分四舍五入，
// 持久化的超时截止时间可能比进程内按纳秒计时的 timer 晚最多 0.5ms，timer 触发后复核会判为未到期。写入前向下截断，
// 持久化的截止时间不晚于进程内 timer。
func truncateMillis(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	truncated := t.Truncate(time.Millisecond)
	return &truncated
}

func (p *ActivityStatePO) ToPO(state *execution.ActivityState) *ActivityStatePO {
	if state == nil {
		return nil
	}
	return &ActivityStatePO{
		RunID:                    state.RunID,
		ScheduledEventID:         state.ScheduledEventID,
		ActivityID:               state.ActivityID,
		ActivityType:             state.ActivityType,
		TaskQueue:                state.TaskQueue,
		TargetWorkerIdentity:     state.TargetWorkerIdentity,
		Attempt:                  state.Attempt,
		Stamp:                    state.Stamp,
		StartedEventID:           state.StartedEventID,
		ActivityReset:            state.ActivityReset,
		CancelRequested:          state.CancelRequested,
		WorkerIdentity:           state.WorkerIdentity,
		WorkerBuildID:            state.WorkerBuildID,
		LastHeartbeatAt:          truncateMillis(state.LastHeartbeatAt),
		RetryLastFailure:         state.RetryLastFailure,
		NextAttemptAt:            truncateMillis(state.NextAttemptAt),
		TimeoutAt:                truncateMillis(execution.ActivityTimeoutAt(state)),
		Status:                   string(state.Status),
		ScheduledTime:            state.ScheduledTime.Truncate(time.Millisecond),
		StartedTime:              truncateMillis(state.StartedTime),
		ScheduleToStartTimeoutMs: state.ScheduleToStartTimeoutMs,
		StartToCloseTimeoutMs:    state.StartToCloseTimeoutMs,
		ScheduleToCloseTimeoutMs: state.ScheduleToCloseTimeoutMs,
		HeartbeatTimeoutMs:       state.HeartbeatTimeoutMs,
		CreatedAt:                state.CreatedAt,
		UpdatedAt:                state.UpdatedAt,
	}
}

func (p *TimerStatePO) ToPO(state *execution.TimerState) *TimerStatePO {
	if state == nil {
		return nil
	}
	return &TimerStatePO{
		RunID:            state.RunID,
		ScheduledEventID: state.ScheduledEventID,
		TimerID:          state.TimerID,
		Deadline:         state.Deadline,
		Status:           string(state.Status),
		Kind:             string(state.Kind),
		CreatedAt:        state.CreatedAt,
		UpdatedAt:        state.UpdatedAt,
	}
}

func (p *ChildWorkflowStatePO) ToPO(state *execution.ChildWorkflowState) *ChildWorkflowStatePO {
	if state == nil {
		return nil
	}
	return &ChildWorkflowStatePO{
		RunID:                 state.RunID,
		ScheduledEventID:      state.ScheduledEventID,
		ChildID:               state.ChildID,
		ChildRunID:            state.ChildRunID,
		WorkflowType:          state.WorkflowType,
		WorkflowConfigVersion: state.WorkflowConfigVersion,
		Status:                string(state.Status),
		ParentClosePolicy:     string(state.ParentClosePolicy),
		CancelRequested:       state.CancelRequested,
		Error:                 state.Error,
		Reason:                state.Reason,
		CreatedAt:             state.CreatedAt,
		UpdatedAt:             state.UpdatedAt,
	}
}
