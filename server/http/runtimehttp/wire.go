package runtimehttp

import (
	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
)

// 本文件在线上契约 protocol/runtimeapi 与服务端的领域实体、用例入参、读模型之间转换。
// 字段集合相同的结构体用类型转换：任何一侧增删字段都会让转换编译失败，两边因此保持同步。

// decodeWorkflowConfig 解码登记请求里的 Workflow 配置，数字的处理与请求体其余部分一致。
func decodeWorkflowConfig(data []byte) (*program.WorkflowConfig, error) {
	var workflow program.WorkflowConfig
	if len(data) == 0 {
		return &workflow, nil
	}
	if err := jsonvalue.Unmarshal(data, &workflow); err != nil {
		return nil, protocol.InvalidRequestf("decode workflow config: %v", err)
	}
	return &workflow, nil
}

func mapSlice[T, U any](in []T, convert func(T) U) []U {
	if in == nil {
		return nil
	}
	out := make([]U, len(in))
	for i := range in {
		out[i] = convert(in[i])
	}
	return out
}

func configRefToWire(ref definition.WorkflowConfigRef, err error) (runtimeapi.WorkflowConfigRef, error) {
	return runtimeapi.WorkflowConfigRef(ref), err
}

func runToWire(run *execution.Run) *runtimeapi.Run {
	if run == nil {
		return nil
	}
	return &runtimeapi.Run{
		ID:                            run.ID,
		Namespace:                     run.Namespace,
		WorkflowID:                    run.WorkflowID,
		WorkflowType:                  run.WorkflowType,
		WorkflowConfigVersion:         run.WorkflowConfigVersion,
		OriginalWorkflowConfigVersion: run.OriginalWorkflowConfigVersion,
		Policy:                        run.Policy,
		PolicySource:                  run.PolicySource,
		TargetWorkerIdentity:          run.TargetWorkerIdentity,
		UpgradeError:                  run.UpgradeError,
		ConfigError:                   run.ConfigError,
		ParentRunID:                   run.ParentRunID,
		ParentChildID:                 run.ParentChildID,
		ParentScheduledEventID:        run.ParentScheduledEventID,
		FirstRunID:                    run.FirstRunID,
		ContinuedRunID:                run.ContinuedRunID,
		ResetRunID:                    run.ResetRunID,
		BranchToken:                   run.BranchToken,
		Status:                        run.Status,
		Input:                         run.Input,
		Output:                        run.Output,
		SearchAttributes:              run.SearchAttributes,
		CreatedAt:                     run.CreatedAt,
		DeadlineAt:                    run.DeadlineAt,
		RunDeadlineAt:                 run.RunDeadlineAt,
		Attempt:                       run.Attempt,
		Initiator:                     run.Initiator,
		IsCurrent:                     run.IsCurrent,
	}
}

func runWithErrToWire(run *execution.Run, err error) (*runtimeapi.Run, error) {
	return runToWire(run), err
}

func waitingToWire(waiting execution.WaitingState) runtimeapi.WaitingState {
	return runtimeapi.WaitingState{
		Type:             string(waiting.Type),
		ActivityID:       waiting.ActivityID,
		ChildID:          waiting.ChildID,
		ActivityType:     waiting.ActivityType,
		TaskQueue:        waiting.TaskQueue,
		TimerID:          waiting.TimerID,
		ChildRunID:       waiting.ChildRunID,
		ScheduledEventID: waiting.ScheduledEventID,
	}
}

func runWithBlockedToWire(run *executionview.RunWithBlocked, err error) (*runtimeapi.RunWithBlocked, error) {
	if run == nil {
		return nil, err
	}
	return &runtimeapi.RunWithBlocked{
		Run:             runToWire(run.Run),
		Blocked:         runtimeapi.BlockedInfo(run.Blocked),
		TerminalFailure: run.TerminalFailure,
	}, err
}

func runListResultToWire(result execution.RunListResult, err error) (runtimeapi.RunListResult, error) {
	return runtimeapi.RunListResult{
		Items: mapSlice(result.Items, func(item execution.RunListItem) runtimeapi.RunListItem {
			var blocked *runtimeapi.BlockedInfo
			if item.Blocked != nil {
				converted := runtimeapi.BlockedInfo(*item.Blocked)
				blocked = &converted
			}
			return runtimeapi.RunListItem{
				RunID:                 item.RunID,
				Namespace:             item.Namespace,
				WorkflowID:            item.WorkflowID,
				WorkflowType:          item.WorkflowType,
				Status:                item.Status,
				WorkflowConfigVersion: item.WorkflowConfigVersion,
				ParentRunID:           item.ParentRunID,
				ResetRunID:            item.ResetRunID,
				IsReset:               item.IsReset,
				CreatedAt:             item.CreatedAt,
				ClosedAt:              item.ClosedAt,
				SearchAttributes:      item.SearchAttributes,
				CurrentWaiting:        waitingToWire(item.CurrentWaiting),
				FailureReason:         item.FailureReason,
				HistoryLength:         item.HistoryLength,
				Blocked:               blocked,
			}
		}),
		Total:         result.Total,
		HasMore:       result.HasMore,
		NextPageToken: result.NextPageToken,
	}, err
}

func historyPageRequestFromWire(request runtimeapi.HistoryPageRequest) execution.HistoryPageRequest {
	return execution.HistoryPageRequest{
		RunID:     request.RunID,
		PageSize:  request.PageSize,
		Direction: execution.HistoryDirection(request.Direction),
		PageToken: request.PageToken,
	}
}

func queryResultToWire(result execution.WorkflowQueryResult, err error) (runtimeapi.WorkflowQueryResult, error) {
	return runtimeapi.WorkflowQueryResult{
		Run:              runToWire(result.Run),
		QueryName:        result.QueryName,
		HistoryLength:    result.HistoryLength,
		SearchAttributes: result.SearchAttributes,
		CurrentWaiting:   waitingToWire(result.CurrentWaiting),
		FailureReason:    result.FailureReason,
		Signals:          result.Signals,
		Updates:          result.Updates,
		QueryResult:      result.QueryResult,
		QueryRejected:    result.QueryRejected,
		QueryError:       result.QueryError,
	}, err
}

func activityStateToWire(state *execution.ActivityState) *runtimeapi.ActivityState {
	if state == nil {
		return nil
	}
	return &runtimeapi.ActivityState{
		TargetWorkerIdentity:     state.TargetWorkerIdentity,
		RunID:                    state.RunID,
		ScheduledEventID:         state.ScheduledEventID,
		ActivityID:               state.ActivityID,
		ActivityType:             state.ActivityType,
		TaskQueue:                state.TaskQueue,
		Attempt:                  state.Attempt,
		Stamp:                    state.Stamp,
		StartedEventID:           state.StartedEventID,
		ActivityReset:            state.ActivityReset,
		CancelRequested:          state.CancelRequested,
		StartedTime:              state.StartedTime,
		WorkerIdentity:           state.WorkerIdentity,
		WorkerBuildID:            state.WorkerBuildID,
		StartRequestID:           state.StartRequestID,
		LastHeartbeat:            state.LastHeartbeat,
		LastHeartbeatAt:          state.LastHeartbeatAt,
		RetryLastFailure:         state.RetryLastFailure,
		NextAttemptAt:            state.NextAttemptAt,
		Status:                   string(state.Status),
		ScheduledTime:            state.ScheduledTime,
		ScheduleToStartTimeoutMs: state.ScheduleToStartTimeoutMs,
		StartToCloseTimeoutMs:    state.StartToCloseTimeoutMs,
		ScheduleToCloseTimeoutMs: state.ScheduleToCloseTimeoutMs,
		HeartbeatTimeoutMs:       state.HeartbeatTimeoutMs,
		RetryPolicy:              state.RetryPolicy,
		Input:                    state.Input,
		TraceContext:             state.TraceContext,
		CreatedAt:                state.CreatedAt,
		UpdatedAt:                state.UpdatedAt,
	}
}

func scheduleToWire(s *schedule.Schedule) *runtimeapi.Schedule {
	if s == nil {
		return nil
	}
	return &runtimeapi.Schedule{
		ID:                  s.ID,
		Namespace:           s.Namespace,
		WorkflowType:        s.WorkflowType,
		Description:         s.Description,
		CronExpression:      s.CronExpression,
		Timezone:            s.Timezone,
		Input:               s.Input,
		Paused:              s.Paused,
		OverlapPolicy:       s.OverlapPolicy,
		LastRunAt:           s.LastRunAt,
		NextRunAt:           s.NextRunAt,
		LastRunID:           s.LastRunID,
		RunCount:            s.RunCount,
		CatchupWindow:       s.CatchupWindow,
		MissedCatchupWindow: s.MissedCatchupWindow,
		BufferDropped:       s.BufferDropped,
		CreatedAt:           s.CreatedAt,
		UpdatedAt:           s.UpdatedAt,
	}
}

func scheduleWithErrToWire(s *schedule.Schedule, err error) (*runtimeapi.Schedule, error) {
	return scheduleToWire(s), err
}

func scheduleListResultToWire(result schedules.ListResult, err error) (runtimeapi.ScheduleListResult, error) {
	return runtimeapi.ScheduleListResult{
		Items: mapSlice(result.Items, func(s schedule.Schedule) runtimeapi.Schedule { return *scheduleToWire(&s) }),
		Total: result.Total,
	}, err
}

func batchStartRequestFromWire(req runtimeapi.BatchStartRequest) batches.StartRequest {
	return batches.StartRequest{
		Namespace: req.Namespace,
		Type:      batch.Type(req.Type),
		Filter:    execution.RunListFilter(req.Filter),
		Reason:    req.Reason,
		ResetType: execution.ResetType(req.ResetType),
	}
}

func batchOperationToWire(op *batch.Operation) *runtimeapi.BatchOperation {
	if op == nil {
		return nil
	}
	return &runtimeapi.BatchOperation{
		ID:             op.ID,
		Namespace:      op.Namespace,
		Type:           string(op.Type),
		Filter:         runtimeapi.RunListFilter(op.Filter),
		ResetType:      string(op.ResetType),
		Reason:         op.Reason,
		Status:         string(op.Status),
		TotalCount:     op.TotalCount,
		SucceededCount: op.SucceededCount,
		FailedCount:    op.FailedCount,
		SkippedCount:   op.SkippedCount,
		Errors: mapSlice(op.Errors, func(e batch.RunError) runtimeapi.BatchRunError {
			return runtimeapi.BatchRunError(e)
		}),
		CreatedAt:  op.CreatedAt,
		StartedAt:  op.StartedAt,
		FinishedAt: op.FinishedAt,
		UpdatedAt:  op.UpdatedAt,
	}
}

func batchOperationWithErrToWire(op *batch.Operation, err error) (*runtimeapi.BatchOperation, error) {
	return batchOperationToWire(op), err
}

func batchListResultToWire(result batches.ListResult, err error) (runtimeapi.BatchListResult, error) {
	return runtimeapi.BatchListResult{
		Items: mapSlice(result.Items, func(op batch.Operation) runtimeapi.BatchOperation { return *batchOperationToWire(&op) }),
		Total: result.Total,
	}, err
}
