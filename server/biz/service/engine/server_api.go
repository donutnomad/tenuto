package engine

import (
	"context"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

type ServerAPI interface {
	WorkflowWorkerAPI
	ActivityWorkerQueueAPI
	activitytasks.Backend
}

type WorkflowWorkerAPI interface {
	PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error)
	AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error
	NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error
	// GetWorkflowConfig 读取 Run 在 programVersion 这一版 Program 下的配置；programVersion 为空时取 Run 当前采用的版本。
	GetWorkflowConfig(ctx context.Context, runID, programVersion string) (*program.WorkflowConfig, error)
	GetEvents(runID string) ([]history.Event, error)
	GetRunStatus(runID string) (history.Status, error)
	HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error
	HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error
	// HandleWorkflowTaskFailed 落 WorkflowTaskFailed 事件（非终态）并按 backoff 重调度 WFT，实现
	// BlockWorkflow：workflow 不终结，等代码回滚 replay 自愈（照 Temporal，仅 non-det 类走此路径）。
	HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error
	// GetWorkflowActivation / CompleteWorkflowActivation 是 sticky execution cache 用的增量激活。
	GetWorkflowActivation(ctx context.Context, request history.WorkflowActivationRequest) (history.WorkflowActivation, error)
	CompleteWorkflowActivation(ctx context.Context, request history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error)
}

// WorkflowTaskFailedCauseActivityRouteUnavailable 表示 Activity 暂无可用路由，Worker 上线后的重试即可恢复。
const WorkflowTaskFailedCauseActivityRouteUnavailable = "activity_route_unavailable"

type ActivityWorkerQueueAPI interface {
	PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error)
	AckActivityTask(ctx context.Context, lease protocol.TaskLease) error
	NackActivityTask(ctx context.Context, lease protocol.TaskLease) error
}
