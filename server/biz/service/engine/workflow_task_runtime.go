package engine

import (
	"context"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type WorkflowTaskRuntimeAPI interface {
	PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error)                                                                               // 拉取 WFT；HTTP 边界给租约签 task token
	AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error                                                                            // 确认 WFT lease
	NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error                                                                           // 重投 WFT lease
	GetWorkflowConfig(ctx context.Context, runID, programVersion string) (*program.WorkflowConfig, error)                                           // 读取 WFT 选中的 Program 版本的配置
	GetEvents(runID string) ([]history.Event, error)                                                                                                // 读取 replay history
	GetRunStatus(runID string) (history.Status, error)                                                                                              // 读取 Run 当前状态
	HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error // 写入 WFT 完成和命令事件
	HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error                                      // 写入 WFT 超时并调度下一轮
	HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error                                 // 写入 WFT 失败（Block）并 backoff 重调度
	GetWorkflowActivation(ctx context.Context, request history.WorkflowActivationRequest) (history.WorkflowActivation, error)                       // 读取增量 activation
	CompleteWorkflowActivation(ctx context.Context, request history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error)       // 提交增量 activation 的结果
}

var _ WorkflowTaskRuntimeAPI = (*Server)(nil)
