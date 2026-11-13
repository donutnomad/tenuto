// Package worker 是 Workflow Worker 与 Activity Worker 的运行时：poll 任务、执行、把结果上报给 Server。
// 它经本文件定义的接口使用 Server，同进程时由 Server 直接实现，跨进程时由 runtime HTTP client 实现。
package worker

import (
	"context"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// WorkflowWorkerAPI 是 Workflow Worker 消费的 Server 能力。
type WorkflowWorkerAPI interface {
	PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error)
	AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error
	NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error
	// 读取 History 与配置、上报 Workflow Task 结果的方法与执行器用的是同一组。
	executor.WorkerWorkflowBackend
}

// IncrementalWorkflowWorkerAPI 是支持增量激活的 Server 额外提供的能力。
type IncrementalWorkflowWorkerAPI interface {
	GetWorkflowActivation(context.Context, history.WorkflowActivationRequest) (history.WorkflowActivation, error)
	CompleteWorkflowActivation(context.Context, history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error)
}

// ActivityWorkerQueueAPI 是 Activity Worker 取任务用到的队列能力。
type ActivityWorkerQueueAPI interface {
	PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error)
	AckActivityTask(ctx context.Context, lease protocol.TaskLease) error
	NackActivityTask(ctx context.Context, lease protocol.TaskLease) error
}

// ActivityRuntimeAPI 是 Activity Worker 上报执行过程用到的 Server 能力。
type ActivityRuntimeAPI interface {
	// CompleteActivityTask 的 failure 为 nil 表示成功，非 nil 表示失败。
	executor.ActivityRuntime
	CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error)
}

// ActivityWorkerVersionRegistryAPI 是 Activity Worker 登记版本、维持心跳并在停止时注销用到的 Server 能力。
type ActivityWorkerVersionRegistryAPI interface {
	RegisterActivityWorkerVersion(ctx context.Context, worker protocol.ActivityWorkerVersion) error
	HeartbeatActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error
	DeregisterActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error
}
