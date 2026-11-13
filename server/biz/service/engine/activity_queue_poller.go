package engine

import (
	"context"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// NewActivityQueueWorkerAPI 把 Server 侧的 Activity 队列路由适配成 Worker 取任务的接口。
func NewActivityQueueWorkerAPI(activityQueue dispatch.ActivityTaskQueueRouter) ActivityWorkerQueueAPI {
	return &activityQueueWorkerAPI{activityQueue: activityQueue}
}

type activityQueueWorkerAPI struct {
	activityQueue dispatch.ActivityTaskQueueRouter
}

func (a *activityQueueWorkerAPI) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	return a.activityQueue.Dequeue(ctx, taskQueue)
}

func (a *activityQueueWorkerAPI) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return a.activityQueue.Ack(ctx, lease)
}

func (a *activityQueueWorkerAPI) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return a.activityQueue.Nack(ctx, lease)
}
