package engine

import (
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// ActivityRuntimeBackend 是 Activity runtime 需要的 Server 能力：执行状态推进与任务队列。
type ActivityRuntimeBackend interface {
	activitytasks.Backend
	ActivityWorkerQueueAPI
}

// ActivityRuntimeService 是 Activity Worker 面对的 runtime：任务执行过程经 activitytasks 用例，
// 取任务与确认直接走 Server 的 Activity 队列。
type ActivityRuntimeService struct {
	*activitytasks.UseCase
	ActivityWorkerQueueAPI
}

func NewActivityRuntimeService(backend ActivityRuntimeBackend) *ActivityRuntimeService {
	return &ActivityRuntimeService{UseCase: activitytasks.NewUseCase(backend), ActivityWorkerQueueAPI: backend}
}
