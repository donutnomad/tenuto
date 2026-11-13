package assembly

import (
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// NewWorker 创建与 Server 同进程的 Worker：Activity 的开始、心跳与完成直接调用 Server。没有指定
// ActivityWorkerVersionRegistry 时 Worker 直接向这个 Server 登记自己的 Activity 路由，与远程 Worker 一样进路由目录；
// 没有 Identity 时生成一个，路由登记的心跳与注销按它进行。
func NewWorker(server *engine.Server, activities *activity.Registry, opts worker.WorkerOptions) *worker.Worker {
	if opts.ActivityWorkerVersionRegistry == nil {
		opts.ActivityWorkerVersionRegistry = server
	}
	if opts.Identity == "" {
		opts.Identity = "in-process-" + execution.NewID()
	}
	return worker.New(server, server, engine.NewActivityRuntimeService(server), activities, opts)
}

// NewActivityWorker 创建直接从 Server 侧队列路由取任务的 Activity Worker。
func NewActivityWorker(activityQueue dispatch.ActivityTaskQueueRouter, runtime activitytasks.API, activities *activity.Registry, opts worker.WorkerOptions) *worker.Worker {
	return worker.NewActivityWorkerWithQueue(engine.NewActivityQueueWorkerAPI(activityQueue), runtime, activities, opts)
}
