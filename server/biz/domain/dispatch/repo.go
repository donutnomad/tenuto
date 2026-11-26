package dispatch

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// Repository 持久化派发租约。消费方是 Service 与执行核心的恢复路径。
type Repository interface {
	// AcquireTaskDispatch 为任务取得派发租约。任务标识不完整、已派发，
	// 或另一方持有未过期的租约时 acquired 为 false。
	AcquireTaskDispatch(task protocol.Task, leaseTTL time.Duration) (lease Lease, acquired bool, err error)
	// MarkTaskDispatchDispatched 与 MarkTaskDispatchFailed 只对 leaseID 的持有者生效。
	MarkTaskDispatchDispatched(task protocol.Task, leaseID string) error
	MarkTaskDispatchFailed(task protocol.Task, leaseID, errMsg string) error
	// ReleaseTaskDispatch 让任务可以再次派发；另一方持有的未过期租约保持原状。
	ReleaseTaskDispatch(task protocol.Task, errMsg string) error
	// GetTaskDispatch 读取任务的派发记录；没有记录时 found 为 false。
	GetTaskDispatch(task protocol.Task) (state State, found bool, err error)
}

// WorkerVersionRepository 持久化 Activity Worker 的路由登记，支撑服务重启后的路由恢复。
// 消费方是执行核心、usecase/definitions 与 view/executionview。
type WorkerVersionRepository interface {
	// UpsertActivityWorkerVersionInNamespace 按 Worker 的 Activity 类型与 task queue 展开登记，
	// 并移除同一 Worker（namespace、identity、build）不再声明的路由。
	UpsertActivityWorkerVersionInNamespace(namespace string, worker protocol.ActivityWorkerVersion, now time.Time, ttl time.Duration) (protocol.ActivityWorkerVersion, error)
	// HeartbeatActivityWorkerVersionInNamespace 给 Worker 的全部路由续期：登记了 HeartbeatTTL 的按登记值，其余按 ttl。
	HeartbeatActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time, ttl time.Duration) error
	// DeregisterActivityWorkerVersionInNamespace 让 Worker 的全部路由在 now 到期：路由立刻退出选路，
	// 登记记录保留。Worker 没有登记过时直接返回 nil。
	DeregisterActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time) error
	// ListActiveActivityWorkerVersions 返回 now 时刻未过期的路由，每个 Activity 类型与 task queue 一条，
	// 按 (Namespace, Activity 类型, BuildID, task queue, Identity) 升序。
	ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error)
	// ListActivityWorkerVersionsOfWorker 返回 namespace 内 identity 的全部路由（含已过期的），排序同上。
	// 指定了目标 Worker 的 Run 按它解析路由：Worker 下线后任务仍投到它最后登记的队列与 build。
	ListActivityWorkerVersionsOfWorker(namespace, identity string) ([]protocol.ActivityWorkerVersion, error)
	// DeleteExpiredActivityWorkerVersions 删除在 before 之前已经到期的路由，返回删除的行数。
	DeleteExpiredActivityWorkerVersions(before time.Time) (int64, error)
}

// TaskQueue 是 Workflow 任务队列端口。
type TaskQueue interface {
	Enqueue(ctx context.Context, task protocol.Task) error
	Dequeue(ctx context.Context) (protocol.TaskLease, error)
	Ack(ctx context.Context, lease protocol.TaskLease) error
	Nack(ctx context.Context, lease protocol.TaskLease) error
	// EnqueueSticky 把 Workflow 任务定向投递给 workerIdentity；DequeueForWorker 领取定向给它的任务与普通任务。
	EnqueueSticky(ctx context.Context, workerIdentity string, task protocol.Task) error
	DequeueForWorker(ctx context.Context, workerIdentity string) (protocol.TaskLease, error)
	// HandlesStickyRoutingDeadline 报告队列是否自行在 sticky schedule-to-start 期限到期后把任务改投普通通道；
	// 返回 false 时由引擎释放派发记录并重新投递。
	HandlesStickyRoutingDeadline() bool
	// Durable 报告队列里的任务在 Server 进程重启后是否仍在；返回 false 时启动恢复释放派发记录并重新投递。
	Durable() bool
}

// ActivityTaskQueueRouter 按 task queue 投递与领取 Activity 任务。
type ActivityTaskQueueRouter interface {
	Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error
	Dequeue(ctx context.Context, taskQueue string) (protocol.TaskLease, error)
	Ack(ctx context.Context, lease protocol.TaskLease) error
	Nack(ctx context.Context, lease protocol.TaskLease) error
	// Durable 报告队列里的任务在 Server 进程重启后是否仍在；返回 false 时启动恢复释放派发记录并重新投递。
	Durable() bool
}
