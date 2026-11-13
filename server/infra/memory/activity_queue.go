package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

var _ dispatch.ActivityTaskQueueRouter = (*ActivityQueueRouter)(nil)

// ActivityQueueRouter 是内存后端的 Activity 队列，投递约定与 TaskQ 后端一致：
// 任务按 (namespace, task queue) 或 (namespace, task queue, 目标 Worker) 分通道：
// 没有目标 Worker 的任务交给订阅这个队列的任何 Worker，与它的 build 无关；带目标 Worker 的任务只交给这个 Identity。
type ActivityQueueRouter struct {
	mu     sync.Mutex
	size   int
	queues map[string]*memoryActivityTaskQueue
}

func NewActivityQueueRouter(size int) *ActivityQueueRouter {
	if size <= 0 {
		size = 100
	}
	return &ActivityQueueRouter{
		size:   size,
		queues: make(map[string]*memoryActivityTaskQueue),
	}
}

func (r *ActivityQueueRouter) Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error {
	queueName := dispatch.NormalizeActivityTaskQueue(taskQueue)
	task.TaskQueue = queueName
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	return r.queue(activityMemoryQueueKey(task.Namespace, queueName, task.TargetWorkerIdentity)).Enqueue(ctx, task)
}

func (r *ActivityQueueRouter) Dequeue(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	queueName := dispatch.NormalizeActivityTaskQueue(taskQueue)
	namespace := protocol.NamespaceFromContext(ctx)
	worker, _ := protocol.WorkerRoutingInfoFrom(ctx)
	// 与 taskq 的 accepted routing keys 一一对应；nil 通道在 select 里永不就绪。
	var targeted chan protocol.TaskLease
	shared := r.queue(activityMemoryQueueKey(namespace, queueName, "")).ch
	if worker.Identity != "" {
		targeted = r.queue(activityMemoryQueueKey(namespace, queueName, worker.Identity)).ch
	}
	select {
	case lease := <-shared:
		return lease, nil
	case lease := <-targeted:
		return lease, nil
	case <-ctx.Done():
		return protocol.TaskLease{}, ctx.Err()
	}
}

func (r *ActivityQueueRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue(activityMemoryQueueKey(lease.Task.Namespace, lease.Task.TaskQueue, lease.Task.TargetWorkerIdentity)).Ack(ctx, lease)
}

func (r *ActivityQueueRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue(activityMemoryQueueKey(lease.Task.Namespace, lease.Task.TaskQueue, lease.Task.TargetWorkerIdentity)).Nack(ctx, lease)
}

// Durable 返回 false：任务只在进程内存里。
func (r *ActivityQueueRouter) Durable() bool { return false }

func (r *ActivityQueueRouter) queue(key string) *memoryActivityTaskQueue {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.queues[key]
	if q == nil {
		q = &memoryActivityTaskQueue{ch: make(chan protocol.TaskLease, r.size)}
		r.queues[key] = q
	}
	return q
}

type memoryActivityTaskQueue struct {
	ch   chan protocol.TaskLease
	next atomic.Int64
}

func (q *memoryActivityTaskQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	token := fmt.Sprintf("memory-activity-%d", q.next.Add(1))
	select {
	case q.ch <- protocol.TaskLease{Task: task, Token: token}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *memoryActivityTaskQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (q *memoryActivityTaskQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return q.Enqueue(ctx, lease.Task)
}

func activityMemoryQueueKey(namespace, queue, identity string) string {
	return dispatch.ActivityTaskQueueKey(namespace, queue) + "\x00worker:" + identity
}
