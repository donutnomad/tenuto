// Package dispatchtest 是 Activity 队列投递约定的共享测试：内存后端与 TaskQ 后端各自的测试调用
// RunActivityRoutingContract，对同一组断言负责。
package dispatchtest

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// 契约用到的两个 task queue；newRouter 返回的 Router 需要能承接它们。
const (
	QueuePayments = "payments"
	QueueEmails   = "emails"
)

type poller struct {
	namespace, queue, identity, build string
}

func (p poller) ctx(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := protocol.WithNamespace(context.Background(), p.namespace)
	ctx = protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Namespace: p.namespace, Identity: p.identity, BuildID: p.build})
	return context.WithTimeout(ctx, timeout)
}

// RunActivityRoutingContract 固定 Activity Task 的投递约定：
//   - 任务只交给同一个 namespace、同一个 task queue 上的 Worker；
//   - 没有目标 Worker 的任务，队列上任何 build 的 Worker 都能领，任务不带 build；
//   - 带 TargetWorkerIdentity 的任务只交给这个 Identity 的 Worker，与它的 build 无关；
//   - Nack 之后任务按原来的路由重新投递。
func RunActivityRoutingContract(t *testing.T, newRouter func(t *testing.T) dispatch.ActivityTaskQueueRouter) {
	const namespace = "tenant"
	base := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: namespace, TaskQueue: QueuePayments, RunID: "run", ScheduledEventID: 1, Attempt: 1}
	general := func(task protocol.Task) protocol.Task { return task }
	targeted := func(task protocol.Task) protocol.Task { task.TargetWorkerIdentity = "worker-a"; return task }
	cases := []struct {
		name    string
		task    func(protocol.Task) protocol.Task
		refused []poller
		served  poller
	}{
		{
			name:   "queue task is served by the old build",
			task:   general,
			served: poller{namespace, QueuePayments, "worker-1", "v1"},
			refused: []poller{
				{namespace, QueueEmails, "worker-1", "v1"},
				{"other-tenant", QueuePayments, "worker-1", "v1"},
			},
		},
		{
			name:   "queue task is served by the new build",
			task:   general,
			served: poller{namespace, QueuePayments, "worker-2", "v2"},
			refused: []poller{
				{namespace, QueueEmails, "worker-2", "v2"},
			},
		},
		{
			name:   "queue task is served by a worker without build",
			task:   general,
			served: poller{namespace, QueuePayments, "", ""},
		},
		{
			name:   "target identity is served by that worker on any build",
			task:   targeted,
			served: poller{namespace, QueuePayments, "worker-a", "v2"},
			refused: []poller{
				{namespace, QueuePayments, "worker-b", "v2"},
				{namespace, QueuePayments, "", ""},
				{namespace, QueueEmails, "worker-a", "v2"},
				{"other-tenant", QueuePayments, "worker-a", "v2"},
			},
		},
		{
			name:   "target identity is served by that worker without build",
			task:   targeted,
			served: poller{namespace, QueuePayments, "worker-a", ""},
			refused: []poller{
				{namespace, QueuePayments, "worker-b", ""},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newRouter(t)
			task := tc.task(base)
			if err := router.Enqueue(context.Background(), task.TaskQueue, task); err != nil {
				t.Fatal(err)
			}
			// 第二轮在 Nack 之后：重新投递的任务保持原来的路由。
			for round := range 2 {
				for _, refused := range tc.refused {
					ctx, cancel := refused.ctx(30 * time.Millisecond)
					lease, err := router.Dequeue(ctx, refused.queue)
					cancel()
					if err == nil {
						t.Fatalf("round %d: %+v received task %+v", round, refused, lease.Task)
					}
				}
				ctx, cancel := tc.served.ctx(2 * time.Second)
				lease, err := router.Dequeue(ctx, tc.served.queue)
				cancel()
				if err != nil {
					t.Fatalf("round %d: %+v did not receive the task: %v", round, tc.served, err)
				}
				if lease.Task.RunID != task.RunID || lease.Task.TargetWorkerIdentity != task.TargetWorkerIdentity ||
					lease.Task.TaskQueue != QueuePayments || lease.Task.Namespace != namespace {
					t.Fatalf("round %d: lease task = %+v, want %+v", round, lease.Task, task)
				}
				if round == 0 {
					if err := router.Nack(context.Background(), lease); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := router.Ack(context.Background(), lease); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
