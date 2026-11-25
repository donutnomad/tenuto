package worker

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 远程 Worker 经 HTTP 的增量激活协议使用粘性执行：第二个 Workflow Task 命中缓存，只重放新增事件。
func TestRemoteWorkerStickyExecutionUsesIncrementalHTTPProtocol(t *testing.T) {
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: queueActivityRouter{queue: activityTasks},
	})
	t.Cleanup(server.Stop)
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "sticky-activity-worker", TaskQueues: []string{protocol.DefaultActivityTaskQueue},
		ActivityTypes: []string{"activity.first", "activity.second"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "remote-sticky-two-activities"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Minute`},
	)); err != nil {
		t.Fatal(err)
	}
	handlerCtx, cancelHandler := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(runtimehttp.NewRuntimeHTTPHandler(server))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return handlerCtx }
	httpServer.Start()
	t.Cleanup(func() {
		cancelHandler()
		httpServer.Close()
	})

	c, err := client.Dial(client.Options{HostPort: httpServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	w, err := newWorker(c, Options{Role: RoleWorkflow, Identity: "remote-sticky-worker-1", BuildID: "build-1"}, cache)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	t.Cleanup(func() {
		cancel()
		w.Stop()
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
	defer readyCancel()
	if err := w.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "remote-sticky-two-activities"}, "remote-sticky-two-activities")
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueActivityTask(t, activityTasks)
	completeActivityTask(t, server, first, map[string]any{"first": "done"})
	second := dequeueActivityTask(t, activityTasks)
	if second.RunID != run.GetRunID() || second.ActivityID != "#a2" {
		t.Fatalf("second activity task = %+v", second)
	}
	snapshot := cache.Snapshot()
	if snapshot.Hits < 1 || snapshot.Admissions != 1 {
		t.Fatalf("remote sticky cache snapshot = %+v", snapshot)
	}
	completeActivityTask(t, server, second, map[string]any{"second": "done"})
	getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
	defer getCancel()
	if err := run.Get(getCtx, nil); err != nil {
		t.Fatal(err)
	}
}

func dequeueActivityTask(t *testing.T, queue *memory.TaskQueue) protocol.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return lease.Task
}

func completeActivityTask(t *testing.T, server *engine.Server, task protocol.Task, output map[string]any) {
	t.Helper()
	if _, err := server.HandleActivityStartedWithMetadata(task, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, output); err != nil {
		t.Fatal(err)
	}
}

// queueActivityRouter 让一条测试队列承接全部 Activity task queue，测试直接从队列取出 Activity Task。
type queueActivityRouter struct {
	queue *memory.TaskQueue
}

func (r queueActivityRouter) Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error {
	task.TaskQueue = taskQueue
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	return r.queue.Enqueue(ctx, task)
}

func (r queueActivityRouter) Dequeue(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return r.queue.Dequeue(ctx)
}

func (r queueActivityRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Ack(ctx, lease)
}

func (r queueActivityRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Nack(ctx, lease)
}

func (r queueActivityRouter) Durable() bool { return r.queue.Durable() }
