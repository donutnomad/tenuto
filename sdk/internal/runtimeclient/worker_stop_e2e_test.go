package runtimeclient_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// pollEnteredRouter 在每次 Dequeue 进入等待时通知测试。
type pollEnteredRouter struct {
	dispatch.ActivityTaskQueueRouter
	entered chan struct{}
}

func (r pollEnteredRouter) Dequeue(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	return r.ActivityTaskQueueRouter.Dequeue(ctx, taskQueue)
}

// onPollCancel 在 Activity poll 请求的 context 被取消时执行 armed 的回调（只执行一次）：模拟 Worker 断开长轮询的
// 同一时刻有新任务入队。
type onPollCancel struct {
	base  http.RoundTripper
	mu    sync.Mutex
	armed func()
}

func (t *onPollCancel) arm(fn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.armed = fn
}

func (t *onPollCancel) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == runtimeapi.ActivityRuntimePollPath {
		context.AfterFunc(req.Context(), func() {
			t.mu.Lock()
			fn := t.armed
			t.armed = nil
			t.mu.Unlock()
			if fn != nil {
				fn()
			}
		})
	}
	return t.base.RoundTrip(req)
}

// Worker 经 HTTP 停止、断开长轮询的同一时刻入队的任务，必须由下一个 poll 领到。修复前 Worker.Stop 直接断开长轮询，Server 察觉
// 断开之前 Dequeue 出队这份任务并把应答写进已关闭的连接，任务要等可见性超时才重新投递。
func TestStoppedHTTPWorkerDoesNotSwallowNextActivityTask(t *testing.T) {
	router := pollEnteredRouter{ActivityTaskQueueRouter: memory.NewActivityQueueRouter(16), entered: make(chan struct{}, 1)}
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: router})
	defer server.Stop()
	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	defer httpServer.Close()
	options := runtimeclient.RuntimeHTTPClientOptions{}
	transport := &onPollCancel{base: httpServer.Client().Transport}
	activities := runtimeclient.NewActivityRuntimeHTTPClient(httpServer.URL, &http.Client{Transport: transport}, options)
	control := runtimeclient.NewWorkflowRuntimeHTTPClient(httpServer.URL, httpServer.Client(), options)

	for i := range 5 {
		w := worker.New(nil, activities, activities, nil, worker.WorkerOptions{
			Role:               worker.WorkerRoleActivity,
			Identity:           "worker-a",
			ActivityTaskQueues: []string{"q"},
			PollShutdown:       control,
		})
		w.Start(context.Background())
		select {
		case <-router.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("worker 没有开始长轮询")
		}
		task := protocol.Task{Type: protocol.TaskTypeActivity, RunID: fmt.Sprintf("run-%d", i), ScheduledEventID: 1, TaskQueue: "q"}
		enqueued := make(chan error, 1)
		transport.arm(func() { enqueued <- router.Enqueue(context.Background(), "q", task) })
		w.Stop()
		if err := <-enqueued; err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		lease, err := activities.PollActivityTask(protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "worker-b"}), "q")
		cancel()
		<-router.entered
		if err != nil || lease.Task.RunID != task.RunID {
			t.Fatalf("iter %d: 停止的 Worker 吞掉了任务：poll = %+v, %v", i, lease.Task.RunID, err)
		}
		if err := activities.AckActivityTask(context.Background(), lease); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}
