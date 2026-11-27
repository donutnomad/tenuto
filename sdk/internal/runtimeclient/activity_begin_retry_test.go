// BeginActivityTask 的应答丢失后客户端按幂等调用重发：重发带着同一个 RequestID，Server 认作同一次开始，
// 返回 Execute=true 与原来的开始时间（Temporal recordActivityTaskStarted 的 RequestId 语义），Worker 照常执行。
package runtimeclient_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// dropFirstBeginResponse 让第一次 Begin 请求到达 Server 并被处理，然后把应答换成连接中断。
type dropFirstBeginResponse struct {
	next    http.RoundTripper
	dropped atomic.Bool
	begins  atomic.Int64
}

func (d *dropFirstBeginResponse) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := d.next.RoundTrip(req)
	if req.URL.Path != runtimeapi.ActivityRuntimeBeginPath || err != nil {
		return resp, err
	}
	d.begins.Add(1)
	if d.dropped.CompareAndSwap(false, true) {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, io.ErrUnexpectedEOF // 应答在途中丢失（LB 重置、Server 滚动重启）
	}
	return resp, nil
}

func TestBeginRetryAfterLostResponseExecutesAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(ctx, protocol.ActivityWorkerVersion{
		Identity: "act-worker", TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: []string{"activity.first"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "begin-retry"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Hour`},
	)); err != nil {
		t.Fatal(err)
	}
	wfWorker := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "wf-worker", BuildID: "b1"})
	wctx, cancel := context.WithCancel(ctx)
	wfWorker.Start(wctx)
	defer func() { cancel(); wfWorker.Stop() }()

	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	defer httpServer.Close()
	transport := &dropFirstBeginResponse{next: http.DefaultTransport}
	client := runtimeclient.NewActivityRuntimeHTTPClient(httpServer.URL, &http.Client{Transport: transport}, runtimeclient.RuntimeHTTPClientOptions{PollTimeout: 5 * time.Second})

	run, err := server.StartWorkflow(ctx, "", "begin-retry", "begin-retry", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	pollCtx := protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "act-worker", BuildID: "b1"})
	lease, err := client.PollActivityTask(pollCtx, protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	task := lease.Task
	task.WorkerIdentity, task.WorkerBuildID = "act-worker", "b1"

	registry := activity.NewRegistry()
	var executed atomic.Int64
	if err := registry.Register("activity.first", func(context.Context, map[string]any) (map[string]any, error) {
		executed.Add(1)
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	outcome, executionErr, reportErr := executor.RunActivityTask(ctx, task, client, registry, func() {})
	if outcome != "success" || executionErr != nil || reportErr != nil || transport.begins.Load() != 2 {
		t.Fatalf("RunActivityTask outcome=%q executionErr=%v reportErr=%v begin requests=%d, want success after 2 begin requests",
			outcome, executionErr, reportErr, transport.begins.Load())
	}

	state, ok, err := mgr.GetActivityState(run.ID, task.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("activity state: ok=%v err=%v", ok, err)
	}
	if executed.Load() != 1 || state.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("activity executed %d times, state %s; want 1 execution and completed", executed.Load(), state.Status)
	}
}

// 另一份投递（RequestID 不同）在 attempt 已开始后得到 attempt_already_started，不执行。
func TestBeginFromAnotherDeliveryIsRefusedAfterStart(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(ctx, protocol.ActivityWorkerVersion{
		Identity: "act-worker", TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: []string{"activity.first"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "begin-other-delivery"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Hour`},
	)); err != nil {
		t.Fatal(err)
	}
	wfWorker := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "wf-worker", BuildID: "b1"})
	wctx, cancel := context.WithCancel(ctx)
	wfWorker.Start(wctx)
	defer func() { cancel(); wfWorker.Stop() }()
	if _, err := server.StartWorkflow(ctx, "", "begin-other-delivery", "begin-other-delivery", "", nil); err != nil {
		t.Fatal(err)
	}
	lease, err := server.PollActivityTask(protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "act-worker", BuildID: "b1"}), protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	task := lease.Task
	task.WorkerIdentity, task.WorkerBuildID = "act-worker", "b1"
	first, second := task, task
	first.RequestID, second.RequestID = "request-a", "request-b"
	runtime := engine.NewActivityRuntimeService(server)
	started, err := runtime.BeginActivityTask(ctx, first)
	if err != nil || !started.Execute {
		t.Fatalf("first begin = %+v, %v", started, err)
	}
	retried, err := runtime.BeginActivityTask(ctx, first)
	if err != nil || !retried.Execute || !retried.StartedTime.Equal(started.StartedTime) {
		t.Fatalf("retried begin = %+v, %v; want Execute with started time %v", retried, err, started.StartedTime)
	}
	other, err := runtime.BeginActivityTask(ctx, second)
	if err != nil || other.Execute || other.Reason != "attempt_already_started" {
		t.Fatalf("begin from another delivery = %+v, %v; want attempt_already_started", other, err)
	}
}
