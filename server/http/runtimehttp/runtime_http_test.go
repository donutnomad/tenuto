package runtimehttp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestRuntimeHTTPHandlerMountsEveryRuntime(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	paths := []string{
		runtimeapi.ActivityRuntimeBeginPath,
		runtimeapi.WorkflowRuntimeStartPath,
		runtimeapi.WorkflowTaskRuntimePollPath,
	}
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s status = %d, want %d", path, recorder.Code, http.StatusMethodNotAllowed)
		}
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/runtime/unknown", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown runtime status = %d", recorder.Code)
	}
}

func TestRuntimeHTTPTaskPollsExpireAndRenew(t *testing.T) {
	queue := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: queue, ActivityRouter: queue.activities()})
	defer server.Stop()
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	if _, err := pollWorkflowTask(context.Background(), httpServer.URL, 10*time.Millisecond); !errors.Is(err, protocol.ErrTaskPollTimeout) {
		t.Fatalf("workflow poll error = %v", err)
	}
	ctx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Namespace: "tenant-a"})
	if _, err := pollActivityTask(ctx, httpServer.URL, "payments", 10*time.Millisecond); !errors.Is(err, protocol.ErrTaskPollTimeout) {
		t.Fatalf("activity poll error = %v", err)
	}
}

func TestActivityRuntimeHTTPQueueLeaseLifecyclePreservesNamespace(t *testing.T) {
	workflowQueue := newRuntimeHTTPTestQueue()
	activityQueue := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: workflowQueue, ActivityRouter: activityQueue.activities()})
	defer server.Stop()
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	ctx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Namespace: "tenant-a"})
	activityQueue.enqueue(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-a", ScheduledEventID: 7, TaskQueue: "payments"})
	lease, err := pollActivityTask(ctx, httpServer.URL, "payments", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.Namespace != "tenant-a" || lease.Task.TaskQueue != "payments" {
		t.Fatalf("lease route = %q/%q", lease.Task.Namespace, lease.Task.TaskQueue)
	}
	if lease.Task.TaskToken == "" || lease.Task.TaskToken != lease.Task.TaskToken {
		t.Fatalf("lease token was not normalized: %+v", lease)
	}
	if err := ackActivityTask(context.Background(), httpServer.URL, lease); err != nil {
		t.Fatal(err)
	}

	activityQueue.enqueue(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-a", ScheduledEventID: 8, TaskQueue: "payments"})
	lease, err = pollActivityTask(ctx, httpServer.URL, "payments", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := nackActivityTask(context.Background(), httpServer.URL, lease); err != nil {
		t.Fatal(err)
	}
	acked, nacked := activityQueue.counts()
	if acked != 1 || nacked != 1 {
		t.Fatalf("lease acknowledgements = ack:%d nack:%d", acked, nacked)
	}
}

func TestRuntimeHTTPHandlerUsesInjectedTaskTokenCodec(t *testing.T) {
	workflowQueue := newRuntimeHTTPTestQueue()
	activityQueue := newRuntimeHTTPTestQueue()
	codec, err := dispatch.NewTaskTokenCodec([]byte("runtime-http-secret-0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: workflowQueue, ActivityRouter: activityQueue.activities(), TaskTokens: codec})
	defer server.Stop()
	otherCodec, err := dispatch.NewTaskTokenCodec([]byte("different-codec-secret-0123456789ab"))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	activityQueue.enqueue(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-codec", ScheduledEventID: 7, TaskQueue: "payments"})
	ctx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Namespace: "tenant-a"})
	lease, err := pollActivityTask(ctx, httpServer.URL, "payments", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(lease.Task.TaskToken); err != nil {
		t.Fatalf("injected codec rejected token: %v", err)
	}
	if _, err := otherCodec.Decode(lease.Task.TaskToken); err == nil {
		t.Fatal("runtime token was accepted by a different codec")
	}
}

func TestRuntimeHTTPLeaseAcknowledgementsRejectTampering(t *testing.T) {
	workflowQueue := newRuntimeHTTPTestQueue()
	activityQueue := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: workflowQueue, ActivityRouter: activityQueue.activities()})
	defer server.Stop()
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	activityCtx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Namespace: "tenant-a"})
	activityQueue.enqueue(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-activity-lease", ScheduledEventID: 7, TaskQueue: "payments"})
	activityLease, err := pollActivityTask(activityCtx, httpServer.URL, "payments", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tamperedActivityLease := activityLease
	tamperedActivityLease.Token += "-tampered"
	if err := ackActivityTask(context.Background(), httpServer.URL, tamperedActivityLease); err == nil {
		t.Fatal("activity acknowledgement accepted a tampered queue lease")
	}
	acked, nacked := activityQueue.counts()
	if acked != 0 || nacked != 0 {
		t.Fatalf("tampered activity lease reached queue: ack=%d nack=%d", acked, nacked)
	}
	if err := ackActivityTask(context.Background(), httpServer.URL, activityLease); err != nil {
		t.Fatal(err)
	}

	codec, err := dispatch.NewTaskTokenCodec([]byte("workflow-lease-test-secret-0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	upstreamCodec, err := dispatch.NewTaskTokenCodec([]byte("upstream-runtime-secret-0123456789abcd"))
	if err != nil {
		t.Fatal(err)
	}
	workflowTask := protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-workflow-lease", ScheduledEventID: 11}
	workflowTask.TaskToken, err = upstreamCodec.EncodeLease(workflowTask, "workflow-queue-lease")
	if err != nil {
		t.Fatal(err)
	}
	workflowRuntime := &workflowLeaseRuntimeFake{lease: protocol.TaskLease{Task: workflowTask, Token: "workflow-queue-lease"}}
	workflowHTTPServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(workflowRuntime, codec, slog.New(slog.DiscardHandler)))
	defer workflowHTTPServer.Close()
	workflowLease, err := pollWorkflowTask(context.Background(), workflowHTTPServer.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.DecodeLease(workflowLease.Task.TaskToken, workflowLease.Token); err != nil {
		t.Fatalf("handler codec rejected its workflow lease: %v", err)
	}
	if _, err := upstreamCodec.Decode(workflowLease.Task.TaskToken); err == nil {
		t.Fatal("workflow poll retained the upstream runtime codec")
	}
	tamperedWorkflowLease := workflowLease
	tamperedWorkflowLease.Task.TaskToken = activityLease.Task.TaskToken
	if err := nackWorkflowTask(context.Background(), workflowHTTPServer.URL, tamperedWorkflowLease); err == nil {
		t.Fatal("workflow nack accepted a token from another lease")
	}
	if workflowRuntime.acked != 0 || workflowRuntime.nacked != 0 {
		t.Fatalf("tampered workflow lease reached queue: ack=%d nack=%d", workflowRuntime.acked, workflowRuntime.nacked)
	}
	if err := nackWorkflowTask(context.Background(), workflowHTTPServer.URL, workflowLease); err != nil {
		t.Fatal(err)
	}
	if workflowRuntime.nacked != 1 {
		t.Fatalf("workflow nack count = %d", workflowRuntime.nacked)
	}
}

type workflowLeaseRuntimeFake struct {
	engine.WorkflowTaskRuntimeAPI
	lease  protocol.TaskLease
	acked  int
	nacked int
}

func (f *workflowLeaseRuntimeFake) PollWorkflowTask(context.Context) (protocol.TaskLease, error) {
	return f.lease, nil
}

func (f *workflowLeaseRuntimeFake) AckWorkflowTask(context.Context, protocol.TaskLease) error {
	f.acked++
	return nil
}

func (f *workflowLeaseRuntimeFake) NackWorkflowTask(context.Context, protocol.TaskLease) error {
	f.nacked++
	return nil
}

type runtimeHTTPTestQueue struct {
	tasks chan protocol.TaskLease
	mu    sync.Mutex
	acked int
	nack  int
}

func newRuntimeHTTPTestQueue() *runtimeHTTPTestQueue {
	return &runtimeHTTPTestQueue{tasks: make(chan protocol.TaskLease, 4)}
}

func (q *runtimeHTTPTestQueue) enqueue(task protocol.Task) {
	q.tasks <- protocol.TaskLease{Task: task}
}

func (q *runtimeHTTPTestQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	select {
	case q.tasks <- protocol.TaskLease{Task: task}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sticky 任务在这个测试队列里与普通任务走同一通道，任何 Worker 都能领取。
func (q *runtimeHTTPTestQueue) EnqueueSticky(ctx context.Context, _ string, task protocol.Task) error {
	return q.Enqueue(ctx, task)
}

func (q *runtimeHTTPTestQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (*runtimeHTTPTestQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*runtimeHTTPTestQueue) Durable() bool                      { return false }

func (q *runtimeHTTPTestQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	select {
	case lease := <-q.tasks:
		return lease, nil
	case <-ctx.Done():
		return protocol.TaskLease{}, ctx.Err()
	}
}

func (q *runtimeHTTPTestQueue) Ack(context.Context, protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked++
	return nil
}

func (q *runtimeHTTPTestQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	q.nack++
	q.mu.Unlock()
	select {
	case q.tasks <- lease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *runtimeHTTPTestQueue) counts() (int, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked, q.nack
}

// activities 让这条测试队列承接全部 Activity task queue，出队不区分 task queue 与 Worker。
func (q *runtimeHTTPTestQueue) activities() dispatch.ActivityTaskQueueRouter {
	return runtimeHTTPActivityRouter{queue: q}
}

type runtimeHTTPActivityRouter struct {
	queue *runtimeHTTPTestQueue
}

func (r runtimeHTTPActivityRouter) Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error {
	task.TaskQueue = taskQueue
	return r.queue.Enqueue(ctx, task)
}

// Dequeue 给直接塞进测试队列、没有 namespace 的任务补上 poll 方的 namespace，与按 namespace 分通道的队列一致。
func (r runtimeHTTPActivityRouter) Dequeue(ctx context.Context, _ string) (protocol.TaskLease, error) {
	lease, err := r.queue.Dequeue(ctx)
	if err == nil && lease.Task.Namespace == "" {
		lease.Task.Namespace = protocol.NamespaceFromContext(ctx)
	}
	return lease, err
}

func (r runtimeHTTPActivityRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Ack(ctx, lease)
}

func (r runtimeHTTPActivityRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return r.queue.Nack(ctx, lease)
}

func (r runtimeHTTPActivityRouter) Durable() bool { return false }

// testTaskTokens 返回不经 Server 的用例共用的 task token codec。
func testTaskTokens() *dispatch.TaskTokenCodec {
	codec, err := dispatch.NewTaskTokenCodec([]byte("runtimehttp-test-task-token-secret-0123456789"))
	if err != nil {
		panic(err)
	}
	return codec
}

// activityRuntimeForTest 让只实现任务执行过程的测试替身满足 ActivityRuntime；队列方法不在这些用例的路径上。
type activityRuntimeForTest struct {
	activitytasks.API
	engine.ActivityWorkerQueueAPI
}
