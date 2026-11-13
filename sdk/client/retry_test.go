package client_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// fault 是对某个路径的下一次请求注入的故障。
type fault int

const (
	// lostResponse：Server 处理完请求，应答在路上丢失（连接被关闭）。
	lostResponse fault = iota + 1
	// droppedConnection：请求到达后连接立即断开，Server 没有处理它（副本重启）。
	droppedConnection
	// unavailable：网关应答 503，Server 没有处理它。
	unavailable
)

// faultyServer 是真实的 runtime HTTP Server 前面加一层按路径注入故障的代理。
type faultyServer struct {
	server *engine.Server
	client client.Client

	mu       sync.Mutex
	pending  map[string][]fault
	attempts map[string]int
}

func newFaultyServer(t *testing.T) *faultyServer {
	t.Helper()
	f := &faultyServer{pending: map[string][]fault{}, attempts: map[string]int{}}
	f.server = assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
	})
	serverCtx, stopServer := context.WithCancel(context.Background())
	runtime := runtimehttp.NewRuntimeHTTPHandler(f.server)
	httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.attempts[r.URL.Path]++
		var injected fault
		if queue := f.pending[r.URL.Path]; len(queue) > 0 {
			injected, f.pending[r.URL.Path] = queue[0], queue[1:]
		}
		f.mu.Unlock()
		switch injected {
		case unavailable:
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			return
		case lostResponse:
			runtime.ServeHTTP(httptest.NewRecorder(), r)
			fallthrough
		case droppedConnection:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		runtime.ServeHTTP(w, r)
	}))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	c, err := client.Dial(client.Options{HostPort: httpServer.URL, Retry: client.RetryOptions{InitialInterval: 10 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	f.client = c
	t.Cleanup(func() {
		c.Close()
		stopServer()
		httpServer.Close()
		f.server.Stop()
	})
	config := generateWorkflow(t, "waiting", waitingSrc)
	if _, err := f.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, config); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *faultyServer) inject(path string, faults ...fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending[path] = append(f.pending[path], faults...)
}

func (f *faultyServer) attemptsOf(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[path]
}

// 启动的应答丢失后 SDK 重发：Server 按 request ID 认出重发，返回同一个 Run，不报 AlreadyStarted、不多启动一个 Run。
// Workflow ID 为空时由 SDK 生成，重发用同一个。
func TestExecuteWorkflowRetriesLostResponse(t *testing.T) {
	f := newFaultyServer(t)
	for _, id := range []string{"start-lost-response", ""} {
		f.inject(runtimeapi.WorkflowRuntimeStartPath, lostResponse, unavailable)
		before := f.attemptsOf(runtimeapi.WorkflowRuntimeStartPath)
		run, err := f.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: id}, "waiting")
		if err != nil {
			t.Fatalf("ExecuteWorkflow(id=%q): %v", id, err)
		}
		if got := f.attemptsOf(runtimeapi.WorkflowRuntimeStartPath) - before; got != 3 {
			t.Fatalf("start attempts = %d, want 3", got)
		}
		listed, err := f.client.ListWorkflow(context.Background(), client.ListWorkflowOptions{WorkflowID: run.GetID()})
		if err != nil {
			t.Fatal(err)
		}
		if listed.Total != 1 || listed.Executions[0].RunID != run.GetRunID() {
			t.Fatalf("runs of %s = %+v, want only %s", run.GetID(), listed.Executions, run.GetRunID())
		}
	}
}

// Signal 的应答丢失后 SDK 重发，Server 按 request ID 只记一条 WorkflowSignaled。
func TestSignalWorkflowRetriesLostResponse(t *testing.T) {
	f := newFaultyServer(t)
	run, err := f.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: "signal-lost-response"}, "waiting")
	if err != nil {
		t.Fatal(err)
	}
	f.inject(runtimeapi.WorkflowRuntimeSignalPath, lostResponse, droppedConnection)
	if err := f.client.SignalWorkflow(context.Background(), run.GetID(), "", "add", map[string]any{"item": "a"}); err != nil {
		t.Fatal(err)
	}
	if got := f.attemptsOf(runtimeapi.WorkflowRuntimeSignalPath); got != 3 {
		t.Fatalf("signal attempts = %d, want 3", got)
	}
	events, err := f.server.GetEvents(run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	signals := 0
	for _, event := range events {
		if event.EventType() == history.EventWorkflowSignaled {
			signals++
		}
	}
	if signals != 1 {
		t.Fatalf("WorkflowSignaled events = %d, want 1", signals)
	}
}

// Get 的长轮询遇到连接断开与 503（副本滚动重启）时继续等待，Run 结束后返回结果。
func TestGetSurvivesDroppedLongPolls(t *testing.T) {
	f := newFaultyServer(t)
	run, err := f.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: "get-dropped"}, "waiting")
	if err != nil {
		t.Fatal(err)
	}
	f.inject(runtimeapi.WorkflowRuntimeWaitClosedPath, droppedConnection, unavailable, droppedConnection)
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = f.server.TerminateWorkflow(context.Background(), "", run.GetID(), run.GetRunID(), "rolled")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = run.Get(ctx, nil)
	var terminated *client.TerminatedError
	if !errors.As(err, &terminated) || terminated.Reason != "rolled" {
		t.Fatalf("Get err = %v, want TerminatedError", err)
	}
	if got := f.attemptsOf(runtimeapi.WorkflowRuntimeWaitClosedPath); got < 4 {
		t.Fatalf("wait-closed attempts = %d, want at least 4", got)
	}
}

// 重发会再执行一次的调用（Schedule 的 Trigger）在请求可能已被处理时不重发：错误返回给调用方，只启动了一个 Run。
// 503 表示请求没有被处理，照常重试。
func TestTriggerScheduleIsNotRetriedAfterLostResponse(t *testing.T) {
	f := newFaultyServer(t)
	handle, err := f.client.ScheduleClient().Create(context.Background(), client.ScheduleOptions{
		WorkflowType: "waiting", CronExpression: "0 0 1 1 *", OverlapPolicy: client.ScheduleOverlapPolicyAllowAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.inject(runtimeapi.WorkflowRuntimeTriggerSchedulePath, lostResponse)
	if _, err := handle.Trigger(context.Background()); err == nil {
		t.Fatal("Trigger succeeded although its response was lost")
	}
	if got := f.attemptsOf(runtimeapi.WorkflowRuntimeTriggerSchedulePath); got != 1 {
		t.Fatalf("trigger attempts after a lost response = %d, want 1", got)
	}
	// 手动触发的 Workflow ID 取触发时刻（秒级），下一次触发放到下一秒。
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second)))
	f.inject(runtimeapi.WorkflowRuntimeTriggerSchedulePath, unavailable)
	if _, err := handle.Trigger(context.Background()); err != nil {
		t.Fatalf("Trigger after 503: %v", err)
	}
	if got := f.attemptsOf(runtimeapi.WorkflowRuntimeTriggerSchedulePath); got != 3 {
		t.Fatalf("trigger attempts = %d, want 3", got)
	}
	description, err := handle.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if description.RunCount != 2 {
		t.Fatalf("schedule run count = %d, want 2 (the lost trigger and the retried one)", description.RunCount)
	}
}

// 4xx 是确定的结果，不重试。
func TestClientErrorsAreNotRetried(t *testing.T) {
	f := newFaultyServer(t)
	err := f.client.SignalWorkflow(context.Background(), "missing-workflow", "", "add", nil)
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("signal to a missing workflow: err = %v, want ErrNotFound", err)
	}
	if got := f.attemptsOf(runtimeapi.WorkflowRuntimeSignalPath); got != 1 {
		t.Fatalf("signal attempts = %d, want 1", got)
	}
}
