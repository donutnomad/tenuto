package runtimehttp

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 长轮询在任务交出之前断开
// ============================================
// Worker 停止时取消在途的长轮询。Server 若恰好在连接断开之后领到任务，应答送不到：
// Workflow Task 已 started，Activity 租约已出队。Server 发现请求已结束时立即交还任务，
// 不让它等 Workflow Task 的 start-to-close 超时或队列的可见性超时。
// ============================================

// disconnectedWorkflowPoll 在客户端断开之后才执行真实的 poll，并在交还的 Ack 之后通知测试。
type disconnectedWorkflowPoll struct {
	*engine.Server
	polling chan struct{}
	acked   chan struct{}
}

func (p disconnectedWorkflowPoll) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	close(p.polling)
	<-ctx.Done()
	return p.Server.PollWorkflowTask(context.WithoutCancel(ctx))
}

func (p disconnectedWorkflowPoll) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	defer close(p.acked)
	return p.Server.AckWorkflowTask(ctx, lease)
}

func TestWorkflowTaskPolledAfterDisconnectIsReturnedAtOnce(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: newRuntimeHTTPTestQueue().activities()})
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		Type: "disconnect-workflow",
		Program: programpkg.WLangProgram{
			Schema:           programpkg.WLangProgramSchemaV2,
			Lang:             programpkg.WLangLanguageV1,
			Program:          []any{map[string]any{"return": nil}},
			HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
		},
		Activities: map[string]programpkg.ActivityRef{},
	}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "disconnect-workflow", "disconnect-workflow", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	runtime := disconnectedWorkflowPoll{Server: server, polling: make(chan struct{}), acked: make(chan struct{})}
	httpServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(runtime, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()

	ctx, cancel := context.WithCancel(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "stopping-worker", WorkflowCapabilities: programpkg.CurrentWLangCapabilities(),
	}))
	pollDone := make(chan error, 1)
	go func() {
		_, err := pollWorkflowTask(ctx, httpServer.URL, 30*time.Second)
		pollDone <- err
	}()
	<-runtime.polling
	cancel()
	if err := <-pollDone; err == nil {
		t.Fatal("取消的 poll 不应收到任务")
	}
	select {
	case <-runtime.acked:
	case <-time.After(5 * time.Second):
		t.Fatal("断开之后领到的 Workflow Task 没有交还")
	}

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if started, timedOut, scheduled := countRuntimeEvents(history, historypkg.EventWorkflowTaskStarted),
		countRuntimeEvents(history, historypkg.EventWorkflowTaskTimedOut),
		countRuntimeEvents(history, historypkg.EventWorkflowTaskScheduled); started != 1 || timedOut != 1 || scheduled != 2 {
		t.Fatalf("started=%d timedOut=%d scheduled=%d，want 1/1/2：%+v", started, timedOut, scheduled, history)
	}
	if queued := len(workflowTasks.tasks); queued != 1 {
		t.Fatalf("交还之后队列应有一个新的 Workflow Task，实际 %d", queued)
	}
}

// disconnectedActivityPoll 在客户端断开之后才交出 Activity 租约，记录退回。
type disconnectedActivityPoll struct {
	polling chan struct{}
	nacked  chan protocol.TaskLease
}

func (p disconnectedActivityPoll) PollActivityTask(ctx context.Context, _ string) (protocol.TaskLease, error) {
	close(p.polling)
	<-ctx.Done()
	return protocol.TaskLease{Token: "lease-1", Task: protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1"}}, nil
}

func (disconnectedActivityPoll) AckActivityTask(context.Context, protocol.TaskLease) error {
	return nil
}

func (p disconnectedActivityPoll) NackActivityTask(_ context.Context, lease protocol.TaskLease) error {
	p.nacked <- lease
	return nil
}

func TestActivityTaskPolledAfterDisconnectIsNackedAtOnce(t *testing.T) {
	queue := disconnectedActivityPoll{polling: make(chan struct{}), nacked: make(chan protocol.TaskLease, 1)}
	httpServer := httptest.NewServer(NewActivityRuntimeHTTPHandler(activityRuntimeForTest{ActivityWorkerQueueAPI: queue}, testTaskTokens(), slog.New(slog.DiscardHandler)))
	defer httpServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	pollDone := make(chan error, 1)
	go func() {
		_, err := pollActivityTask(ctx, httpServer.URL, "default", 30*time.Second)
		pollDone <- err
	}()
	<-queue.polling
	cancel()
	if err := <-pollDone; err == nil {
		t.Fatal("取消的 poll 不应收到任务")
	}
	select {
	case lease := <-queue.nacked:
		if lease.Token != "lease-1" {
			t.Fatalf("退回的租约 = %+v", lease)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("断开之后出队的 Activity 租约没有退回")
	}
}
