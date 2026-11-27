package runtimehttp

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Worker 停止时由 Server 结束它的长轮询
// ============================================
// Worker 先断开长轮询会丢投递：Server 察觉断开之前出队的任务写进已关闭的连接。Worker 停止时先调用
// shutdown-worker：同一 session 在途的领取长轮询以 204 结束（连接仍然连着），之后到达的领取请求立即 204；
// 其他 session（例如重启后复用同一 identity 的新实例）不受影响。
// ============================================

func TestShutdownWorkerEndsInFlightPollsOfTheSession(t *testing.T) {
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	httpServer := httptest.NewServer(NewRuntimeHTTPHandler(server))
	defer httpServer.Close()

	stopping := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Session: "s-1"})
	restarted := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "worker-a", Session: "s-2"})
	polls := []struct {
		name string
		poll func(ctx context.Context, timeout time.Duration) error
	}{
		{"workflow", func(ctx context.Context, timeout time.Duration) error {
			_, err := pollWorkflowTask(ctx, httpServer.URL, timeout)
			return err
		}},
		{"activity", func(ctx context.Context, timeout time.Duration) error {
			_, err := pollActivityTask(ctx, httpServer.URL, "q", timeout)
			return err
		}},
	}
	done := make(chan error, len(polls))
	for _, p := range polls {
		go func() { done <- p.poll(stopping, 30*time.Second) }()
	}
	time.Sleep(100 * time.Millisecond) // 让两个长轮询进入等待
	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeShutdownWorkerPath,
		runtimeapi.WorkflowRuntimeShutdownWorkerRequest{Identity: "worker-a", Session: "s-1"}, nil); err != nil {
		t.Fatal(err)
	}
	for range polls {
		select {
		case err := <-done:
			if !errors.Is(err, protocol.ErrTaskPollTimeout) {
				t.Fatalf("在途长轮询应以 204 结束，得到 %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown-worker 之后长轮询没有结束")
		}
	}
	for _, p := range polls {
		started := time.Now()
		if err := p.poll(stopping, 30*time.Second); !errors.Is(err, protocol.ErrTaskPollTimeout) || time.Since(started) > time.Second {
			t.Fatalf("%s: 已关闭 session 的新 poll 应立即 204，得到 %v（%v）", p.name, err, time.Since(started))
		}
		if err := p.poll(restarted, 50*time.Millisecond); !errors.Is(err, protocol.ErrTaskPollTimeout) {
			t.Fatalf("%s: 其他 session 的 poll 应照常长轮询到期，得到 %v", p.name, err)
		}
	}
	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeShutdownWorkerPath,
		runtimeapi.WorkflowRuntimeShutdownWorkerRequest{Identity: "worker-a"}, nil); err == nil {
		t.Fatal("缺少 session 的 shutdown-worker 应被拒绝")
	}
}
