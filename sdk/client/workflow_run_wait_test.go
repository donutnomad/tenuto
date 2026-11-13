package client_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Get 经 wait-closed 长轮询等待：Run 结束后很快返回，不随等待时长退避。Run 在 Get 开始 1.6s 之后结束，
// 此前按 50ms 起翻倍到 1s 的轮询要到约 2.55s 才再读一次，晚约 950ms。
// 两种结束方：Client 连接的 Server 本身，与共用同一存储的另一个 Server 副本（读取间隔 100ms）。
func TestGetReturnsPromptlyAfterRunCloses(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	other := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: env.executions},
		WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
	})
	t.Cleanup(other.Stop)

	for _, tc := range []struct {
		name   string
		closer *engine.Server
	}{
		{"same replica", env.server},
		{"other replica", other},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := env.execute("waiting", "get-latency-"+tc.name)
			returned := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				returned <- run.Get(ctx, nil)
			}()
			time.Sleep(1600 * time.Millisecond)
			if err := tc.closer.TerminateWorkflow(context.Background(), "", run.GetID(), run.GetRunID(), "latency"); err != nil {
				t.Fatal(err)
			}
			closedAt := time.Now()
			select {
			case err := <-returned:
				var terminated *client.TerminatedError
				if !errors.As(err, &terminated) || terminated.Reason != "latency" {
					t.Fatalf("Get err = %v, want TerminatedError", err)
				}
				if latency := time.Since(closedAt); latency > 400*time.Millisecond {
					t.Fatalf("Get returned %v after the run closed", latency)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Get did not return after the run closed")
			}
		})
	}
}

// wait-closed 的 wire 语义：等待到期时 Run 仍在运行应答 204 且没有响应体；Run 不存在时 404。
func TestWaitClosedWireSemantics(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	run := env.execute("waiting", "wait-closed-wire")
	post := func(body string) *http.Response {
		t.Helper()
		resp, err := http.Post(env.url+"/runtime/workflow/wait-closed", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	started := time.Now()
	if resp := post(`{"run_id":"` + run.GetRunID() + `","timeout_millis":50}`); resp.StatusCode != http.StatusNoContent || resp.ContentLength > 0 {
		t.Fatalf("running run: status %d, length %d", resp.StatusCode, resp.ContentLength)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("a 50ms wait took %v", elapsed)
	}
	if resp := post(`{"run_id":"missing","timeout_millis":50}`); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing run: status %d", resp.StatusCode)
	}
}
