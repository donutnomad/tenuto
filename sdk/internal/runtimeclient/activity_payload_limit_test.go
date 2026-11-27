// Activity 调用的请求体只带 task token 与 Worker 身份：大小与 Activity 输入无关。输出与心跳详情超过 Server 的
// 载荷上限时，Worker 上报不可重试的 PayloadSizeLimitExceeded 失败，不发送超限的载荷。
package runtimeclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// requestRecorder 记录每个 runtime 路径的请求次数与最大请求体。
type requestRecorder struct {
	next http.RoundTripper
	mu   sync.Mutex
	hits map[string]int
	size map[string]int64
}

func (r *requestRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.next.RoundTrip(req)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits[req.URL.Path]++
	r.size[req.URL.Path] = max(r.size[req.URL.Path], req.ContentLength)
	return resp, err
}

func (r *requestRecorder) stats(path string) (hits int, maxSize int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits[path], r.size[path]
}

// startPayloadActivity 启动一个 Run，调度一个带 input 的 Activity，并用经 HTTP 的 Activity Worker 执行 fn。
func startPayloadActivity(t *testing.T, activityType string, input map[string]any, fn protocol.ActivityFunc) (*engine.Server, *memory.ExecutionManager, *requestRecorder, string) {
	t.Helper()
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	t.Cleanup(server.Stop)
	if err := server.RegisterActivityWorkerVersion(ctx, protocol.ActivityWorkerVersion{
		Identity: "act-worker", BuildID: "b1", TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: []string{activityType},
	}); err != nil {
		t.Fatal(err)
	}
	wf := &program.WorkflowConfig{Type: "payload-" + activityType, Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	run, err := server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	wfCtx := protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "wf-worker", BuildID: "b1", WorkflowCapabilities: program.CurrentWLangCapabilities()})
	lease, err := server.PollWorkflowTask(wfCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(protocol.WithWorkflowTaskToken(ctx, lease.Task.TaskToken), run.ID, lease.Task.ScheduledEventID, []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "step", ActivityType: activityType, Input: input,
		StartToCloseTimeout: time.Hour,
	}}, "wf-worker"); err != nil {
		t.Fatal(err)
	}
	_ = server.AckWorkflowTask(ctx, lease)

	httpServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	t.Cleanup(httpServer.Close)
	recorder := &requestRecorder{next: http.DefaultTransport, hits: map[string]int{}, size: map[string]int64{}}
	client := runtimeclient.NewActivityRuntimeHTTPClient(httpServer.URL, &http.Client{Transport: recorder}, runtimeclient.RuntimeHTTPClientOptions{PollTimeout: time.Second})
	registry := activity.NewRegistry()
	if err := registry.Register(activityType, fn); err != nil {
		t.Fatal(err)
	}
	w := worker.NewActivityWorkerWithQueue(client, client, registry, worker.WorkerOptions{Identity: "act-worker", BuildID: "b1", GracefulStopTimeout: time.Second})
	w.Start(ctx)
	t.Cleanup(w.Stop)
	return server, mgr, recorder, run.ID
}

// waitActivityClosed 等到 Activity 的 History 里出现完成或失败事件。
func waitActivityClosed(t *testing.T, server *engine.Server, runID string) historypkg.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := server.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.ActivityCompleted != nil || event.ActivityFailed != nil {
				return event
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("activity did not close within 10s")
	return historypkg.Event{}
}

func TestActivityRequestsCarryTokenWithoutInput(t *testing.T) {
	input := map[string]any{"blob": strings.Repeat("x", 1200*1024)} // 1.2MB：旧格式下 Begin 请求体超过 4MB
	var executed atomic.Int64
	server, _, recorder, runID := startPayloadActivity(t, "payload.big-input", input, func(context.Context, map[string]any) (map[string]any, error) {
		executed.Add(1)
		return map[string]any{"ok": true}, nil
	})
	if event := waitActivityClosed(t, server, runID); event.ActivityCompleted == nil {
		t.Fatalf("activity with a 1.2MB input closed with %+v, want ActivityCompleted", event)
	}
	if executed.Load() != 1 {
		t.Fatalf("activity executed %d times, want 1", executed.Load())
	}
	for _, path := range []string{runtimeapi.ActivityRuntimeBeginPath, runtimeapi.ActivityRuntimeCompletePath} {
		if _, size := recorder.stats(path); size > 8*1024 {
			t.Fatalf("%s request body = %d bytes, want it independent of the 1.2MB input", path, size)
		}
	}
	if _, nacks := recorder.stats(runtimeapi.ActivityRuntimeNackPath); nacks != 0 {
		t.Fatalf("nack requests = %d, want 0", nacks)
	}
}

func TestActivityResultWithinLimitIsReported(t *testing.T) {
	input := map[string]any{"blob": strings.Repeat("x", 700*1024)}
	output := map[string]any{"blob": strings.Repeat("y", 1600*1024)} // 1.6MB，低于 2MB 缺省上限
	server, _, _, runID := startPayloadActivity(t, "payload.big-output", input, func(context.Context, map[string]any) (map[string]any, error) {
		return output, nil
	})
	if event := waitActivityClosed(t, server, runID); event.ActivityCompleted == nil {
		t.Fatalf("activity with a 1.6MB result closed with %+v, want ActivityCompleted", event)
	}
}

func TestActivityResultOverLimitFailsNonRetryable(t *testing.T) {
	output := map[string]any{"blob": strings.Repeat("y", protocol.DefaultPayloadSizeLimit)}
	var executed atomic.Int64
	server, mgr, recorder, runID := startPayloadActivity(t, "payload.oversized-output", nil, func(context.Context, map[string]any) (map[string]any, error) {
		executed.Add(1)
		return output, nil
	})
	event := waitActivityClosed(t, server, runID)
	assertPayloadLimitFailure(t, event)
	if hits, _ := recorder.stats(runtimeapi.ActivityRuntimeCompletePath); hits != 0 {
		t.Fatalf("complete requests = %d, want 0: the oversized result is not sent", hits)
	}
	time.Sleep(200 * time.Millisecond)
	if executed.Load() != 1 {
		t.Fatalf("activity executed %d times, want 1: the failure is not retryable", executed.Load())
	}
	if states, _ := mgr.ListActivityStates(runID); states[0].Status != "failed" {
		t.Fatalf("activity state = %s, want failed", states[0].Status)
	}
}

func TestActivityHeartbeatOverLimitFailsNonRetryable(t *testing.T) {
	details := map[string]any{"blob": strings.Repeat("h", protocol.DefaultPayloadSizeLimit)}
	var executed atomic.Int64
	server, _, recorder, runID := startPayloadActivity(t, "payload.oversized-heartbeat", nil, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		executed.Add(1)
		activity.RecordHeartbeat(ctx, details)
		<-ctx.Done() // 超限的心跳结束这次 attempt，Activity context 随之取消
		return nil, ctx.Err()
	})
	event := waitActivityClosed(t, server, runID)
	assertPayloadLimitFailure(t, event)
	if hits, _ := recorder.stats(runtimeapi.ActivityRuntimeHeartbeatPath); hits != 0 {
		t.Fatalf("heartbeat requests = %d, want 0: the oversized details are not sent", hits)
	}
	time.Sleep(200 * time.Millisecond)
	if executed.Load() != 1 {
		t.Fatalf("activity executed %d times, want 1", executed.Load())
	}
}

func assertPayloadLimitFailure(t *testing.T, event historypkg.Event) {
	t.Helper()
	if event.ActivityFailed == nil || event.ActivityFailed.Failure == nil {
		t.Fatalf("activity closed with %+v, want ActivityFailed", event)
	}
	failure := event.ActivityFailed.Failure
	if failure.Type != protocol.FailureTypePayloadSizeLimitExceeded || !failure.NonRetryable {
		t.Fatalf("failure = %+v, want a non-retryable %s", failure, protocol.FailureTypePayloadSizeLimitExceeded)
	}
}
