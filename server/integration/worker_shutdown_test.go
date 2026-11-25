package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Worker 优雅停止与 Activity 取消（内存后端端到端）
// ============================================
// 测试策略:
// - Happy Path: 停止中的 Worker 交还 attempt，另一个 Worker 以同一个 attempt 完成，History 没有失败或超时
// - Edge Case: 宽限期到期取消 Activity 的 context，同样交还；有重试策略与没有重试策略两种 attempt
// - Error Case: 取消请求经心跳送达，Activity 结束但不上报失败；不心跳的 Activity 收不到取消，晚到的结果被忽略
// ============================================

// countingActivityRuntime 记录 Worker 的失败、完成与交还上报，再交给 Server。
type countingActivityRuntime struct {
	activitytasks.API
	mu        sync.Mutex
	failed    int
	completed int
	released  int
}

func (r *countingActivityRuntime) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	r.mu.Lock()
	r.failed++
	r.mu.Unlock()
	return r.API.FailActivityTask(ctx, task, failure)
}

func (r *countingActivityRuntime) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	r.mu.Lock()
	r.completed++
	r.mu.Unlock()
	return r.API.CompleteActivityTask(ctx, task, output, failure)
}

func (r *countingActivityRuntime) ReleaseActivityTask(ctx context.Context, task protocol.Task) error {
	r.mu.Lock()
	r.released++
	r.mu.Unlock()
	return r.API.ReleaseActivityTask(ctx, task)
}

func (r *countingActivityRuntime) counts() (failed, completed, released int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed, r.completed, r.released
}

type shutdownHarness struct {
	server   *engine.Server
	queues   *memory.ActivityQueueRouter
	runtime  *countingActivityRuntime
	workflow *worker.Worker
}

const shutdownActivityType = "shutdown.Long"

// newShutdownHarness 登记只调用一次 shutdownActivityType 的 Workflow 并启动 Run；options 是该调用的
// workflow.ActivityOptions 字段。
func newShutdownHarness(t *testing.T, options string) (*shutdownHarness, string) {
	t.Helper()
	queues := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: queues})
	t.Cleanup(server.Stop)
	registerActivityRoutes(t, server, shutdownActivityType)

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "worker-shutdown-" + t.Name()},
		sdkgentest.Step{Type: shutdownActivityType, Options: options})
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	workflowWorker := assembly.NewWorker(server, activity.NewRegistry(), worker.WorkerOptions{Role: worker.WorkerRoleWorkflow})
	workflowWorker.Start(context.Background())
	t.Cleanup(workflowWorker.Stop)

	h := &shutdownHarness{server: server, queues: queues, runtime: &countingActivityRuntime{API: engine.NewActivityRuntimeService(server)}, workflow: workflowWorker}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	return h, runID
}

func (h *shutdownHarness) startActivityWorker(t *testing.T, identity string, graceful time.Duration, fn protocol.ActivityFunc) *worker.Worker {
	t.Helper()
	registry := activity.NewRegistry()
	if err := registry.Register(shutdownActivityType, fn); err != nil {
		t.Fatal(err)
	}
	w := assembly.NewActivityWorker(h.queues, h.runtime, registry, worker.WorkerOptions{Identity: identity, GracefulStopTimeout: graceful})
	w.Start(context.Background())
	t.Cleanup(w.Stop)
	return w
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertNoActivityFailureEvents(t *testing.T, server *engine.Server, runID string) []historypkg.Event {
	t.Helper()
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []historypkg.EventType{historypkg.EventActivityAttemptFailed, historypkg.EventActivityTimedOut, historypkg.EventActivityFailed} {
		if n := countEvent(history, typ); n != 0 {
			t.Fatalf("history has %d %s events, want none: %+v", n, typ, history)
		}
	}
	return history
}

func TestStoppedWorkerReleasesActivityToAnotherWorker(t *testing.T) {
	cases := []struct {
		name     string
		retry    string
		graceful time.Duration
		first    func(ctx context.Context) (map[string]any, error)
	}{
		{
			// 没有重试策略：attempt 若被消耗，Activity 与 Workflow 会直接失败。
			name:     "activity honours the stop channel",
			graceful: 5 * time.Second,
			first: func(ctx context.Context) (map[string]any, error) {
				<-activity.GetWorkerStopChannel(ctx)
				if ctx.Err() != nil {
					return nil, fmt.Errorf("context cancelled at the start of the drain: %w", ctx.Err())
				}
				activity.RecordHeartbeat(ctx, map[string]any{"progress": "half"})
				return nil, activity.ErrWorkerStopping
			},
		},
		{
			name:     "graceful stop timeout cancels the activity",
			retry:    "RetryPolicy: workflow.RetryPolicy{MaxAttempts: 3}",
			graceful: 20 * time.Millisecond,
			first: func(ctx context.Context) (map[string]any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, runID := newShutdownHarness(t, "StartToCloseTimeout: time.Minute, "+tc.retry)

			started := make(chan struct{})
			first := h.startActivityWorker(t, "shutdown-worker-1", tc.graceful, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				close(started)
				return tc.first(ctx)
			})
			waitSignal(t, started, "the first execution")
			first.Stop()
			if failed, completed, released := h.runtime.counts(); failed != 0 || completed != 0 || released != 1 {
				t.Fatalf("first worker reported failed=%d completed=%d released=%d, want one release", failed, completed, released)
			}

			type execution struct {
				info      activity.Info
				heartbeat map[string]any
			}
			executions := make(chan execution, 1)
			h.startActivityWorker(t, "shutdown-worker-2", 0, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				info := activity.GetInfo(ctx)
				executions <- execution{info: info, heartbeat: activity.GetLastHeartbeatDetails(ctx)}
				return map[string]any{"done": true}, nil
			})
			waitForServerRunStatus(t, h.server, runID, historypkg.StatusCompleted, 5*time.Second)
			got := <-executions
			second, heartbeat := got.info, got.heartbeat

			if second.Attempt != 1 {
				t.Fatalf("redelivered attempt = %d, want 1: a released attempt is not consumed", second.Attempt)
			}
			if tc.retry == "" && fmt.Sprint(heartbeat["progress"]) != "half" {
				t.Fatalf("redelivered heartbeat details = %v, want the released attempt's progress", heartbeat)
			}
			history := assertNoActivityFailureEvents(t, h.server, runID)
			if n := countEvent(history, historypkg.EventActivityCompleted); n != 1 {
				t.Fatalf("ActivityCompleted events = %d, want 1", n)
			}
		})
	}
}

func TestHeartbeatingActivityEndsAsCancelledWithoutFailure(t *testing.T) {
	h, runID := newShutdownHarness(t, "StartToCloseTimeout: time.Minute, HeartbeatTimeout: 10 * time.Second")
	heartbeating := make(chan struct{})
	returned := make(chan struct{})
	var once sync.Once
	h.startActivityWorker(t, "cancel-worker", 0, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		defer close(returned)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			activity.RecordHeartbeat(ctx, nil)
			once.Do(func() { close(heartbeating) })
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
			}
		}
	})
	waitSignal(t, heartbeating, "the first heartbeat")
	if err := h.server.CancelWorkflow(context.Background(), "", "", runID, "test"); err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, h.server, runID, historypkg.StatusCancelled, 5*time.Second)
	waitSignal(t, returned, "the cancelled activity to return")
	h.workflow.Stop()

	if failed, completed, released := h.runtime.counts(); failed != 0 || completed != 0 || released != 0 {
		t.Fatalf("cancelled activity reported failed=%d completed=%d released=%d, want nothing", failed, completed, released)
	}
	assertNoActivityFailureEvents(t, h.server, runID)
}

func TestNonHeartbeatingActivityIsNotCancelledAndItsLateResultIsIgnored(t *testing.T) {
	h, runID := newShutdownHarness(t, "StartToCloseTimeout: time.Minute")
	started := make(chan struct{})
	unblock := make(chan struct{})
	ctxErrs := make(chan error, 1)
	h.startActivityWorker(t, "no-heartbeat-worker", 0, func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		close(started)
		<-unblock
		ctxErrs <- ctx.Err()
		return map[string]any{"late": true}, nil
	})
	waitSignal(t, started, "the activity")
	if err := h.server.CancelWorkflow(context.Background(), "", "", runID, "test"); err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, h.server, runID, historypkg.StatusCancelled, 5*time.Second)
	close(unblock)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, completed, _ := h.runtime.counts(); completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late completion was not reported")
		}
		time.Sleep(time.Millisecond)
	}
	if ctxErr := <-ctxErrs; ctxErr != nil {
		t.Fatalf("activity context = %v; cancellation reaches an activity only through heartbeats", ctxErr)
	}
	history := assertNoActivityFailureEvents(t, h.server, runID)
	if n := countEvent(history, historypkg.EventActivityCompleted); n != 0 {
		t.Fatalf("late completion after the run was cancelled was recorded: %+v", history)
	}
	if status, err := h.server.GetRunStatus(runID); err != nil || status != historypkg.StatusCancelled {
		t.Fatalf("run status = %s, %v; want Cancelled", status, err)
	}
}
