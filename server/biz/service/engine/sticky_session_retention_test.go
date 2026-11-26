package engine_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

type routeCountingQueue struct {
	*memory.TaskQueue
	normal, sticky atomic.Int64
}

func (q *routeCountingQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	q.normal.Add(1)
	return q.TaskQueue.Enqueue(ctx, task)
}

func (q *routeCountingQueue) EnqueueSticky(ctx context.Context, identity string, task protocol.Task) error {
	q.sticky.Add(1)
	return q.TaskQueue.EnqueueSticky(ctx, identity, task)
}

// sticky 绑定不随时间失效（Temporal 保留到 WFT 超时或失败）：Activity 运行远长于 sticky schedule-to-start 之后，
// 下一个 WFT 仍投给持有缓存 session 的 Worker。
func TestStickySessionOutlivesLongActivity(t *testing.T) {
	clk := clocktest.NewFake(time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC))
	mgr := memory.NewExecutionManager().WithClock(clk)
	workflowTasks := &routeCountingQueue{TaskQueue: memory.NewTaskQueue(16)}
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}, Clock: clk})
	defer server.Stop()
	registerE2EActivities(t, server)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "sticky-long-activity"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Hour`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Hour`},
	)); err != nil {
		t.Fatal(err)
	}
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	w := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "sticky-worker-1", BuildID: "build-1", ExecutionCache: cache})
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	defer func() { cancel(); w.Stop() }()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "sticky-long-activity", "sticky-long-activity", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueActivityTask(t, activityTasks)
	waitForStickyOwner(t, mgr, runID, "sticky-worker-1")
	stickyBefore := workflowTasks.sticky.Load()

	// Activity 运行 1 分钟（sticky schedule-to-start 的 12 倍）后才完成。
	clk.Advance(time.Minute)
	registerE2EActivities(t, server) // Activity Worker 在这 1 分钟里保持登记
	if err := server.FallbackExpiredStickyWorkflowTasks(ctx); err != nil {
		t.Fatal(err)
	}
	completeActivityTask(t, server, first, map[string]any{"first": "done"})
	_ = dequeueActivityTask(t, activityTasks)

	if got := workflowTasks.sticky.Load() - stickyBefore; got != 1 {
		t.Fatalf("next workflow task enqueued sticky %d times after a one-minute activity (normal enqueues %d), want 1", got, workflowTasks.normal.Load())
	}
	if snapshot := cache.Snapshot(); snapshot.Hits < 1 || snapshot.Admissions != 1 {
		t.Fatalf("cache snapshot = %+v, want the second activation served from the cached session", snapshot)
	}
}
