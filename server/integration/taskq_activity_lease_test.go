package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
)

// 这组场景固定 Activity 在持久队列上的投递语义：attempt 开始后 Worker 立即确认租约，attempt 的存活
// 由 StartToClose 与 heartbeat 超时负责；Server 重启时持久队列里的任务不再入队第二份。
// TaskQ 只有 GORM 存储，这组场景只在 TENUTO_TEST_MYSQL=1 时运行。

func TestTaskQRunningActivityIsNotRedeliveredAfterVisibilityTimeout(t *testing.T) {
	testRunningActivityIsNotRedelivered(t)
}

func TestTaskQRestartDoesNotDuplicateQueuedTasks(t *testing.T) {
	testRestartDoesNotDuplicateQueuedTasks(t)
}

func TestTaskQStartedAttemptOfVanishedWorkerRetriesAfterTimeout(t *testing.T) {
	testVanishedWorkerAttemptRetries(t)
}

func newLeaseTestServer(t *testing.T, h *taskQHarness, store *persistence.Store) *engine.Server {
	t.Helper()
	return assembly.NewServer(assembly.Options{
		Stores:         assembly.Persistent(store),
		WorkflowQueue:  taskq.NewQueue(h.frontend(t, "workflow-tasks")),
		ActivityRouter: h.activityRouter(t, nil),
	})
}

// testRunningActivityIsNotRedelivered：Activity 的运行时间是可见性超时的数倍，期间没有任何故障。
// 函数体只进入一次，History 只有一个 ActivityStarted。
func testRunningActivityIsNotRedelivered(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarnessWithOptions(t, map[string]int{"workflow-tasks": 1}, taskQHarnessOptions{VisTimeout: 400 * time.Millisecond})
	defer h.close()
	server := newLeaseTestServer(t, h, newMySQLWorkflowStore(t))
	defer server.Stop()

	var entered atomic.Int32
	registry := wfsdk.NewRegistry()
	registry.Register("slow_charge", func(ctx context.Context, input m) (m, error) {
		entered.Add(1)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return m{"charged": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 2})
	worker.Start(ctx)
	defer worker.Stop()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-slow-activity"},
		sdkgentest.Step{Type: "slow_charge", Options: `StartToCloseTimeout: 30 * time.Second`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 15*time.Second)

	if got := entered.Load(); got != 1 {
		t.Fatalf("activity body entered %d times, want 1", got)
	}
	events, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(events, historypkg.EventActivityStarted) != 1 || countEvent(events, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("unexpected activity history: %+v", events)
	}
}

// testRestartDoesNotDuplicateQueuedTasks：Server 在 Workflow Task 与 Activity 任务各自排队时重启，
// 启动恢复之后队列里每个任务仍只有一份，Activity 只执行一次。
func testRestartDoesNotDuplicateQueuedTasks(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarnessWithOptions(t, map[string]int{"workflow-tasks": 1}, taskQHarnessOptions{})
	defer h.close()
	store := newMySQLWorkflowStore(t)

	first := newLeaseTestServer(t, h, store)
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-restart-no-duplicate"},
		sdkgentest.Step{Type: "capture", Options: `StartToCloseTimeout: 30 * time.Second`},
	)
	if _, err := first.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(first.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForPersistedTaskCount(t, h, "workflow-tasks", 1, 5*time.Second)
	first.Stop()

	// 重启时 Workflow Task 仍在队列里。
	second := newLeaseTestServer(t, h, store)
	registerActivityRoutes(t, second, "capture")
	if err := second.RecoverRunningWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	if rows := persistedTaskQRows(t, h, "workflow-tasks"); len(rows) != 1 {
		t.Fatalf("workflow task rows after restart = %+v, want 1", rows)
	}

	// 完成 Workflow Task 调度 Activity，再在 Activity 任务排队时重启。
	workflowQueue := taskq.NewQueue(h.frontend(t, "workflow-tasks"))
	lease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, lease); err != nil {
		t.Fatal(err)
	}
	wftID := openWorkflowTaskID(t, second, runID)
	if err := second.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, second, runID, wftID), runID, wftID, []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "#a1", ActivityType: "capture", StartToCloseTimeout: 30 * time.Second,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	waitForActivityRowCount(t, h, 1, 5*time.Second)
	second.Stop()

	third := newLeaseTestServer(t, h, store)
	defer third.Stop()
	if err := third.RecoverRunningWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	if count := activityRowCount(t, h); count != 1 {
		t.Fatalf("activity task rows after restart = %d, want 1", count)
	}

	var callsMu sync.Mutex
	calls := 0
	registry := wfsdk.NewRegistry()
	registry.Register("capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(third, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 2})
	worker.Start(ctx)
	defer worker.Stop()
	waitForServerRunStatus(t, third, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	defer callsMu.Unlock()
	if calls != 1 {
		t.Fatalf("activity executed %d times, want 1", calls)
	}
}

// testVanishedWorkerAttemptRetries：Worker 记录 attempt started 后消失，连租约都没确认。重投的副本
// 被丢弃，StartToClose 超时后第二个 attempt 由另一个 Worker 执行完成。
func testVanishedWorkerAttemptRetries(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarnessWithOptions(t, map[string]int{"workflow-tasks": 1}, taskQHarnessOptions{VisTimeout: 300 * time.Millisecond})
	defer h.close()
	store := newMySQLWorkflowStore(t)
	activityRouter := h.activityRouter(t, nil)
	server := assembly.NewServer(assembly.Options{
		Stores:         assembly.Persistent(store),
		WorkflowQueue:  taskq.NewQueue(h.frontend(t, "workflow-tasks")),
		ActivityRouter: activityRouter,
	})
	defer server.Stop()
	registerActivityRoutes(t, server, "charge")

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-vanished-worker"},
		sdkgentest.Step{Type: "charge", Options: `StartToCloseTimeout: time.Second, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1}`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	workflowQueue := taskq.NewQueue(h.frontend(t, "workflow-tasks"))
	wftLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, wftLease); err != nil {
		t.Fatal(err)
	}
	wftID := openWorkflowTaskID(t, server, runID)
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, runID, wftID), runID, wftID, []historypkg.Command{{
		Type: historypkg.CmdScheduleActivity, ActivityID: "#a1", ActivityType: "charge",
		StartToCloseTimeout: time.Second,
		RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 2, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	// 消失的 Worker：领取并记录 started，之后既不确认租约也不上报结果。
	vanished := dequeueDefaultActivity(t, activityRouter)
	begin, err := engine.NewActivityRuntimeService(server).BeginActivityTask(ctx, vanished.Task)
	if err != nil || !begin.Execute {
		t.Fatalf("begin first attempt: %+v err=%v", begin, err)
	}

	var attemptsMu sync.Mutex
	var attempts []int
	registry := wfsdk.NewRegistry()
	registry.Register("charge", func(ctx context.Context, input m) (m, error) {
		info := wfsdk.GetInfo(ctx)
		attemptsMu.Lock()
		attempts = append(attempts, info.Attempt)
		attemptsMu.Unlock()
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 2})
	worker.Start(ctx)
	defer worker.Stop()
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 15*time.Second)

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	if len(attempts) != 1 || attempts[0] != 2 {
		t.Fatalf("executed attempts = %v, want only attempt 2", attempts)
	}
	events, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(events, historypkg.EventActivityTimedOut) != 1 || countEvent(events, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("unexpected activity history: %+v", events)
	}
}

// dequeueDefaultActivity 从默认 task queue 领取一个 Activity 任务；空轮询时继续，5 秒内领不到时失败。
func dequeueDefaultActivity(t *testing.T, router *taskq.ActivityQueueRouter) protocol.TaskLease {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lease, err := router.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
		cancel()
		if err == nil {
			return lease
		}
	}
	t.Fatal("no activity task within 5s")
	return protocol.TaskLease{}
}

// activityRowCount 统计全部 Activity 物理队列（名字带 activity: 前缀）里的任务行。
func activityRowCount(t *testing.T, h *taskQHarness) int64 {
	t.Helper()
	var count int64
	if err := h.store.DB().Table("taskq_tasks").Where("queue_name LIKE ?", "activity:%").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func waitForActivityRowCount(t *testing.T, h *taskQHarness, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if activityRowCount(t, h) == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("activity task rows = %d, want %d", activityRowCount(t, h), want)
}
