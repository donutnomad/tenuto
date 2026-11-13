package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Activity 任务不带 build：同一队列上两个 build 的 Worker 都能领取，Run 照常完成。
func TestRealTaskQActivityWorkersOfAnyBuildServeTheQueue(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks":    2,
		"activity-payments": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, map[string]string{"payments": "activity-payments"})})
	defer server.Stop()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{ID: "taskq-build-routing-workers"},
		sdkgentest.Step{Type: "payments.Targeted", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "payments"`},
		sdkgentest.Step{Type: "payments.Untargeted", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "payments"`},
	)); err != nil {
		t.Fatal(err)
	}

	var callsMu sync.Mutex
	var calls []string
	newRegistry := func(worker string) *wfsdk.Registry {
		registry := wfsdk.NewRegistry()
		registry.Register("payments.Targeted", func(context.Context, m) (m, error) {
			callsMu.Lock()
			calls = append(calls, worker+":targeted")
			callsMu.Unlock()
			return m{"worker": worker}, nil
		})
		registry.Register("payments.Untargeted", func(context.Context, m) (m, error) {
			callsMu.Lock()
			calls = append(calls, worker+":untargeted")
			callsMu.Unlock()
			return m{"worker": worker}, nil
		})
		return registry
	}

	workflowWorker := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role:                worker.WorkerRoleWorkflow,
		WorkflowConcurrency: 1,
		Identity:            "workflow-worker",
	})
	v1Worker := assembly.NewWorker(server, newRegistry("v1"), worker.WorkerOptions{
		Role:                worker.WorkerRoleActivity,
		ActivityConcurrency: 1,
		ActivityTaskQueues:  []string{"payments"},
		Identity:            "payments-worker-v1",
		BuildID:             "payments-v1",
	})
	v2Worker := assembly.NewWorker(server, newRegistry("v2"), worker.WorkerOptions{
		Role:                worker.WorkerRoleActivity,
		ActivityConcurrency: 1,
		ActivityTaskQueues:  []string{"payments"},
		Identity:            "payments-worker-v2",
		BuildID:             "payments-v2",
	})
	workflowWorker.Start(ctx)
	v1Worker.Start(ctx)
	v2Worker.Start(ctx)
	defer workflowWorker.Stop()
	defer v1Worker.Stop()
	defer v2Worker.Stop()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "taskq-build-routing-workers", "taskq-build-routing-workers", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, history.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	got := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(got) != 2 {
		t.Fatalf("activity calls = %v", got)
	}
	if got[0] != "v1:targeted" && got[0] != "v2:targeted" {
		t.Fatalf("first activity call = %q, all calls = %v", got[0], got)
	}
	if got[1] != "v1:untargeted" && got[1] != "v2:untargeted" {
		t.Fatalf("untargeted activity call = %q", got[1])
	}
}

func TestRealTaskQStickyWorkflowTasksStayWithOwner(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{"workflow-tasks": 2, "activity-tasks": 2})
	defer h.close()
	workflowStore := newMySQLWorkflowStore(t)
	workflowFrontend := h.frontend(t, "workflow-tasks")
	activityQueue := h.activityRouter(t, nil)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFrontend), ActivityRouter: activityQueue})
	defer server.Stop()
	registerStickyWorkflow(t, server, "taskq-sticky-owner")

	ownerCache := newRoutingExecutionCache(t)
	otherCache := newRoutingExecutionCache(t)
	owner := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role: worker.WorkerRoleWorkflow, WorkflowConcurrency: 1,
		Identity: "owner-worker", BuildID: "workflow-v1", ExecutionCache: ownerCache,
	})
	owner.Start(ctx)
	waitForWorkerReady(t, owner)
	defer owner.Stop()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "taskq-sticky-owner", "taskq-sticky-owner", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueRealActivity(t, activityQueue)
	waitForRealStickyOwner(t, workflowStore, runID, "owner-worker")

	other := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role: worker.WorkerRoleWorkflow, WorkflowConcurrency: 1,
		Identity: "other-worker", BuildID: "workflow-v1", ExecutionCache: otherCache,
	})
	other.Start(ctx)
	waitForWorkerReady(t, other)
	defer other.Stop()

	completeRealActivity(t, server, activityQueue, first)
	second := dequeueRealActivity(t, activityQueue)
	waitForCacheHits(t, ownerCache, 1)
	if snapshot := otherCache.Snapshot(); snapshot.Admissions != 0 || snapshot.Hits != 0 {
		t.Fatalf("other worker cache = %+v", snapshot)
	}
	completeRealActivity(t, server, activityQueue, second)
	waitForServerRunStatus(t, server, runID, history.StatusCompleted, 5*time.Second)
}

func TestRealTaskQStickyWorkflowFallsBackAfterOwnerStops(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{"workflow-tasks": 1, "activity-tasks": 1})
	defer h.close()
	workflowStore := newMySQLWorkflowStore(t)
	activityQueue := h.activityRouter(t, nil)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: activityQueue})
	defer server.Stop()
	registerStickyWorkflow(t, server, "taskq-sticky-fallback")

	ownerCache := newRoutingExecutionCache(t)
	owner := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role: worker.WorkerRoleWorkflow, WorkflowConcurrency: 1,
		Identity: "owner-worker", BuildID: "workflow-v1", ExecutionCache: ownerCache,
	})
	owner.Start(ctx)
	waitForWorkerReady(t, owner)
	runID, err := startedRunID(server.StartWorkflow(ctx, "", "taskq-sticky-fallback", "taskq-sticky-fallback", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueRealActivity(t, activityQueue)
	waitForRealStickyOwner(t, workflowStore, runID, "owner-worker")
	state, err := workflowStore.Executions.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	routingDeadline := time.Now().Add(300 * time.Millisecond)
	if _, assigned, err := workflowStore.Executions.AssignStickyExecution(
		runID,
		state.ExecutionGeneration,
		state.StickyGeneration,
		"owner-worker",
		"workflow-v1",
		routingDeadline,
	); err != nil || !assigned {
		t.Fatalf("shorten sticky assignment: assigned=%v err=%v", assigned, err)
	}
	owner.Stop()

	completeRealActivity(t, server, activityQueue, first)
	keyedTask := waitForPersistedKeyedTask(t, h, "workflow-tasks", 3*time.Second)
	rowsBeforeFallback := persistedTaskQRows(t, h, "workflow-tasks")
	waitUntil(t, routingDeadline.Add(50*time.Millisecond))
	if err := server.FallbackExpiredStickyWorkflowTasks(ctx); err != nil {
		t.Fatal(err)
	}
	rowsAfterFallback := persistedTaskQRows(t, h, "workflow-tasks")
	if len(rowsAfterFallback) != len(rowsBeforeFallback) || !containsPersistedTask(rowsAfterFallback, keyedTask.TaskID) {
		t.Fatalf("fallback changed TaskQ rows: before=%+v after=%+v", rowsBeforeFallback, rowsAfterFallback)
	}

	takeoverCache := newRoutingExecutionCache(t)
	takeover := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role: worker.WorkerRoleWorkflow, WorkflowConcurrency: 1,
		Identity: "takeover-worker", BuildID: "workflow-v1", ExecutionCache: takeoverCache,
	})
	takeover.Start(ctx)
	waitForWorkerReady(t, takeover)
	defer takeover.Stop()

	second := dequeueRealActivity(t, activityQueue)
	waitForCacheAdmissions(t, takeoverCache, 1)
	if snapshot := takeoverCache.Snapshot(); snapshot.Hits != 0 {
		t.Fatalf("takeover cache = %+v", snapshot)
	}
	waitForRealStickyOwner(t, workflowStore, runID, "takeover-worker")
	completeRealActivity(t, server, activityQueue, second)
	waitForServerRunStatus(t, server, runID, history.StatusCompleted, 5*time.Second)
	waitForPersistedTaskCount(t, h, "workflow-tasks", 0, 10*time.Second)
	queueInfo, err := h.store.GetQueue(context.Background(), "workflow-tasks", keyedTask.PartitionID)
	if err != nil {
		t.Fatal(err)
	}
	if queueInfo.AckLevel < keyedTask.TaskID {
		t.Fatalf("workflow queue ack level = %d, want at least %d", queueInfo.AckLevel, keyedTask.TaskID)
	}
}

type persistedTaskQTask struct {
	TaskID          int64
	PartitionID     int
	RoutingKey      string
	RoutingDeadline *time.Time
}

func persistedTaskQRows(t *testing.T, h *taskQHarness, queueName string) []persistedTaskQTask {
	t.Helper()
	var rows []persistedTaskQTask
	if err := h.store.DB().Table("taskq_tasks").
		Select("task_id, partition_id, routing_key, routing_deadline").
		Where("queue_name = ?", queueName).
		Order("partition_id ASC, task_id ASC").
		Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func waitForPersistedKeyedTask(t *testing.T, h *taskQHarness, queueName string, timeout time.Duration) persistedTaskQTask {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, row := range persistedTaskQRows(t, h, queueName) {
			if row.RoutingKey != "" && row.RoutingDeadline != nil {
				return row
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("keyed TaskQ task did not persist: %+v", persistedTaskQRows(t, h, queueName))
	return persistedTaskQTask{}
}

func waitForPersistedTaskCount(t *testing.T, h *taskQHarness, queueName string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rows := persistedTaskQRows(t, h, queueName); len(rows) == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("TaskQ rows = %+v, want count %d", persistedTaskQRows(t, h, queueName), want)
}

func containsPersistedTask(rows []persistedTaskQTask, taskID int64) bool {
	for _, row := range rows {
		if row.TaskID == taskID {
			return true
		}
	}
	return false
}

func waitUntil(t *testing.T, deadline time.Time) {
	t.Helper()
	if delay := time.Until(deadline); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C
	}
}

func registerStickyWorkflow(t *testing.T, server *engine.Server, workflowID string) {
	t.Helper()
	registerActivityRoutes(t, server, "activity.first", "activity.second")
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{ID: workflowID},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Minute`},
	)); err != nil {
		t.Fatal(err)
	}
}

func newRoutingExecutionCache(t *testing.T) *executor.ExecutionCache {
	t.Helper()
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{
		MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func waitForWorkerReady(t *testing.T, worker *worker.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

func dequeueRealActivity(t *testing.T, queue *taskq.ActivityQueueRouter) protocol.TaskLease {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lease, err := queue.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
		cancel()
		if err == nil {
			return lease
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("dequeue real activity: %v", lastErr)
	return protocol.TaskLease{}
}

func completeRealActivity(t *testing.T, server *engine.Server, queue *taskq.ActivityQueueRouter, lease protocol.TaskLease) {
	t.Helper()
	if _, err := server.HandleActivityStartedWithMetadata(lease.Task, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(
		context.Background(), lease.Task.RunID, lease.Task.ScheduledEventID, lease.Task.Attempt, lease.Task.Stamp, m{"ok": true},
	); err != nil {
		t.Fatal(err)
	}
	if err := queue.Ack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
}

func waitForRealStickyOwner(t *testing.T, store *persistence.Store, runID, owner string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := store.Executions.GetWorkflowExecutionState(runID)
		if err == nil && state.StickyWorkerIdentity == owner {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := store.Executions.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatalf("get sticky owner: %v", err)
	}
	t.Fatalf("sticky owner = %q, want %q", state.StickyWorkerIdentity, owner)
}

func waitForCacheHits(t *testing.T, cache *executor.ExecutionCache, hits uint64) {
	t.Helper()
	waitForCacheSnapshot(t, cache, func(snapshot protocol.ExecutionCacheSnapshot) bool { return snapshot.Hits >= hits })
}

func waitForCacheAdmissions(t *testing.T, cache *executor.ExecutionCache, admissions uint64) {
	t.Helper()
	waitForCacheSnapshot(t, cache, func(snapshot protocol.ExecutionCacheSnapshot) bool { return snapshot.Admissions >= admissions })
}

func waitForCacheSnapshot(t *testing.T, cache *executor.ExecutionCache, ready func(protocol.ExecutionCacheSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready(cache.Snapshot()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cache snapshot = %+v", cache.Snapshot())
}

// 发给 Run 目标 Worker 的任务在 Nack 与 frontend 重启之后仍只交给这个 Worker。
func TestRealTaskQActivityTargetRoutingSurvivesNackAndFrontendRestart(t *testing.T) {
	h := newTaskQHarness(t, map[string]int{"activity-payments": 2})
	defer h.close()

	router := h.activityRouter(t, map[string]string{
		"payments": "activity-payments",
	})
	task := protocol.Task{
		Type:                 protocol.TaskTypeActivity,
		Namespace:            "tenant-a",
		RunID:                "run-v2",
		ScheduledEventID:     1,
		TaskQueue:            "payments",
		TargetWorkerIdentity: "worker-payments-v2",
	}
	if err := router.Enqueue(context.Background(), "payments", task); err != nil {
		t.Fatal(err)
	}
	assertNoActivityForBuild(t, router, "payments-v1")

	v2Context := activityRoutingContext("payments-v2")
	lease, err := dequeueActivityWithTimeout(router, v2Context, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != task.RunID || lease.Task.TargetWorkerIdentity != task.TargetWorkerIdentity {
		t.Fatalf("v2 lease = %+v", lease)
	}
	if err := router.Nack(context.Background(), lease); err != nil {
		t.Fatal(err)
	}

	restartedRouter := h.activityRouter(t, map[string]string{
		"payments": "activity-payments",
	})
	assertNoActivityForBuild(t, restartedRouter, "payments-v1")
	retried, err := dequeueActivityWithTimeout(restartedRouter, v2Context, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Task.RunID != task.RunID || retried.Task.TargetWorkerIdentity != task.TargetWorkerIdentity {
		t.Fatalf("retried lease = %+v, first = %+v", retried, lease)
	}
	if retried.Token == lease.Token {
		t.Fatalf("redelivery reused token %q", retried.Token)
	}
	// 旧 lease 的 token 在 nack 重投后已失效。taskq 动态分区化后，matching handler 对
	// 「过期 token 的重复 ack」不再回 InvalidArgument，而是幂等静默成功（server.go:AckTask
	// 把 ErrTaskNotOutstanding 与退役分区的迟到 ack 都吞成 nil，避免消费者收到不可重试的硬错）。
	// 不变式不变——旧 token 不再 outstanding，ack 它无副作用；两种失效表现都接受。
	if err := restartedRouter.Ack(context.Background(), lease); err != nil && status.Code(err) != codes.InvalidArgument {
		t.Fatalf("previous delivery ack error = %v, want nil (idempotent) or InvalidArgument", err)
	}
	// 旧 token 的幂等 ack 不得误伤新交付：新 token 仍必须能成功 ack。
	if err := restartedRouter.Ack(context.Background(), retried); err != nil {
		t.Fatal(err)
	}

	untargeted := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		Namespace:        "tenant-a",
		RunID:            "run-unversioned",
		ScheduledEventID: 2,
		TaskQueue:        "payments",
	}
	if err := restartedRouter.Enqueue(context.Background(), "payments", untargeted); err != nil {
		t.Fatal(err)
	}
	competed, err := dequeueActivityWithTimeout(restartedRouter, activityRoutingContext("payments-v1"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if competed.Task.RunID != untargeted.RunID {
		t.Fatalf("untargeted lease = %+v", competed)
	}
}

func TestRealTaskQActivitySharedPhysicalQueueIsolatesNamespaces(t *testing.T) {
	h := newTaskQHarness(t, map[string]int{"activity-payments": 2})
	defer h.close()

	router := h.activityRouter(t, map[string]string{
		"payments": "activity-payments",
	})
	for _, task := range []protocol.Task{
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-a-default", ScheduledEventID: 1, TaskQueue: "payments"},
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-a-v2", ScheduledEventID: 2, TaskQueue: "payments", TargetWorkerIdentity: "worker-payments-v2"},
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-b", RunID: "run-b-default", ScheduledEventID: 1, TaskQueue: "payments"},
		{Type: protocol.TaskTypeActivity, Namespace: "tenant-b", RunID: "run-b-v2", ScheduledEventID: 2, TaskQueue: "payments", TargetWorkerIdentity: "worker-payments-v2"},
	} {
		if err := router.Enqueue(context.Background(), "payments", task); err != nil {
			t.Fatal(err)
		}
	}

	assertNamespaceActivityLeases(t, router, "tenant-b", "payments-v2", map[string]bool{
		"run-b-default": true,
		"run-b-v2":      true,
	})
	assertNamespaceActivityLeases(t, router, "tenant-a", "payments-v2", map[string]bool{
		"run-a-default": true,
		"run-a-v2":      true,
	})
}

func assertNamespaceActivityLeases(t *testing.T, router *taskq.ActivityQueueRouter, namespace, buildID string, want map[string]bool) {
	t.Helper()
	ctx := activityRoutingContextForNamespace(namespace, buildID)
	for range want {
		lease, err := dequeueActivityWithTimeout(router, ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Task.Namespace != namespace || !want[lease.Task.RunID] {
			t.Fatalf("%s worker received lease %+v", namespace, lease)
		}
		delete(want, lease.Task.RunID)
		if err := router.Ack(context.Background(), lease); err != nil {
			t.Fatal(err)
		}
	}
}

func activityRoutingContext(buildID string) context.Context {
	return activityRoutingContextForNamespace("tenant-a", buildID)
}

func activityRoutingContextForNamespace(namespace, buildID string) context.Context {
	ctx := protocol.WithNamespace(context.Background(), namespace)
	return protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{
		Namespace: namespace,
		Identity:  "worker-" + buildID,
		BuildID:   buildID,
	})
}

func assertNoActivityForBuild(t *testing.T, router *taskq.ActivityQueueRouter, buildID string) {
	t.Helper()
	if lease, err := dequeueActivityWithTimeout(router, activityRoutingContext(buildID), 100*time.Millisecond); err == nil {
		t.Fatalf("build %q received lease %+v", buildID, lease)
	}
}

func dequeueActivityWithTimeout(router *taskq.ActivityQueueRouter, ctx context.Context, timeout time.Duration) (protocol.TaskLease, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return router.Dequeue(pollCtx, "payments")
}
