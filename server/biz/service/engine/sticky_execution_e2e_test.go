package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkerStickyExecutionRetainsSessionAndConsumesDelta(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
	registerE2EActivities(t, server)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "sticky-two-activities"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Minute`},
	)); err != nil {
		t.Fatal(err)
	}
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{
		MaxEntries:        10,
		MaxEstimatedBytes: 16 << 20,
		IdleTTL:           time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := worker.NewWorkflowWorker(server, worker.WorkerOptions{
		Identity:       "sticky-worker-1",
		BuildID:        "build-1",
		ExecutionCache: cache,
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)
	t.Cleanup(func() {
		cancel()
		worker.Stop()
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "sticky-two-activities", "sticky-two-activities", "", map[string]any{"request": "one"}))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueActivityTask(t, activityTasks)
	if first.RunID != runID || first.ActivityID != "#a1" {
		t.Fatalf("first activity task = %+v", first)
	}
	completeActivityTask(t, server, first, map[string]any{"first": "done"})

	second := dequeueActivityTask(t, activityTasks)
	if second.RunID != runID || second.ActivityID != "#a2" {
		t.Fatalf("second activity task = %+v", second)
	}
	snapshot := cache.Snapshot()
	if snapshot.Entries != 1 || snapshot.Hits < 1 || snapshot.Admissions != 1 {
		t.Fatalf("sticky cache snapshot after second activation = %+v", snapshot)
	}
	completeActivityTask(t, server, second, map[string]any{"second": "done"})
	waitForRunStatus(t, server, runID, history.StatusCompleted)
	waitForCacheEntries(t, cache, 0)
}

func TestStickyExecutionFallsBackToAnotherWorkerAfterOwnerStops(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
	registerE2EActivities(t, server)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "sticky-fallback"},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Minute`},
	)); err != nil {
		t.Fatal(err)
	}
	cache1, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	worker1 := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "owner-worker", BuildID: "build-1", ExecutionCache: cache1})
	ctx1, cancel1 := context.WithCancel(context.Background())
	worker1.Start(ctx1)
	if err := worker1.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "sticky-fallback", "sticky-fallback", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueActivityTask(t, activityTasks)
	state := waitForStickyOwner(t, mgr, runID, "owner-worker")
	cancel1()
	worker1.Stop()
	completeActivityTask(t, server, first, map[string]any{"first": "done"})

	state, err = mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, assigned, err := mgr.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "owner-worker", "build-1", time.Now().Add(-time.Second)); err != nil || !assigned {
		t.Fatalf("expire sticky assignment: assigned=%v err=%v", assigned, err)
	}
	if err := server.FallbackExpiredStickyWorkflowTasks(context.Background()); err != nil {
		t.Fatal(err)
	}

	cache2, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	worker2 := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "takeover-worker", BuildID: "build-1", ExecutionCache: cache2})
	ctx2, cancel2 := context.WithCancel(context.Background())
	worker2.Start(ctx2)
	t.Cleanup(func() {
		cancel2()
		worker2.Stop()
	})
	if err := worker2.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := dequeueActivityTask(t, activityTasks)
	if second.ActivityID != "#a2" {
		t.Fatalf("takeover activity = %+v", second)
	}
	waitForCacheEntries(t, cache2, 1)
	if snapshot := cache2.Snapshot(); snapshot.Admissions != 1 || snapshot.Hits != 0 {
		t.Fatalf("takeover cache snapshot = %+v", snapshot)
	}
	state = waitForStickyOwner(t, mgr, runID, "takeover-worker")
	if state.StickyGeneration < 2 {
		t.Fatalf("sticky generation = %d", state.StickyGeneration)
	}
}

func dequeueActivityTask(t *testing.T, queue *memory.TaskQueue) protocol.Task {
	t.Helper()
	// 10s: 全量 go test 时包级并行会放大调度延迟，3s 在慢机上会偶发超时。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return lease.Task
}

func completeActivityTask(t *testing.T, server *engine.Server, task protocol.Task, output map[string]any) {
	t.Helper()
	if _, err := server.HandleActivityStartedWithMetadata(task, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, output); err != nil {
		t.Fatal(err)
	}
}

func waitForRunStatus(t *testing.T, server *engine.Server, runID string, expected history.Status) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := server.GetRunStatus(runID)
		if err == nil && status == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := server.GetRunStatus(runID)
	t.Fatalf("run status = %s, err=%v, want %s", status, err, expected)
}

func waitForCacheEntries(t *testing.T, cache *executor.ExecutionCache, expected int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cache.Snapshot().Entries == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cache entries = %d, want %d", cache.Snapshot().Entries, expected)
}

func waitForStickyOwner(t *testing.T, mgr *memory.ExecutionManager, runID, owner string) *execution.WorkflowExecutionState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := mgr.GetWorkflowExecutionState(runID)
		if err == nil && state.StickyWorkerIdentity == owner {
			return state
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	t.Fatalf("sticky owner = %q err=%v, want %q", state.StickyWorkerIdentity, err, owner)
	return nil
}

// TestStickySessionAcceptsChangeVersionUpsertInCommittedTail 验证版本索引事件夹在命令事件之间时，
// 粘性会话的权威尾部校验通过，增量激活继续命中缓存。
func TestStickySessionAcceptsChangeVersionUpsertInCommittedTail(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
	registerE2EActivities(t, server)
	workflowID := "sticky-change-version"
	registerGeneratedWorkflow(t, server, workflowID, upgradeSourceB, execution.RunPolicyAutoUpgrade)
	cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{
		MaxEntries:        10,
		MaxEstimatedBytes: 16 << 20,
		IdleTTL:           time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := worker.NewWorkflowWorker(server, worker.WorkerOptions{
		Identity:       "sticky-version-worker",
		BuildID:        "build-1",
		ExecutionCache: cache,
	})
	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)
	t.Cleanup(func() {
		cancel()
		worker.Stop()
	})
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
	defer readyCancel()
	if err := worker.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, activityType := range []string{"activities.Step1", "activities.Step2", "activities.Step6"} {
		completeByType(t, server, activityTasks, activityType)
	}
	beforeCompletion := cache.Snapshot()
	completeByType(t, server, activityTasks, "activities.Step4")
	waitForRunStatus(t, server, runID, history.StatusCompleted)
	if beforeCompletion.Hits < 1 || beforeCompletion.Admissions != 1 {
		t.Fatalf("sticky cache snapshot = %+v, want incremental hits on one session", beforeCompletion)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if failures := workflowFailures(events); len(failures) != 0 {
		t.Fatalf("unexpected workflow task failures: %v", failures)
	}
}
