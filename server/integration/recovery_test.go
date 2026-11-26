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
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowTaskReplaysAfterEngineRestartFromPersistedConfig(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-replay"},
		sdkgentest.Step{Type: "complete", Options: `StartToCloseTimeout: time.Minute`},
	)

	creator := assembly.NewEngine(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, activity.NewRegistry(), worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	if _, err := creator.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := creator.Run(ctx, wf.Type, nil)
	if err != nil {
		t.Fatal(err)
	}

	registry := activity.NewRegistry()
	registry.Register("complete", func(ctx context.Context, input m) (m, error) {
		return m{"ok": true}, nil
	})
	recovered := assembly.NewEngine(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	recovered.Start(ctx)
	defer recovered.Stop()

	waitForRunStatus(t, recovered, ctx, runID, historypkg.StatusCompleted, 2*time.Second)
}

func TestActivityWorkerRouteRestoresAfterWorkflowAppRestart(t *testing.T) {
	ctx := context.Background()
	openStores := newReopenableWorkflowStores(t)
	workflowTasks := memory.NewTaskQueue(16)
	activityQueues := memory.NewActivityQueueRouter(16)
	firstServer := assembly.NewServer(assembly.Options{Stores: openStores(), WorkflowQueue: workflowTasks, ActivityRouter: activityQueues})
	if err := firstServer.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity:      "custodian-worker-1",
		BuildID:       "custodian@v1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()
	activityTasks := memory.NewActivityQueueRouter(16)
	recoveredServer := assembly.NewServer(assembly.Options{Stores: openStores(), WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	defer recoveredServer.Stop()

	ref, err := recoveredServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "route-restore"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute`},
	))
	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(recoveredServer.StartWorkflowVersion(ctx, ref, ref.WorkflowType, map[string]any{"order_id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	history, err := recoveredServer.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var workflowTaskID int64
	for _, event := range history {
		if event.EventType() == historypkg.EventWorkflowTaskScheduled {
			workflowTaskID = event.ID
		}
	}
	if workflowTaskID == 0 {
		t.Fatal("workflow task scheduled event missing")
	}

	if err := recoveredServer.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, recoveredServer, runID, workflowTaskID), runID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "custodian.Capture",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	// 恢复后的路由目录仍确定队列；任务不绑定 build，订阅这个队列的 Worker 能领。
	pollCtx, cancel := context.WithTimeout(protocol.WithWorkerInfo(ctx, protocol.WorkerInfo{Identity: "custodian-worker-1", BuildID: "custodian@v1"}), 5*time.Second)
	defer cancel()
	lease, err := activityTasks.Dequeue(pollCtx, "custodian")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.TaskQueue != "custodian" {
		t.Fatalf("unexpected recovered route task: %+v", lease.Task)
	}
}

func TestRetryContinuesAfterEngineRestart(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)

	var mu sync.Mutex
	attempts := 0
	firstRegistry := activity.NewRegistry()
	firstRegistry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		return nil, fmt.Errorf("temporary")
	})

	first := assembly.NewEngine(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, firstRegistry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-retry"},
		sdkgentest.Step{Type: "sometimes_fails", Options: `StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond, BackoffCoefficient: 1}`},
	)

	first.Start(ctx)
	if _, err := first.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := first.Run(ctx, wf.Type, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForEventCount(t, first, ctx, runID, historypkg.EventActivityAttemptFailed, 1, 2*time.Second)
	first.Stop()

	time.Sleep(300 * time.Millisecond)

	secondRegistry := activity.NewRegistry()
	secondRegistry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		return m{"ok": true}, nil
	})
	recovered := assembly.NewEngine(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, secondRegistry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	recovered.Start(ctx)
	defer recovered.Stop()

	waitForRunStatus(t, recovered, ctx, runID, historypkg.StatusCompleted, 2*time.Second)

	mu.Lock()
	gotAttempts := attempts
	mu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("expected retry attempt after restart, got %d attempts", gotAttempts)
	}
}

func TestScheduleToStartTimeoutRecoversAfterServerRestart(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	registerActivityRoutes(t, server, "never_started")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-schedule-to-start-timeout"},
		sdkgentest.Step{Type: "never_started", Options: `ScheduleToStartTimeout: 50 * time.Millisecond, StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		StartToCloseTimeout:    time.Minute,
		Type:                   historypkg.CmdScheduleActivity,
		ActivityID:             "#a1",
		ActivityType:           "never_started",
		ScheduleToStartTimeout: 50 * time.Millisecond,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	server.Stop()

	time.Sleep(80 * time.Millisecond)

	recovered := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	if err := recovered.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	defer recovered.Stop()

	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityTimedOut, 1, time.Second)
	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityFailed, 1, time.Second)
}

func TestHeartbeatTimeoutRecoversAfterServerRestart(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	registerActivityRoutes(t, server, "needs_heartbeat")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-heartbeat-timeout"},
		sdkgentest.Step{Type: "needs_heartbeat", Options: `StartToCloseTimeout: time.Minute, HeartbeatTimeout: 50 * time.Millisecond`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "needs_heartbeat",
		HeartbeatTimeout:    50 * time.Millisecond,
		RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 1},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	scheduledEventID := scheduledEventIDByStep(t, history, "#a1")
	_, _ = server.HandleActivityStartedWithMetadata(protocol.Task{RunID: runID, ScheduledEventID: scheduledEventID, Attempt: 1, Stamp: 1}, 0, 50*time.Millisecond)
	if err := server.HandleActivityHeartbeat(runID, scheduledEventID, 1, 1, m{"offset": float64(1)}); err != nil {
		t.Fatal(err)
	}
	server.Stop()

	time.Sleep(80 * time.Millisecond)

	recovered := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	if err := recovered.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	defer recovered.Stop()

	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityTimedOut, 1, time.Second)
	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityFailed, 1, time.Second)
}

func TestStartToCloseTimeoutRecoversAfterServerRestart(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	registerActivityRoutes(t, server, "slow_activity")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-start-to-close-timeout"},
		sdkgentest.Step{Type: "slow_activity", Options: `StartToCloseTimeout: 50 * time.Millisecond`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "slow_activity",
		StartToCloseTimeout: 50 * time.Millisecond,
		RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 1},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	scheduledEventID := scheduledEventIDByStep(t, history, "#a1")
	_, _ = server.HandleActivityStartedWithMetadata(protocol.Task{RunID: runID, ScheduledEventID: scheduledEventID, Attempt: 1, Stamp: 1}, 50*time.Millisecond, 0)
	server.Stop()

	time.Sleep(80 * time.Millisecond)

	recovered := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	if err := recovered.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	defer recovered.Stop()

	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityTimedOut, 1, time.Second)
	waitForServerEventCount(t, recovered, runID, historypkg.EventActivityFailed, 1, time.Second)
}

func TestWorkflowExecutionTimeoutRecoversAfterServerRestart(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "restart-workflow-timeout", ExecutionTimeout: "50ms"},
		sdkgentest.Step{Type: "pending_activity", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	server.Stop()

	time.Sleep(80 * time.Millisecond)

	recovered := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	if err := recovered.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	defer recovered.Stop()

	waitForServerRunStatus(t, recovered, runID, historypkg.StatusFailed, time.Second)
	waitForServerEventCount(t, recovered, runID, historypkg.EventWorkflowFailed, 1, time.Second)
}

func TestConcurrentServersDeduplicateScheduleCommandByStore(t *testing.T) {
	ctx := context.Background()
	workflowStores := newWorkflowStores(t)

	run := execution.NewRun(execution.NewRunOptions{ID: "run-concurrent-schedule", WorkflowID: "wf-concurrent"})
	if err := workflowStores.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID, inserted, err := workflowStores.Executions.AppendWorkflowTaskScheduled(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected workflow task schedule to be inserted")
	}

	serverA := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	registerActivityRoutes(t, serverA, "work")
	serverB := assembly.NewServer(assembly.Options{Stores: workflowStores, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	cmd := historypkg.Command{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "work",
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, server := range []*engine.Server{serverA, serverB} {
		wg.Add(1)
		go func(s *engine.Server) {
			defer wg.Done()
			errs <- s.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, s, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{cmd}, "")
		}(server)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	history, err := workflowStores.Executions.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvent(history, historypkg.EventActivityScheduled); got != 1 {
		t.Fatalf("expected one ActivityScheduled event, got %d: %+v", got, history)
	}
}

func scheduledEventIDByStep(t *testing.T, history []historypkg.Event, stepID string) int64 {
	t.Helper()
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled && event.ActivityScheduled.ActivityID == stepID {
			return event.ID
		}
	}
	t.Fatalf("missing scheduled event for %s: %+v", stepID, history)
	return 0
}
