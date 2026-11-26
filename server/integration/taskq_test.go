package integration

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/app/assembly"

	"github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	taskqgormstore "github.com/donutnomad/taskq/store/gormstore"
	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	sdkworker "github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEngineRunsThroughRealTaskQ(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 64,
		"activity-tasks": 64,
	})
	defer h.close()

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)

	registry := wfsdk.NewRegistry()
	workflowStore := newMySQLWorkflowStore(t)
	eng := assembly.NewEngine(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter}, registry, worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: 2})

	var callsMu sync.Mutex
	var calls []string
	registry.Register("load_order", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, "load_order")
		callsMu.Unlock()
		return m{"amount": int64(42)}, nil
	})
	registry.Register("charge_card", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, "charge_card")
		callsMu.Unlock()
		return m{"charged": input["amount"]}, nil
	})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-order"},
		sdkgentest.Step{Type: "load_order", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "charge_card", Input: `map[string]any{"amount": out0["amount"]}`, Options: `StartToCloseTimeout: time.Minute`},
	)

	eng.Start(ctx)
	defer eng.Stop()

	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.Type, m{"order_id": "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 2 || gotCalls[0] != "load_order" || gotCalls[1] != "charge_card" {
		t.Fatalf("unexpected activity calls: %v", gotCalls)
	}

	history, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	assertTemporalLikeHistory(t, history)
}

func TestSeparateWorkerRegistersActivitiesAndRunsThroughRealTaskQ(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})

	type captureInput struct {
		Value string `json:"value"`
	}
	type captureOutput struct {
		Seen string `json:"seen"`
	}

	registry := wfsdk.NewRegistry()
	var callsMu sync.Mutex
	var calls []string
	wfsdk.RegisterActivity[captureInput, captureOutput](registry, "capture", func(ctx context.Context, input captureInput) (captureOutput, error) {
		callsMu.Lock()
		calls = append(calls, input.Value)
		callsMu.Unlock()
		return captureOutput{Seen: input.Value}, nil
	})

	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "separate-taskq-worker"},
		sdkgentest.Step{Type: "capture", Input: `map[string]any{"value": "from-taskq"}`, Options: `StartToCloseTimeout: time.Minute`},
	)

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 1 || gotCalls[0] != "from-taskq" {
		t.Fatalf("unexpected worker calls: %v", gotCalls)
	}
}

func TestTaskQAppWorkersConsumeRoutedActivityQueues(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks":     4,
		"activity-custodian": 2,
		"activity-wallets":   2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, map[string]string{
		"custodian": "activity-custodian",
		"wallets":   "activity-wallets",
	})})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-app-worker-routing"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "custodian"`},
		sdkgentest.Step{Type: "wallets.Sign", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "wallets"`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	var callsMu sync.Mutex
	var calls []string
	custodianRegistry := wfsdk.NewRegistry()
	custodianRegistry.Register("custodian.Capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, "custodian")
		callsMu.Unlock()
		return m{"ok": true}, nil
	})
	walletsRegistry := wfsdk.NewRegistry()
	walletsRegistry.Register("wallets.Sign", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, "wallets")
		callsMu.Unlock()
		return m{"ok": true}, nil
	})

	workflowWorker := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role:                worker.WorkerRoleWorkflow,
		WorkflowConcurrency: 1,
	})
	custodianWorker := assembly.NewWorker(server, custodianRegistry, worker.WorkerOptions{
		Role:               worker.WorkerRoleActivity,
		ActivityTaskQueues: []string{"custodian"},
		Identity:           "custodian-worker",
	})
	walletsWorker := assembly.NewWorker(server, walletsRegistry, worker.WorkerOptions{
		Role:               worker.WorkerRoleActivity,
		ActivityTaskQueues: []string{"wallets"},
		Identity:           "wallets-worker",
	})
	workflowWorker.Start(ctx)
	custodianWorker.Start(ctx)
	walletsWorker.Start(ctx)
	defer workflowWorker.Stop()
	defer custodianWorker.Stop()
	defer walletsWorker.Stop()
	defer server.Stop()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 2 || gotCalls[0] != "custodian" || gotCalls[1] != "wallets" {
		t.Fatalf("unexpected routed calls: %v", gotCalls)
	}
}

func TestTaskQAppWorkerReportsActivityResultThroughRuntimeHTTPAPI(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks":     4,
		"activity-custodian": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, map[string]string{"custodian": "activity-custodian"})})
	runtimeServer := httptest.NewServer(runtimehttp.NewRuntimeHTTPHandler(server))
	defer runtimeServer.Close()
	defer runtimeServer.CloseClientConnections()
	c, err := client.Dial(client.Options{HostPort: runtimeServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-runtime-api-result-reporting"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "custodian"`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	var callsMu sync.Mutex
	var calls int
	registry := wfsdk.NewRegistry()
	registry.Register("custodian.Capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		return m{"ok": true}, nil
	})

	workflowWorker := assembly.NewWorker(server, wfsdk.NewRegistry(), worker.WorkerOptions{
		Role:                worker.WorkerRoleWorkflow,
		WorkflowConcurrency: 1,
	})
	// Activity Worker 经 runtime HTTP API 领取 taskq 里的 Activity Task 并上报结果。
	appWorker, err := sdkworker.New(c, sdkworker.Options{
		Role: sdkworker.RoleActivity, Activities: registry, ActivityTaskQueues: []string{"custodian"}, Identity: "taskq-http-activity-worker", BuildID: "taskq-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	workflowWorker.Start(ctx)
	appWorker.Start(ctx)
	defer workflowWorker.Stop()
	defer appWorker.Stop()
	defer server.Stop()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	gotCalls := calls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("expected one taskq app worker call, got %d", gotCalls)
	}
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("expected one ActivityCompleted event through runtime API, got history %+v", history)
	}
}

func TestGeneratedWorkflowRunsThroughRealTaskQWorker(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 4,
		"activity-tasks": 4,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})

	source := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func GeneratedWorkflow(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Capture, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	if err != nil {
		return err
	}
	return nil
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "generated-taskq-flow",
		FuncName:     "GeneratedWorkflow",
		Filename:     "generated_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	wf := generated.Workflow

	registry := wfsdk.NewRegistry()
	var callsMu sync.Mutex
	var calls []string
	registry.Register("activities.Capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, fmt.Sprintf("%v", input["order_id"]))
		callsMu.Unlock()
		return m{"seen": input["order_id"]}, nil
	})

	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", m{"order_id": "o-generated"}))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 10*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 1 || gotCalls[0] != "o-generated" {
		t.Fatalf("unexpected generated workflow calls: %v", gotCalls)
	}
}

func TestTaskQRetryContinuesAfterServerWorkerRestart(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	var callsMu sync.Mutex
	attempts := 0
	firstRegistry := wfsdk.NewRegistry()
	firstRegistry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		defer callsMu.Unlock()
		attempts++
		return nil, fmt.Errorf("temporary")
	})
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	firstWorker := assembly.NewWorker(firstServer, firstRegistry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	firstWorker.Start(ctx)

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-retry-restart"},
		sdkgentest.Step{Type: "sometimes_fails", Options: `StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 250 * time.Millisecond, BackoffCoefficient: 1}`},
	)

	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(firstServer.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerEventCount(t, firstServer, runID, historypkg.EventActivityAttemptFailed, 1, 5*time.Second)
	firstWorker.Stop()
	firstServer.Stop()

	time.Sleep(300 * time.Millisecond)

	secondRegistry := wfsdk.NewRegistry()
	secondRegistry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		defer callsMu.Unlock()
		attempts++
		return m{"ok": true}, nil
	})
	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	secondWorker := assembly.NewWorker(secondServer, secondRegistry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	secondWorker.Start(ctx)
	defer secondWorker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatus(t, secondServer, runID, historypkg.StatusCompleted, 5*time.Second)

	callsMu.Lock()
	gotAttempts := attempts
	callsMu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("expected retry attempt after restart, got %d attempts", gotAttempts)
	}
}

func TestTaskQScheduledActivityKeepsRetryPolicyAfterServerRestart(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-scheduled-retry-restart"},
		sdkgentest.Step{Type: "sometimes_fails", Options: `StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1}`},
	)
	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(firstServer.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := firstServer.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, firstServer, workflowStore.Executions, runID, openWorkflowTaskID(t, firstServer, runID)), runID, openWorkflowTaskID(t, firstServer, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "sometimes_fails",
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        2,
			InitialInterval:    10 * time.Millisecond,
			BackoffCoefficient: 1,
		},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()

	var callsMu sync.Mutex
	attempts := 0
	registry := wfsdk.NewRegistry()
	registry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		defer callsMu.Unlock()
		attempts++
		if attempts == 1 {
			return nil, fmt.Errorf("temporary")
		}
		return m{"ok": true}, nil
	})
	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	worker := assembly.NewWorker(secondServer, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatus(t, secondServer, runID, historypkg.StatusCompleted, 5*time.Second)

	callsMu.Lock()
	gotAttempts := attempts
	callsMu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("expected retry policy to survive server restart, got %d attempts", gotAttempts)
	}
}

// 持久队列里没有这个 Workflow Task，派发记录是 failed（入队失败后进程退出）：启动恢复按派发记录重新入队。
func TestTaskQRecoversOpenWorkflowTaskWhoseDispatchFailed(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-lost-workflow-task"},
		sdkgentest.Step{Type: "capture", Input: `map[string]any{"value": "from-workflow-recovery"}`, Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(firstServer.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	lostLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, lostLease); err != nil {
		t.Fatal(err)
	}
	if err := workflowStore.Dispatches.ReleaseTaskDispatch(protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: openWorkflowTaskID(t, firstServer, runID),
	}, "enqueue failed"); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()

	var callsMu sync.Mutex
	var calls []string
	registry := wfsdk.NewRegistry()
	registry.Register("capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, fmt.Sprintf("%v", input["value"]))
		callsMu.Unlock()
		return m{"ok": true}, nil
	})
	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	worker := assembly.NewWorker(secondServer, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatus(t, secondServer, runID, historypkg.StatusCompleted, 5*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 1 || gotCalls[0] != "from-workflow-recovery" {
		t.Fatalf("unexpected recovered workflow calls: %v", gotCalls)
	}
	history, err := secondServer.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	assertTemporalLikeHistory(t, history)
}

// 持久队列里没有这个 Activity 任务，派发记录是 failed（入队失败后进程退出）：启动恢复按派发记录重新入队。
func TestTaskQRecoversOpenActivityWhoseDispatchFailed(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	registerActivityRoutes(t, firstServer, "capture")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-lost-activity"},
		sdkgentest.Step{Type: "capture", Input: `map[string]any{"value": "from-recovery"}`, Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(firstServer.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	initialWorkflowLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, initialWorkflowLease); err != nil {
		t.Fatal(err)
	}
	if err := firstServer.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, firstServer, workflowStore.Executions, runID, openWorkflowTaskID(t, firstServer, runID)), runID, openWorkflowTaskID(t, firstServer, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "capture",
		Input:               m{"value": "from-recovery"},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	lostLease, err := activityRouter.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := activityRouter.Ack(ctx, lostLease); err != nil {
		t.Fatal(err)
	}
	if err := workflowStore.Dispatches.ReleaseTaskDispatch(lostLease.Task, "enqueue failed"); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()

	var callsMu sync.Mutex
	var calls []string
	registry := wfsdk.NewRegistry()
	registry.Register("capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls = append(calls, fmt.Sprintf("%v", input["value"]))
		callsMu.Unlock()
		return m{"ok": true}, nil
	})
	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	worker := assembly.NewWorker(secondServer, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatus(t, secondServer, runID, historypkg.StatusCompleted, 5*time.Second)

	callsMu.Lock()
	gotCalls := append([]string(nil), calls...)
	callsMu.Unlock()
	if len(gotCalls) != 1 || gotCalls[0] != "from-recovery" {
		t.Fatalf("unexpected recovered activity calls: %v", gotCalls)
	}
	history, err := secondServer.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventActivityScheduled) != 1 || countEvent(history, historypkg.EventActivityStarted) != 1 || countEvent(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("unexpected recovered activity history: %+v", history)
	}
}

func TestTaskQCompensationContinuesAfterServerWorkerRestart(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	registerActivityRoutes(t, firstServer, "charge", "refund")

	wf := compensationWorkflow(t, "taskq-compensation-restart")
	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(firstServer.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	initialWorkflowLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, initialWorkflowLease); err != nil {
		t.Fatal(err)
	}
	if err := firstServer.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, firstServer, workflowStore.Executions, runID, openWorkflowTaskID(t, firstServer, runID)), runID, openWorkflowTaskID(t, firstServer, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "charge",
		RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 1},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	chargeLease, err := activityRouter.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = firstServer.HandleActivityStartedWithMetadata(chargeLease.Task, 0, 0)
	if err := firstServer.HandleActivityFailed(ctx, chargeLease.Task.RunID, chargeLease.Task.ScheduledEventID, chargeLease.Task.Attempt, chargeLease.Task.Stamp, protocol.Failure{Message: "declined"}); err != nil {
		t.Fatal(err)
	}
	if err := activityRouter.Ack(ctx, chargeLease); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()

	var callsMu sync.Mutex
	var refundReasons []string
	registry := wfsdk.NewRegistry()
	registry.Register("refund", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		refundReasons = append(refundReasons, fmt.Sprintf("%v", input["reason"]))
		callsMu.Unlock()
		return m{"refunded": true}, nil
	})
	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	worker := assembly.NewWorker(secondServer, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatus(t, secondServer, runID, historypkg.StatusFailed, 5*time.Second)
	waitForServerEventCount(t, secondServer, runID, historypkg.EventActivityCompleted, 1, time.Second)

	callsMu.Lock()
	gotReasons := append([]string(nil), refundReasons...)
	callsMu.Unlock()
	if len(gotReasons) != 1 || gotReasons[0] != "declined" {
		t.Fatalf("unexpected refund reasons: %v", gotReasons)
	}

	history, err := secondServer.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countStepScheduled(history, compensationRefundStepID) != 1 {
		t.Fatalf("expected one refund schedule, got history %+v", history)
	}
}

func TestTaskQSkipsQueuedActivityAfterWorkflowTerminated(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	registerActivityRoutes(t, server, "must_skip")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-terminal-skip"},
		sdkgentest.Step{Type: "must_skip", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	initialWorkflowLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, initialWorkflowLease); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, workflowStore.Executions, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "must_skip",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	if err := server.TerminateWorkflow(context.Background(), "", "", runID, "manual stop"); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	registry := wfsdk.NewRegistry()
	registry.Register("must_skip", func(ctx context.Context, input m) (m, error) {
		called <- struct{}{}
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer server.Stop()

	select {
	case <-called:
		t.Fatal("terminal workflow activity was executed")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestTaskQSkipsClosedActivityDuplicateByEventHistory(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	registerActivityRoutes(t, server, "must_skip")
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-closed-activity-skip"},
		sdkgentest.Step{Type: "must_skip", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	initialWorkflowLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, initialWorkflowLease); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, workflowStore.Executions, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "must_skip",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	originalLease, err := activityRouter.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = server.HandleActivityStartedWithMetadata(originalLease.Task, 0, 0)
	if err := server.HandleActivityCompleted(ctx, originalLease.Task.RunID, originalLease.Task.ScheduledEventID, originalLease.Task.Attempt, originalLease.Task.Stamp, m{"ok": true}); err != nil {
		t.Fatal(err)
	}
	if err := activityRouter.Ack(ctx, originalLease); err != nil {
		t.Fatal(err)
	}
	if err := activityRouter.Enqueue(ctx, originalLease.Task.TaskQueue, originalLease.Task); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	registry := wfsdk.NewRegistry()
	registry.Register("must_skip", func(ctx context.Context, input m) (m, error) {
		called <- struct{}{}
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer server.Stop()

	select {
	case <-called:
		t.Fatal("closed activity duplicate was executed")
	case <-time.After(200 * time.Millisecond):
	}
}

// 已 started 的 attempt 在队列里多出一份副本：Worker 领到后 Begin 判定不执行，确认丢弃。
func TestTaskQSkipsStartedActivityDuplicate(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})
	registerActivityRoutes(t, server, "must_skip")
	defer server.Stop()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-started-activity-skip"},
		sdkgentest.Step{Type: "must_skip", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	initialWorkflowLease, err := workflowQueue.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Ack(ctx, initialWorkflowLease); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(workflowTaskContext(t, ctx, server, workflowStore.Executions, runID, openWorkflowTaskID(t, server, runID)), runID, openWorkflowTaskID(t, server, runID), []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "#a1",
		ActivityType:        "must_skip",
		RetryPolicy:         &protocol.ParsedRetryPolicy{MaxAttempts: 1},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	originalLease, err := activityRouter.Dequeue(ctx, protocol.DefaultActivityTaskQueue)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = server.HandleActivityStartedWithMetadata(originalLease.Task, 0, 0)
	if err := activityRouter.Ack(ctx, originalLease); err != nil {
		t.Fatal(err)
	}
	if err := activityRouter.Enqueue(ctx, originalLease.Task.TaskQueue, originalLease.Task); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	registry := wfsdk.NewRegistry()
	registry.Register("must_skip", func(ctx context.Context, input m) (m, error) {
		called <- struct{}{}
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()

	// 副本被确认丢弃：函数体不进入，attempt 仍是 started，等待它的超时。
	select {
	case <-called:
		t.Fatal("started activity duplicate was executed")
	case <-time.After(time.Second):
	}
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventActivityStarted) != 1 || countEvent(history, historypkg.EventActivityCompleted) != 0 {
		t.Fatalf("unexpected started duplicate history: %+v", history)
	}
}

func TestTaskQDuplicateWorkflowTaskDoesNotDoubleScheduleActivity(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})

	release := make(chan struct{})
	registry := wfsdk.NewRegistry()
	registry.Register("blocked", func(ctx context.Context, input m) (m, error) {
		<-release
		return m{"ok": true}, nil
	})
	worker := assembly.NewWorker(server, registry, worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: 1})
	worker.Start(ctx)
	defer worker.Stop()
	defer server.Stop()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "taskq-duplicate-workflow-task"},
		sdkgentest.Step{Type: "blocked", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	started, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Enqueue(ctx, protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: firstWorkflowTaskScheduledID(t, started)}); err != nil {
		t.Fatal(err)
	}

	waitForServerEventCount(t, server, runID, historypkg.EventActivityScheduled, 1, 5*time.Second)
	time.Sleep(200 * time.Millisecond)

	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvent(history, historypkg.EventActivityScheduled); got != 1 {
		t.Fatalf("expected one ActivityScheduled event, got %d: %+v", got, history)
	}

	close(release)
	waitForServerRunStatus(t, server, runID, historypkg.StatusCompleted, 5*time.Second)
}

func TestTaskQWorkflowTaskTimeoutRequeuesAndCompletesWithNewWorker(t *testing.T) {
	ctx := context.Background()
	h := newTaskQHarness(t, map[string]int{
		"workflow-tasks": 2,
		"activity-tasks": 2,
	})
	defer h.close()

	workflowStore := newMySQLWorkflowStore(t)

	workflowFE := h.frontend(t, "workflow-tasks")
	activityRouter := h.activityRouter(t, nil)
	workflowQueue := taskq.NewQueue(workflowFE)
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(workflowFE), ActivityRouter: activityRouter})

	const activityCount = 5000
	wf := sdkgentest.Loop(t, &program.WorkflowConfig{Type: "taskq-workflow-task-timeout"}, "already_done", activityCount)
	ref, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	run := execution.NewRun(execution.NewRunOptions{
		ID: "run-taskq-workflow-task-timeout", WorkflowID: ref.WorkflowType, WorkflowType: ref.WorkflowType, WorkflowConfigVersion: ref.Version,
	})
	if err := workflowStore.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	if _, err := workflowStore.Executions.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < activityCount; i++ {
		stepID := fmt.Sprintf("#a%d", i+1)
		scheduledID, err := workflowStore.Executions.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: stepID}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := workflowStore.Executions.Append(run.ID, run.ID, run.ID, historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
			ScheduledEventID: scheduledID,
			Output:           m{"ok": true},
		})); err != nil {
			t.Fatal(err)
		}
	}
	workflowTaskID, err := workflowStore.Executions.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowQueue.Enqueue(ctx, protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: workflowTaskID}); err != nil {
		t.Fatal(err)
	}

	firstWorker := assembly.NewWorker(firstServer, wfsdk.NewRegistry(), worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
		WorkflowTaskTimeout: time.Nanosecond,
	})
	firstWorker.Start(ctx)
	waitForServerEventCount(t, firstServer, run.ID, historypkg.EventWorkflowTaskTimedOut, 1, 30*time.Second)
	waitForServerEventCount(t, firstServer, run.ID, historypkg.EventWorkflowTaskScheduled, 2, 30*time.Second)
	firstWorker.Stop()
	time.Sleep(300 * time.Millisecond)
	firstServer.Stop()

	secondServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(workflowStore), WorkflowQueue: taskq.NewQueue(h.frontend(t, "workflow-tasks")), ActivityRouter: h.activityRouter(t, nil)})
	if err := secondServer.RecoverRunningWorkflows(ctx, engine.DefaultReconcileLookahead); err != nil {
		t.Fatal(err)
	}
	secondWorker := assembly.NewWorker(secondServer, wfsdk.NewRegistry(), worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
		WorkflowTaskTimeout: 60 * time.Second,
	})
	secondWorker.Start(ctx)
	defer secondWorker.Stop()
	defer secondServer.Stop()

	waitForServerRunStatusWithHistory(t, secondServer, run.ID, historypkg.StatusCompleted, 90*time.Second)
	history, err := secondServer.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventWorkflowTaskTimedOut) == 0 {
		t.Fatalf("missing WorkflowTaskTimedOut in history: %+v", history)
	}
	if countEvent(history, historypkg.EventWorkflowCompleted) != 1 {
		t.Fatalf("expected one WorkflowCompleted event, got history %+v", history)
	}
}

func waitForServerRunStatusWithHistory(t *testing.T, server *engine.Server, runID string, want historypkg.Status, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := server.GetRunStatus(runID)
		if err == nil && status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := server.GetRunStatus(runID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}
	history, _ := server.GetEvents(runID)
	tail := history
	if len(tail) > 20 {
		tail = tail[len(tail)-20:]
	}
	t.Fatalf("expected run status %s, got %s, history events=%d, workflow_scheduled=%d, workflow_started=%d, workflow_completed=%d, workflow_timed_out=%d, workflow_done=%d: %+v",
		want,
		status,
		len(history),
		countEvent(history, historypkg.EventWorkflowTaskScheduled),
		countEvent(history, historypkg.EventWorkflowTaskStarted),
		countEvent(history, historypkg.EventWorkflowTaskCompleted),
		countEvent(history, historypkg.EventWorkflowTaskTimedOut),
		countEvent(history, historypkg.EventWorkflowCompleted),
		tail,
	)
}

func assertTemporalLikeHistory(t *testing.T, history []historypkg.Event) {
	t.Helper()

	seen := map[historypkg.EventType]int{}
	scheduledIDs := map[int64]int{}
	startedIDs := map[int64]int{}
	completedIDs := map[int64]int{}
	workflowTaskScheduledIDs := map[int64]int{}
	workflowTaskStartedIDs := map[int64]int{}
	workflowTaskCompletedIDs := map[int64]int{}
	positionsByType := map[historypkg.EventType][]int{}
	for pos, event := range history {
		typ := event.EventType()
		seen[typ]++
		positionsByType[typ] = append(positionsByType[typ], pos)
		if typ == historypkg.EventWorkflowTaskScheduled {
			workflowTaskScheduledIDs[event.ID] = pos
		}
		if typ == historypkg.EventWorkflowTaskStarted && event.WorkflowTaskStarted != nil {
			workflowTaskStartedIDs[event.WorkflowTaskStarted.ScheduledEventID] = pos
		}
		if typ == historypkg.EventWorkflowTaskCompleted && event.WorkflowTaskCompleted != nil {
			workflowTaskCompletedIDs[event.WorkflowTaskCompleted.ScheduledEventID] = pos
		}
		if typ == historypkg.EventActivityScheduled {
			scheduledIDs[event.ID] = pos
		}
		if typ == historypkg.EventActivityStarted && event.ActivityStarted != nil {
			startedIDs[event.ActivityStarted.ScheduledEventID] = pos
		}
		if typ == historypkg.EventActivityCompleted && event.ActivityCompleted != nil {
			completedIDs[event.ActivityCompleted.ScheduledEventID] = pos
		}
	}

	required := []historypkg.EventType{
		historypkg.EventWorkflowStarted,
		historypkg.EventWorkflowTaskScheduled,
		historypkg.EventWorkflowTaskStarted,
		historypkg.EventWorkflowTaskCompleted,
		historypkg.EventActivityScheduled,
		historypkg.EventActivityStarted,
		historypkg.EventActivityCompleted,
		historypkg.EventWorkflowCompleted,
	}
	for _, typ := range required {
		if seen[typ] == 0 {
			t.Fatalf("missing event type %s in history: %+v", typ, history)
		}
	}
	for scheduledID, scheduledPos := range workflowTaskScheduledIDs {
		startedPos, started := workflowTaskStartedIDs[scheduledID]
		if !started {
			t.Fatalf("workflow task scheduled event %d has no start", scheduledID)
		}
		completedPos, completed := workflowTaskCompletedIDs[scheduledID]
		if !completed {
			t.Fatalf("workflow task scheduled event %d has no completion", scheduledID)
		}
		if scheduledPos > startedPos || startedPos > completedPos {
			t.Fatalf("workflow task event order mismatch for %d: %+v", scheduledID, history)
		}
	}
	for scheduledID := range scheduledIDs {
		startedPos, started := startedIDs[scheduledID]
		if !started {
			t.Fatalf("scheduled event %d has no start", scheduledID)
		}
		completedPos, completed := completedIDs[scheduledID]
		if !completed {
			t.Fatalf("scheduled event %d has no completion", scheduledID)
		}
		scheduledPos := scheduledIDs[scheduledID]
		if scheduledPos > startedPos || startedPos > completedPos {
			t.Fatalf("activity event order mismatch for %d: %+v", scheduledID, history)
		}
	}
	assertEventOrder(t, history, positionsByType, []historypkg.EventType{
		historypkg.EventWorkflowStarted,
		historypkg.EventWorkflowTaskScheduled,
		historypkg.EventWorkflowTaskStarted,
		historypkg.EventWorkflowTaskCompleted,
		historypkg.EventActivityScheduled,
		historypkg.EventActivityStarted,
		historypkg.EventActivityCompleted,
		historypkg.EventWorkflowCompleted,
	})
}

func assertEventOrder(t *testing.T, history []historypkg.Event, positionsByType map[historypkg.EventType][]int, order []historypkg.EventType) {
	t.Helper()

	last := -1
	for _, typ := range order {
		positions := positionsByType[typ]
		if len(positions) == 0 {
			t.Fatalf("missing event type %s in history: %+v", typ, history)
		}
		pos := positions[0]
		if pos < last {
			t.Fatalf("event %s appears before expected order: %+v", typ, history)
		}
		last = pos
	}
}

func waitForServerRunStatus(t *testing.T, server *engine.Server, runID string, want historypkg.Status, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := server.GetRunStatus(runID)
		if err == nil && status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := server.GetRunStatus(runID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}
	t.Fatalf("expected run status %s, got %s", want, status)
}

func waitForServerEventCount(t *testing.T, server *engine.Server, runID string, typ historypkg.EventType, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		history, err := server.GetEvents(runID)
		if err == nil && countEvent(history, typ) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	history, _ := server.GetEvents(runID)
	t.Fatalf("expected at least %d %s events, got %d: %+v", want, typ, countEvent(history, typ), history)
}

type taskQHarness struct {
	monitor membership.Monitor
	server  *matching.Server
	store   *taskqgormstore.Store
}

func newTaskQHarness(t *testing.T, queues map[string]int) *taskQHarness {
	t.Helper()
	return newTaskQHarnessWithOptions(t, queues, taskQHarnessOptions{})
}

// taskQHarnessOptions 调整测试用的 TaskQ：VisTimeout 是租约的可见性超时（零值取 TaskQ 默认的 60s）。
type taskQHarnessOptions struct {
	VisTimeout time.Duration
}

func newTaskQHarnessWithOptions(t *testing.T, queues map[string]int, options taskQHarnessOptions) *taskQHarness {
	t.Helper()

	// TaskQ 只有 GORM 存储：存储放在 mysqltest 分配的独立 MySQL 库里，表按 TaskQ 的 schema.sql 建好；
	// 没有开启 MySQL 后端测试时跳过 t。
	store, err := taskqgormstore.New(mysqltest.NewTaskQDB(t), taskqgormstore.Config{})
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	addrPort, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	monitor := membership.NewMonitor(
		[]string{"matching"},
		newMemoryMemberStore(),
		addrPort,
		50*time.Millisecond,
		50*time.Millisecond,
		time.Time{},
		100,
	)
	monitor.Start()
	if err := monitor.WaitUntilInitialized(context.Background()); err != nil {
		t.Fatal(err)
	}

	srv, err := matching.NewServer(matching.Config{
		Store:         store,
		Monitor:       monitor,
		ListenAddr:    addr,
		BroadcastAddr: addr,
		VisTimeout:    options.VisTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}

	// per-queue 分区配置已从启动期静态 Config 迁移到运行时 DB partition_config 表。
	// 直接写 override 行落定初始分区，绕过 ResizePartitions 的静默期/drain 编排
	// （那是为生产缩扩容设计的、会拖慢测试）。须在 Start() 前写好，queue 首次被 poll 时即就位。
	for name, target := range queues {
		if err := store.SetPartitions(context.Background(), name, target, target); err != nil {
			t.Fatal(err)
		}
	}

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	return &taskQHarness{monitor: monitor, server: srv, store: store}
}

func (h *taskQHarness) frontend(t *testing.T, queueName string) *frontend.Frontend {
	t.Helper()

	fe, err := h.dialFrontend(context.Background(), queueName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fe.Close)
	return fe
}

func (h *taskQHarness) dialFrontend(_ context.Context, queueName string) (*frontend.Frontend, error) {
	return frontend.New(frontend.Config{
		Monitor:     h.monitor,
		QueueName:   queueName,
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 250 * time.Millisecond,
	})
}

// activityRouter 返回把 route 名解析到给定物理 queue 的 activity Router，测试结束时关闭。
func (h *taskQHarness) activityRouter(t *testing.T, routes map[string]string) *taskq.ActivityQueueRouter {
	t.Helper()

	router := taskq.NewLazyActivityQueueRouter(routes, h.dialFrontend)
	t.Cleanup(router.Close)
	return router
}

func (h *taskQHarness) close() {
	if h.server != nil {
		h.server.Stop()
	}
	if h.monitor != nil {
		h.monitor.Stop()
	}
	if h.store != nil {
		_ = h.store.Close()
	}
}

type memoryMemberStore struct {
	mu      sync.Mutex
	members map[string]membership.MemberRecord
}

func newMemoryMemberStore() *memoryMemberStore {
	return &memoryMemberStore{members: make(map[string]membership.MemberRecord)}
}

func (s *memoryMemberStore) UpsertMember(ctx context.Context, req membership.UpsertMemberRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s/%s:%d", req.ServiceName, req.RPCAddress, req.RPCPort)
	s.members[key] = membership.MemberRecord{
		RPCAddress:  req.RPCAddress,
		RPCPort:     req.RPCPort,
		ServiceName: req.ServiceName,
		Draining:    req.Draining,
		StartAt:     req.StartAt,
	}
	return nil
}

func (s *memoryMemberStore) GetActiveMembers(ctx context.Context, ttl time.Duration) ([]membership.MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]membership.MemberRecord, 0, len(s.members))
	for _, record := range s.members {
		records = append(records, record)
	}
	return records, nil
}

// openWorkflowTaskID 返回 run 当前打开的 Workflow Task 的 scheduled 事件 ID。
func openWorkflowTaskID(t testing.TB, server *engine.Server, runID string) int64 {
	t.Helper()
	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Errorf("read execution state of %s: %v", runID, err)
		return 0
	}
	return state.CurrentWorkflowTaskScheduledEventID()
}

// workflowTaskContext 代替 Worker poll 给直接提交 Workflow Task 的用例准备上下文：像 poll 一样先记下
// WorkflowTaskStarted（已 started 时跳过），再签发 token。Server 以 Started 为 Worker 看到的边界，没有 Started 的
// 完成会被拒绝。
func workflowTaskContext(t testing.TB, ctx context.Context, server *engine.Server, mgr execution.ExecutionManager, runID string, scheduledEventID int64) context.Context {
	t.Helper()
	startWorkflowTaskLikePoll(t, mgr, runID, scheduledEventID)
	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Errorf("read execution state of %s: %v", runID, err)
		return ctx
	}
	token, err := server.TaskTokens().Encode(protocol.Task{
		Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: scheduledEventID,
		ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
	})
	if err != nil {
		t.Errorf("sign workflow task token of %s: %v", runID, err)
		return ctx
	}
	return protocol.WithWorkflowTaskToken(ctx, token)
}

// startWorkflowTaskLikePoll 在 open、尚未 started 的 Workflow Task 上写入 WorkflowTaskStarted，与 poll 领取时相同。
func startWorkflowTaskLikePoll(t testing.TB, mgr execution.ExecutionManager, runID string, scheduledEventID int64) {
	t.Helper()
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Errorf("read execution state of %s: %v", runID, err)
		return
	}
	if !state.WorkflowTaskOpen(scheduledEventID) || state.WorkflowTask.StartedScheduledEventID == scheduledEventID {
		return
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Errorf("read run %s: %v", runID, err)
		return
	}
	branch, err := execution.DeserializeBranchToken(run.BranchToken)
	if err != nil {
		t.Errorf("run %s branch: %v", runID, err)
		return
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.StartWorkflowTask(tx, execution.WorkflowTaskStart{
			RunID: runID, TreeID: branch.TreeID, BranchID: branch.BranchID, ScheduledEventID: scheduledEventID,
			ExecutionGeneration: state.ExecutionGeneration, StickyGeneration: state.StickyGeneration,
			ProgramVersion: run.WorkflowConfigVersion, Now: time.Now(),
		})
		return err
	}); err != nil {
		t.Errorf("start workflow task %d of %s: %v", scheduledEventID, runID, err)
	}
}

// firstWorkflowTaskScheduledID 返回 history 里第一个 Workflow Task 的 scheduled 事件 ID。
func firstWorkflowTaskScheduledID(t testing.TB, events []historypkg.Event) int64 {
	t.Helper()
	for _, event := range events {
		if event.WorkflowTaskScheduled != nil {
			return event.ID
		}
	}
	t.Fatal("history has no WorkflowTaskScheduled event")
	return 0
}

// startedRunID 取 Server 启动结果里的 Run ID。
func startedRunID(run *execution.Run, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return run.ID, nil
}
