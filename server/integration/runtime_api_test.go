package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	runtimehttp2 "github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestAppWorkerReportsActivityResultThroughRuntimeHTTPAPI(t *testing.T) {
	ctx := context.Background()
	const namespace = "runtime-http"
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityQueues := memory.NewActivityQueueRouter(16)
	codec, err := dispatch.NewTaskTokenCodec([]byte("runtime-integration-secret-0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityQueues, TaskTokens: codec})
	defer server.Stop()

	runtimeServer := httptest.NewServer(runtimehttp2.NewRuntimeHTTPHandler(server))
	defer runtimeServer.Close()
	defer runtimeServer.CloseClientConnections()
	c, err := client.Dial(client.Options{HostPort: runtimeServer.URL, Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "runtime-api-result-reporting"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "custodian"`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(namespace, wf); err != nil {
		t.Fatal(err)
	}

	var callsMu sync.Mutex
	var calls int
	registry := activity.NewRegistry()
	registry.Register("custodian.Capture", func(ctx context.Context, input m) (m, error) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		return m{"captured": true}, nil
	})

	workflowWorker, err := worker.New(c, worker.Options{
		Role:                worker.RoleWorkflow,
		WorkflowConcurrency: 1,
		Identity:            "runtime-workflow-worker",
		BuildID:             "runtime-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	appWorker, err := worker.New(c, worker.Options{
		Role:               worker.RoleActivity,
		Activities:         registry,
		ActivityTaskQueues: []string{"custodian"},
		Identity:           "runtime-activity-worker",
		BuildID:            "runtime-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	workflowWorker.Start(ctx)
	appWorker.Start(ctx)
	defer workflowWorker.Stop()
	defer appWorker.Stop()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wf.Type}, wf.Type)
	if err != nil {
		t.Fatal(err)
	}
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := run.Get(getCtx, nil); err != nil {
		t.Fatal(err)
	}
	runID := run.GetRunID()

	callsMu.Lock()
	gotCalls := calls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("expected one app worker call, got %d", gotCalls)
	}
	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("expected one ActivityCompleted event from runtime API, got history %+v", history)
	}
}

func TestAppWorkerReportsActivityFailureThroughRuntimeHTTPAPI(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityQueues := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityQueues})
	defer server.Stop()

	runtimeServer := httptest.NewServer(runtimehttp2.NewRuntimeHTTPHandler(server))
	defer runtimeServer.Close()
	defer runtimeServer.CloseClientConnections()
	c, err := client.Dial(client.Options{HostPort: runtimeServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "runtime-api-failure-reporting"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "custodian", RetryPolicy: workflow.RetryPolicy{MaxAttempts: 1}`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	registry := activity.NewRegistry()
	registry.Register("custodian.Capture", func(ctx context.Context, input m) (m, error) {
		return nil, fmt.Errorf("declined")
	})

	workflowWorker, err := worker.New(c, worker.Options{Role: worker.RoleWorkflow, WorkflowConcurrency: 1, Identity: "failure-workflow-worker", BuildID: "runtime-test"})
	if err != nil {
		t.Fatal(err)
	}
	appWorker, err := worker.New(c, worker.Options{
		Role: worker.RoleActivity, Activities: registry, ActivityTaskQueues: []string{"custodian"}, Identity: "failure-activity-worker", BuildID: "runtime-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	workflowWorker.Start(ctx)
	appWorker.Start(ctx)
	defer workflowWorker.Stop()
	defer appWorker.Stop()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	waitForServerRunStatus(t, server, runID, historypkg.StatusFailed, 5*time.Second)

	history, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvent(history, historypkg.EventActivityFailed) != 1 {
		t.Fatalf("expected one ActivityFailed event from runtime API, got history %+v", history)
	}
}
