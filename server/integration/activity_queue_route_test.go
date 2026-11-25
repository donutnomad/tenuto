package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestAppWorkersOnlyConsumeTheirActivityQueues(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityQueues := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityQueues})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "app-worker-routing"},
		sdkgentest.Step{Type: "custodian.Capture", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "custodian"`},
		sdkgentest.Step{Type: "wallets.Sign", Options: `StartToCloseTimeout: time.Minute, TaskQueue: "wallets"`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var calls []string
	custodianRegistry := activity.NewRegistry()
	custodianRegistry.Register("custodian.Capture", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		mu.Lock()
		calls = append(calls, "custodian")
		mu.Unlock()
		return map[string]any{"ok": true}, nil
	})
	walletsRegistry := activity.NewRegistry()
	walletsRegistry.Register("wallets.Sign", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		mu.Lock()
		calls = append(calls, "wallets")
		mu.Unlock()
		return map[string]any{"ok": true}, nil
	})

	workflowWorker := assembly.NewWorker(server, activity.NewRegistry(), worker.WorkerOptions{
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
	waitForServerRunStatus(t, server, runID, history.StatusCompleted, 5*time.Second)

	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "custodian" || got[1] != "wallets" {
		t.Fatalf("unexpected app worker calls: %v", got)
	}
}
