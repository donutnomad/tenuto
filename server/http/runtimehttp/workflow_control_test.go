package runtimehttp

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowControlHTTPRoundTrip(t *testing.T) {
	store := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: store}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	ctx := context.Background()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "control-http", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := startedRunID(server.StartWorkflow(ctx, "default", "control-http", "control-http", "", nil)); err != nil {
		t.Fatal(err)
	}
	var stats runtimeapi.RunStats
	err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeRunStatsPath, runtimeapi.WorkflowRuntimeRunStatsRequest{Namespace: "default"}, &stats)
	if err != nil || stats.Total != 1 || stats.Running != 1 {
		t.Fatalf("run stats = %+v, err=%v", stats, err)
	}

	worker := protocol.ActivityWorkerVersion{
		Namespace: "default", Identity: "worker-control", BuildID: "build-control",
		TaskQueues: []string{"payments"}, ActivityTypes: []string{"charge"}, HeartbeatTTL: "1m",
	}
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeRegisterActivityWorkerPath, runtimeapi.WorkflowRuntimeRegisterActivityWorkerRequest{Worker: worker}, nil); err != nil {
		t.Fatal(err)
	}
	var workers runtimeapi.WorkerListResult
	err = wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeListWorkersPath, runtimeapi.WorkflowRuntimeListWorkersRequest{Filter: runtimeapi.WorkerListFilter{Namespace: "default"}}, &workers)
	if err != nil || workers.Total != 1 || workers.Items[0].Identity != worker.Identity {
		t.Fatalf("workers = %+v, err=%v", workers, err)
	}
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeDispatchPath, map[string]bool{}, nil); err != nil {
		t.Fatal(err)
	}
}
