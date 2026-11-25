package runtimehttp

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowRuntimeHTTPUpdateWithStartPreservesWorkflowType(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "approval-workflow", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()

	var resp runtimeapi.WorkflowRuntimeUpdateWithStartResponse
	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeUpdateWithStartPath, runtimeapi.WorkflowRuntimeUpdateWithStartRequest{
		WorkflowType: "approval-workflow", WorkflowID: "create-rec-http", StartInput: map[string]any{"record_id": "rec-http"},
		UpdateName: "create-record", UpdateID: "update-http", UpdateArgs: protocol.Payloads{},
		WaitStage: string(protocol.UpdateWaitStageEnums.Accepted),
	}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Result.UpdateID != "update-http" {
		t.Fatalf("result = %+v", resp.Result)
	}
	run, err := mgr.Get(resp.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowID != "create-rec-http" || run.WorkflowType != "approval-workflow" {
		t.Fatalf("run identity = %+v", run)
	}
}

func TestWorkflowRuntimeCapabilitiesDiscovery(t *testing.T) {
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()

	var capabilities runtimeapi.RuntimeCapabilities
	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeCapabilitiesPath, struct{}{}, &capabilities); err != nil {
		t.Fatal(err)
	}
	if len(capabilities.WorkflowCapabilities) != len(program.CurrentWLangCapabilities()) {
		t.Fatalf("workflow capabilities = %#v", capabilities.WorkflowCapabilities)
	}
}
