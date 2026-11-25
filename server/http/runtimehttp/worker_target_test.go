package runtimehttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestTargetAPI(t *testing.T) {
	store := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: store}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Namespace: "ns", Identity: "A", ActivityTypes: []string{"charge"}, TaskQueues: []string{"payments"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns", &program.WorkflowConfig{Type: "pay", Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "charge"},
		}}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	setTarget := func(identity string) (runtimeapi.WorkflowConfigRef, error) {
		var ref runtimeapi.WorkflowConfigRef
		err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeSetWorkerTargetPath, runtimeapi.WorkflowRuntimeSetWorkerTargetRequest{
			Namespace: "ns", WorkflowType: "pay", TargetWorkerIdentity: identity,
		}, &ref)
		return ref, err
	}
	ref, err := setTarget("A")
	if err != nil || ref.Version == "" {
		t.Fatalf("target API: %+v %v", ref, err)
	}
	var result runtimeapi.WorkflowConfigListResult
	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeListConfigsPath, runtimeapi.WorkflowRuntimeListConfigsRequest{Namespace: "ns"}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Items[0].TargetWorkerIdentity != "A" || result.Items[0].TargetWorkerStatus != "online" {
		t.Fatalf("list=%+v", result)
	}
	resp, err := http.Post(httpServer.URL+"/runtime/workflow/set-worker-target", "application/json", strings.NewReader(`{"namespace":"ns","workflow_type":"pay","target_worker_identity":"unknown"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("unknown status=%d", resp.StatusCode)
	}
	if _, err := setTarget(""); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleBodies(t *testing.T) {
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	ctx := context.Background()
	var schedule runtimeapi.Schedule
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeCreateSchedulePath, runtimeapi.ScheduleCreateRequest{
		Namespace: "ns", WorkflowType: "pay", CronExpression: "* * * * *", Paused: true,
	}, &schedule); err != nil {
		t.Fatal(err)
	}
	description := "edited"
	var updated runtimeapi.Schedule
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeUpdateSchedulePath, runtimeapi.WorkflowRuntimeUpdateScheduleRequest{
		Namespace: "ns", ScheduleID: schedule.ID, Update: runtimeapi.ScheduleUpdate{Description: &description},
	}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Description != "edited" {
		t.Fatalf("update body lost: %+v", updated)
	}
}
