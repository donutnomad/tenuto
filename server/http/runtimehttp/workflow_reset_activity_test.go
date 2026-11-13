package runtimehttp

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestResetActivityHTTPRoundTrip(t *testing.T) {
	store := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: store}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()

	run := execution.NewRun(execution.NewRunOptions{ID: "run-reset-http", WorkflowID: "reset-http"})
	if err := store.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertActivityState(&execution.ActivityState{
		RunID: run.ID, ScheduledEventID: 1, ActivityID: "charge", ActivityType: "charge",
		Status: execution.ActivityStatusEnums.Scheduled, Attempt: 2, Stamp: 1, TaskQueue: "default",
	}); err != nil {
		t.Fatal(err)
	}

	if err := wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowRuntimeResetActivityPath,
		runtimeapi.WorkflowRuntimeResetActivityRequest{RunID: run.ID, ActivityID: "charge", Reason: "retry"}, nil); err != nil {
		t.Fatal(err)
	}
	state, ok, err := store.GetActivityState(run.ID, 1)
	if err != nil || !ok || state.Attempt != 1 || state.Stamp != 2 {
		t.Fatalf("reset activity state = %+v, present=%t, err=%v", state, ok, err)
	}
	events, err := store.GetEvents(run.ID)
	if err != nil || len(events) != 1 || events[0].ActivityReset == nil || events[0].ActivityReset.Reason != "retry" {
		t.Fatalf("reset history = %+v, err=%v", events, err)
	}
}
