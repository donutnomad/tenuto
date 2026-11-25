package runtimehttp

import (
	"context"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 超过 2^53 的整数经 start / signal / signal-with-start / schedule 的 HTTP 入口原样进入引擎：
// 整数是 int64，超过 int64 的是 uint64，带小数的是 float64。
func TestWorkflowRuntimeHTTPPreservesPayloadNumbers(t *testing.T) {
	const above53 = int64(1)<<53 + 1
	payload := func() map[string]any {
		return map[string]any{"above_2_53": above53, "above_int64": uint64(math.MaxUint64), "fraction": 1.5, "nested": []any{above53}}
	}
	assertNumbers := func(t *testing.T, label string, got map[string]any) {
		t.Helper()
		nested, _ := got["nested"].([]any)
		if got["above_2_53"] != above53 || got["above_int64"] != uint64(math.MaxUint64) || got["fraction"] != 1.5 || len(nested) != 1 || nested[0] != above53 {
			t.Fatalf("%s numbers = %#v", label, got)
		}
	}

	mgr := memory.NewExecutionManager()
	scheduleStore := memory.NewScheduleRepository()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr, Schedules: scheduleStore}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns", &program.WorkflowConfig{Type: "pay", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()
	ctx := context.Background()

	var started runtimeapi.WorkflowRuntimeStartResponse
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeStartPath, runtimeapi.WorkflowRuntimeStartRequest{
		Namespace: "ns", WorkflowType: "pay", WorkflowID: "order-1", Input: payload(),
	}, &started); err != nil {
		t.Fatal(err)
	}
	runID := started.RunID
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	assertNumbers(t, "start input", run.Input)

	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeSignalPath, runtimeapi.WorkflowRuntimeInteractionRequest{
		Namespace: "ns", WorkflowID: "order-1", Name: "notify", Input: payload(),
	}, nil); err != nil {
		t.Fatal(err)
	}
	var withStart runtimeapi.WorkflowRuntimeSignalWithStartResponse
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeSignalWithStartPath, runtimeapi.WorkflowRuntimeSignalWithStartRequest{
		Namespace: "ns", WorkflowType: "pay", WorkflowID: "order-2", StartInput: payload(), SignalName: "notify", SignalInput: payload(),
	}, &withStart); err != nil {
		t.Fatal(err)
	}
	withStartRunID := withStart.RunID
	withStartRun, err := mgr.Get(withStartRunID)
	if err != nil {
		t.Fatal(err)
	}
	assertNumbers(t, "signal-with-start start input", withStartRun.Input)
	for _, id := range []string{runID, withStartRunID} {
		events, err := mgr.GetEvents(id)
		if err != nil {
			t.Fatal(err)
		}
		signals := 0
		for _, event := range events {
			if event.WorkflowSignaled != nil {
				assertNumbers(t, "signal input", event.WorkflowSignaled.Input)
				signals++
			}
		}
		if signals != 1 {
			t.Fatalf("run %s has %d signals, want 1", id, signals)
		}
	}

	var created runtimeapi.Schedule
	if err := wirePost(ctx, httpServer.URL, runtimeapi.WorkflowRuntimeCreateSchedulePath, runtimeapi.ScheduleCreateRequest{
		Namespace: "ns", WorkflowType: "pay", CronExpression: "0 0 * * *", Input: payload(), Paused: true,
	}, &created); err != nil {
		t.Fatal(err)
	}
	stored, err := scheduleStore.GetSchedule("ns", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertNumbers(t, "schedule input", stored.Input)
}
