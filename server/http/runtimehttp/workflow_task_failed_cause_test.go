package runtimehttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Worker 上报 Workflow Task 失败时只能用 Worker 的原因（non_deterministic_error、execution_session_invalid）。
// Server 自己写的「重做」原因（unhandled_events、run_resumed、reset_workflow）不计连续失败、不退避，Worker 上报
// 它们与未知原因都以 400 拒绝，History 不变。
func TestWorkflowTaskFailedRejectsCausesWorkersCannotReport(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: newRuntimeHTTPTestQueue(), ActivityRouter: newRuntimeHTTPTestQueue().activities()})
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, programtest.Workflow("failed-cause")); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "failed-cause", "failed-cause", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowTaskRuntimeHTTPHandler(server, server.TaskTokens(), server.Logger()))
	defer httpServer.Close()
	lease, err := pollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "cause-worker", WorkflowCapabilities: programtest.Noop().RequiredCapabilities(),
	}), httpServer.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	report := func(cause string) error {
		return wirePost(context.Background(), httpServer.URL, runtimeapi.WorkflowTaskRuntimeFailedPath,
			runtimeapi.WorkflowTaskRuntimeFailedRequest{TaskToken: lease.Task.TaskToken, Cause: cause, Reason: "worker report"}, nil)
	}
	failedEvents := func() int {
		events, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		return countRuntimeEvents(events, historypkg.EventWorkflowTaskFailed)
	}

	for _, cause := range []string{"unhandled_events", "run_resumed", "reset_workflow", "", "made_up"} {
		t.Run("Error Case: cause "+cause, func(t *testing.T) {
			err := report(cause)
			var status *wireStatusError
			if !errors.As(err, &status) || status.code != http.StatusBadRequest || !strings.Contains(status.body, "cannot be reported by a worker") {
				t.Fatalf("report %q: %v", cause, err)
			}
			if got := failedEvents(); got != 0 {
				t.Fatalf("rejected report wrote %d WorkflowTaskFailed events", got)
			}
		})
	}
	t.Run("Happy Path: a worker cause is recorded", func(t *testing.T) {
		if err := report(historypkg.WorkflowTaskFailedCauseNonDeterministic); err != nil {
			t.Fatal(err)
		}
		if got := failedEvents(); got != 1 {
			t.Fatalf("WorkflowTaskFailed events = %d, want 1", got)
		}
	})
}
