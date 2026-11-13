package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 按 Run ID 寻址的请求只在 Run 自己的 namespace 里找得到它：namespace 不符、省略 namespace（即默认 namespace）、
// 或给出的 workflowID 不是这个 Run 的，都与 Run 不存在一样返回 ErrRunNotFound，Run 不受影响。
func TestRunAddressedRequestsEnforceNamespace(t *testing.T) {
	ctx := context.Background()
	server := assembly.NewServer(assembly.Options{
		Stores:         assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue:  memory.NewTaskQueue(16),
		ActivityRouter: memory.NewActivityQueueRouter(16),
	})
	t.Cleanup(server.Stop)
	if _, err := server.Definitions().RegisterWorkflowInNamespace("tenant-a", &program.WorkflowConfig{ID: "scoped", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	run, err := server.StartWorkflow(ctx, "tenant-a", "scoped", "scoped-1", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Happy Path：Run 自己的 namespace 读得到它。
	if got, err := getExternalRun(server, "tenant-a", "", run.ID); err != nil || got.ID != run.ID {
		t.Fatalf("own namespace: run=%v err=%v", got, err)
	}

	for _, scope := range []struct{ name, namespace, workflowID string }{
		{"other namespace", "tenant-b", ""},
		{"omitted namespace is default", "", ""},
		{"run of another workflow", "tenant-a", "other-workflow"},
	} {
		calls := map[string]func() error{
			"GetRun": func() error { _, err := getExternalRun(server, scope.namespace, scope.workflowID, run.ID); return err },
			"GetRunWithBlocked": func() error {
				_, err := server.View().GetRunWithBlocked(scope.namespace, scope.workflowID, run.ID)
				return err
			},
			"GetEvents": func() error {
				_, err := server.View().GetEvents(scope.namespace, scope.workflowID, run.ID)
				return err
			},
			"GetEventPage": func() error {
				_, err := server.View().GetEventPage(scope.namespace, scope.workflowID, execution.HistoryPageRequest{RunID: run.ID})
				return err
			},
			"Query": func() error {
				_, err := server.QueryWorkflow(ctx, scope.namespace, scope.workflowID, run.ID, "", nil)
				return err
			},
			"Signal": func() error {
				_, err := server.SignalWorkflow(ctx, scope.namespace, scope.workflowID, run.ID, "poke", "", nil)
				return err
			},
			"Update": func() error {
				_, err := server.UpdateWorkflow(ctx, scope.namespace, scope.workflowID, run.ID, "set", "", protocol.UpdateWaitStageEnums.Accepted, nil)
				return err
			},
			"UpsertSearchAttributes": func() error {
				return server.UpsertSearchAttributes(ctx, scope.namespace, scope.workflowID, run.ID, map[string]string{"k": "v"})
			},
			"Cancel":    func() error { return server.CancelWorkflow(ctx, scope.namespace, scope.workflowID, run.ID, "x") },
			"Terminate": func() error { return server.TerminateWorkflow(ctx, scope.namespace, scope.workflowID, run.ID, "x") },
		}
		if scope.workflowID == "" {
			calls["Reset"] = func() error {
				_, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{Namespace: scope.namespace, RunID: run.ID, ResetType: execution.ResetTypeEnums.FirstWorkflowTask})
				return err
			}
			calls["ResetActivity"] = func() error {
				return server.ResetActivity(scope.namespace, run.ID, "#a1", execution.ResetActivityOptions{})
			}
			calls["ActivityStates"] = func() error { _, err := server.View().GetActivityStates(scope.namespace, run.ID); return err }
			calls["GetConfig"] = func() error { _, err := server.View().GetWorkflowConfig(scope.namespace, run.ID, ""); return err }
			calls["SetRunPolicy"] = func() error { _, err := server.SetRunPolicy(ctx, scope.namespace, run.ID, "pinned"); return err }
		}
		for name, call := range calls {
			if err := call(); !errors.Is(err, execution.ErrRunNotFound) {
				t.Errorf("%s / %s: err = %v, want ErrRunNotFound", scope.name, name, err)
			}
		}
	}

	// Run 没有被任何越界请求改动。
	events, err := server.View().GetEvents("tenant-a", "", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowSignaled != nil || event.WorkflowCancellationRequested != nil || event.WorkflowTerminated != nil {
			t.Fatalf("an out-of-scope request changed the run: %+v", event)
		}
	}
}

// getExternalRun 读取请求寻址的 Run，Status 取 execution state 的投影。
func getExternalRun(s *engine.Server, namespace, workflowID, runID string) (*execution.Run, error) {
	run, _, err := s.View().GetRunWithState(namespace, workflowID, runID)
	return run, err
}
