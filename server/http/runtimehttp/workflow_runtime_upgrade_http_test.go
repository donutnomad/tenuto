package runtimehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func newUpgradeHTTPTest(t *testing.T) (*engine.Server, *httptest.Server) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	handler := NewWorkflowRuntimeHTTPHandler(server)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return server, httpServer
}

func postPath(t *testing.T, base, path string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(base+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

// TestArtifactsAndSetPolicyHTTP 固定两个新增入口的接线：artifacts 只登记、set-policy 覆盖策略。
func TestArtifactsAndSetPolicyHTTP(t *testing.T) {
	server, httpServer := newUpgradeHTTPTest(t)
	ctx := context.Background()

	register := postPath(t, httpServer.URL, "/runtime/workflow/register", map[string]any{
		"namespace": "default",
		"workflow":  program.WorkflowConfig{ID: "http-flow", Name: "A", Policy: "auto_upgrade", Program: programtest.Noop()},
	})
	if register.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", register.StatusCode)
	}
	artifact := postPath(t, httpServer.URL, "/runtime/workflow/artifacts", map[string]any{
		"namespace": "default",
		"workflow":  program.WorkflowConfig{ID: "http-flow", Name: "artifact", Policy: "auto_upgrade", Program: programtest.Noop()},
	})
	if artifact.StatusCode != http.StatusOK {
		t.Fatalf("artifacts status = %d", artifact.StatusCode)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", "http-flow", "http-flow", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	policy := postPath(t, httpServer.URL, "/runtime/workflow/set-policy", map[string]any{
		"namespace": "default",
		"run_id":    runID,
		"policy":    "pinned",
	})
	if policy.StatusCode != http.StatusOK {
		t.Fatalf("set-policy status = %d", policy.StatusCode)
	}
	var updated execution.Run
	if err := json.NewDecoder(policy.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	if updated.Policy != "pinned" || updated.PolicySource != "run_override" {
		t.Fatalf("policy response = %q/%q", updated.Policy, updated.PolicySource)
	}

	bad := postPath(t, httpServer.URL, "/runtime/workflow/set-policy", map[string]any{
		"namespace": "default",
		"run_id":    runID,
		"policy":    "manual",
	})
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown policy status = %d, want 400", bad.StatusCode)
	}
}
