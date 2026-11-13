package runtimehttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 三个启动端点都按 workflow_type 启动；workflow_id 可选，应答总是带回这次执行的 workflow_id。
func TestStartBodies(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	defer server.Stop()
	// 应答里的 workflow_id 就是引擎给 Run 采用的 Workflow ID。
	requireWorkflowID := func(runID, workflowID string) {
		t.Helper()
		run, err := mgr.Get(runID)
		if err != nil || run.WorkflowID != workflowID {
			t.Fatalf("run %s workflow id = %+v err=%v, want %q", runID, run, err, workflowID)
		}
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns", &program.WorkflowConfig{ID: "pay", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewWorkflowRuntimeHTTPHandler(server))
	defer httpServer.Close()

	post := func(path, body string, out any) int {
		t.Helper()
		resp, err := http.Post(httpServer.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode
	}

	var generated, second, explicit runtimeapi.WorkflowRuntimeStartResponse
	if code := post("/runtime/workflow/start", `{"namespace":"ns","workflow_type":"pay"}`, &generated); code != http.StatusOK {
		t.Fatalf("start without workflow_id: status %d", code)
	}
	if code := post("/runtime/workflow/start", `{"namespace":"ns","workflow_type":"pay"}`, &second); code != http.StatusOK {
		t.Fatalf("second start of the same type: status %d", code)
	}
	if generated.RunID == "" || generated.WorkflowID == "" || generated.WorkflowID == second.WorkflowID {
		t.Fatalf("generated ids: %+v %+v", generated, second)
	}
	requireWorkflowID(generated.RunID, generated.WorkflowID)
	requireWorkflowID(second.RunID, second.WorkflowID)
	if code := post("/runtime/workflow/start", `{"namespace":"ns","workflow_type":"pay","workflow_id":"order-1"}`, &explicit); code != http.StatusOK || explicit.WorkflowID != "order-1" {
		t.Fatalf("explicit start: status %d %+v", code, explicit)
	}
	if code := post("/runtime/workflow/start", `{"namespace":"ns","workflow_type":"pay","workflow_id":"order-1"}`, &explicit); code == http.StatusOK {
		t.Fatal("start on a running workflow_id succeeded")
	}
	if code := post("/runtime/workflow/start", `{"namespace":"ns","workflow_id":"order-2"}`, &explicit); code == http.StatusOK {
		t.Fatal("start without workflow_type succeeded")
	}

	var signaled runtimeapi.WorkflowRuntimeSignalWithStartResponse
	if code := post("/runtime/workflow/signal-with-start", `{"namespace":"ns","workflow_type":"pay","workflow_id":"order-1","signal_name":"go"}`, &signaled); code != http.StatusOK {
		t.Fatalf("signal-with-start: status %d", code)
	}
	if signaled.RunID != explicit.RunID || signaled.WorkflowID != "order-1" {
		t.Fatalf("signal-with-start reuse: %+v", signaled)
	}
	var updated runtimeapi.WorkflowRuntimeUpdateWithStartResponse
	if code := post("/runtime/workflow/update-with-start", `{"namespace":"ns","workflow_type":"pay","update_name":"u","update_args":[]}`, &updated); code != http.StatusOK {
		t.Fatalf("update-with-start without workflow_id: status %d", code)
	}
	if updated.RunID == "" || updated.WorkflowID == "" || updated.WorkflowID == "order-1" {
		t.Fatalf("update-with-start generated ids: %+v", updated)
	}
	requireWorkflowID(updated.RunID, updated.WorkflowID)
	var signaledNew runtimeapi.WorkflowRuntimeSignalWithStartResponse
	if code := post("/runtime/workflow/signal-with-start", `{"namespace":"ns","workflow_type":"pay","signal_name":"go"}`, &signaledNew); code != http.StatusOK {
		t.Fatalf("signal-with-start without workflow_id: status %d", code)
	}
	requireWorkflowID(signaledNew.RunID, signaledNew.WorkflowID)
}
