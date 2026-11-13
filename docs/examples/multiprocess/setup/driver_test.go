package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// start 按 Workflow Type 启动、每个订单一个 Workflow ID，后续按 Run 读取时带回服务端返回的 Workflow ID。
func TestStartSendsWorkflowTypeAndPerOrderID(t *testing.T) {
	bodies := map[string]map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode %s: %v", r.URL.Path, err)
		}
		bodies[r.URL.Path] = body
		switch r.URL.Path {
		case "/runtime/workflow/start":
			_, _ = w.Write([]byte(`{"run_id":"run-1","workflow_id":"order-flow-demo-1"}`))
		default:
			_, _ = w.Write([]byte(`{"run":{"id":"run-1","status":"completed"}}`))
		}
	}))
	defer server.Close()

	client := &api{base: server.URL, namespace: "ns", workflowType: "order-flow", idPrefix: "order-flow",
		runs: map[string]string{}, http: server.Client()}
	runID, err := client.start(context.Background(), "demo-1")
	if err != nil {
		t.Fatal(err)
	}
	start := bodies["/runtime/workflow/start"]
	if start["workflow_type"] != "order-flow" || start["workflow_id"] != "order-flow-demo-1" {
		t.Fatalf("start body = %v", start)
	}
	if _, err := client.getRun(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if got := bodies["/runtime/workflow/get-run"]["workflow_id"]; got != "order-flow-demo-1" {
		t.Fatalf("get-run workflow_id = %v, want the started Workflow ID", got)
	}
}
