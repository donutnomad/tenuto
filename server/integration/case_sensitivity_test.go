package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// MySQL 表的排序规则是 utf8mb4_bin：只差大小写的 Workflow ID 与 Namespace 是不同的 Workflow 与 Namespace，与内存后端一致。
func TestWorkflowIDAndNamespaceAreCaseSensitiveOnMySQL(t *testing.T) {
	server, url := newRuntimeHTTPServer(t)
	registerReturnWorkflow(t, server, "tenant-a", "case-flow")

	run, err := server.StartWorkflow(context.Background(), "tenant-a", "case-flow", "order-ab12", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if code, text := postRuntime(t, url, "/runtime/workflow/start", map[string]any{
		"namespace": "tenant-a", "workflow_type": "case-flow", "workflow_id": "order-AB12",
	}); code != http.StatusOK {
		t.Fatalf("start workflow_id order-AB12 while order-ab12 is running = %d %s", code, text)
	}
	code, text := postRuntime(t, url, "/runtime/workflow/signal-with-start", map[string]any{
		"namespace": "tenant-a", "workflow_type": "case-flow", "workflow_id": "order-AB12", "signal_name": "pay",
	})
	if code != http.StatusOK || strings.Contains(text, run.ID) {
		t.Fatalf("signal-with-start order-AB12 = %d %s, want a run of order-AB12", code, text)
	}
	events, err := server.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowSignaled != nil {
			t.Fatalf("signal-with-start addressed to order-AB12 wrote WorkflowSignaled into run %s of order-ab12", run.ID)
		}
	}
	code, text = postRuntime(t, url, "/runtime/workflow/list", map[string]any{"filter": map[string]any{"namespace": "TENANT-A"}})
	if code != http.StatusOK || strings.Contains(text, run.ID) {
		t.Fatalf("list in namespace TENANT-A = %d and returned run %s of namespace tenant-a", code, run.ID)
	}
}
