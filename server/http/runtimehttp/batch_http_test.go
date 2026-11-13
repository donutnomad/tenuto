package runtimehttp

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 批量操作端点测试
// ============================================
// 测试策略:
// - Happy Path: start 应答 202 与 pending 记录；get、list 读到同一条；cancel 应答 200 与 cancelled 记录，重复取消不变
// - Error Case: 取消已结束的批量操作为 409
// ============================================

func decodeBatchOperation(t *testing.T, body []byte) batch.Operation {
	t.Helper()
	var op batch.Operation
	if err := json.Unmarshal(body, &op); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return op
}

func TestRuntimeHTTPBatchLifecycle(t *testing.T) {
	queue := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: queue, ActivityRouter: queue.activities()})
	t.Cleanup(server.Stop)
	handler := NewRuntimeHTTPHandler(server)

	started := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeStartBatchPath,
		`{"req":{"type":"terminate","filter":{"workflow_id":"wf-none"},"reason":"cleanup"}}`)
	if started.Code != http.StatusAccepted {
		t.Fatalf("start-batch = %d %q, want 202", started.Code, started.Body.String())
	}
	op := decodeBatchOperation(t, started.Body.Bytes())
	if op.ID == "" || op.Status != batch.StatusEnums.Pending || op.Namespace != "default" || op.Reason != "cleanup" {
		t.Fatalf("start-batch record = %+v, want a pending record", op)
	}

	got := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeGetBatchPath, `{"operation_id":"`+op.ID+`"}`)
	if got.Code != http.StatusOK || decodeBatchOperation(t, got.Body.Bytes()).ID != op.ID {
		t.Fatalf("get-batch = %d %q", got.Code, got.Body.String())
	}
	listed := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeListBatchesPath, `{"namespace":"default"}`)
	var list batches.ListResult
	if listed.Code != http.StatusOK || json.Unmarshal(listed.Body.Bytes(), &list) != nil || list.Total != 1 || list.Items[0].ID != op.ID {
		t.Fatalf("list-batches = %d %q", listed.Code, listed.Body.String())
	}

	for range 2 {
		cancelled := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeCancelBatchPath, `{"operation_id":"`+op.ID+`"}`)
		if cancelled.Code != http.StatusOK || decodeBatchOperation(t, cancelled.Body.Bytes()).Status != batch.StatusEnums.Cancelled {
			t.Fatalf("cancel-batch = %d %q, want 200 with a cancelled record", cancelled.Code, cancelled.Body.String())
		}
	}

	finished, err := server.Batches().Start(batches.StartRequest{Type: batch.TypeEnums.Cancel})
	if err != nil {
		t.Fatal(err)
	}
	server.Batches().ProcessActive(context.Background())
	conflict := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeCancelBatchPath, `{"operation_id":"`+finished.ID+`"}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("cancel-batch on a completed batch = %d %q, want 409", conflict.Code, conflict.Body.String())
	}
}
