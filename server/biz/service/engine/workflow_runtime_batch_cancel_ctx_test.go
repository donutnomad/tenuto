package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
)

// ============================================
// Batch 驱动停止测试
// ============================================
// 测试策略:
// - Edge Case: 驱动的 ctx 已取消时不处理任何 Run，批量操作保持 pending，之后的驱动照常处理完
// ============================================

// driveBatch 受理批量操作，跑一轮驱动处理到结束，返回最新记录。
func driveBatch(server *Server, req batches.StartRequest) (*batch.Operation, error) {
	op, err := server.Batches().Start(req)
	if err != nil {
		return op, err
	}
	server.Batches().ProcessActive(context.Background())
	return server.Batches().Get(op.Namespace, op.ID)
}

func TestBatchDriverStoppedBeforeProcessingLeavesOperationPending(t *testing.T) {
	// Edge Case: 停止中的驱动不处理 Run；下一任驱动从头处理完
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	runIDs := make([]string, 3)
	for i := range 3 {
		wfID := fmt.Sprintf("wf-ctx-cancel-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(context.Background(), "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		runIDs[i] = id
	}

	op, err := server.Batches().Start(batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Cancel,
		Filter:    execution.RunListFilter{RunIDs: runIDs, Statuses: []history.Status{history.StatusRunning}},
		Reason:    "ctx cancel test",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.Batches().ProcessActive(ctx)
	got, err := server.Batches().Get(op.Namespace, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != batch.StatusEnums.Pending || got.Processed() != 0 {
		t.Fatalf("after a stopped driver: status=%s processed=%d, want pending and 0", got.Status, got.Processed())
	}

	server.Batches().ProcessActive(context.Background())
	got, err = server.Batches().Get(op.Namespace, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != batch.StatusEnums.Completed || got.SucceededCount != 3 || got.TotalCount != 3 {
		t.Fatalf("after the next driver: %+v, want completed with 3 cancellations requested", got)
	}
}
