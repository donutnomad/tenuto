package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
)

// Error Case: reset 点之前启动的子 Workflow 仍未结束 → reset 被拒绝，原 run 保持 Running，仍是 current。
func TestResetRejectedByPendingChildKeepsRunRunning(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	parentRunID, _, childRunID := startChildWorkflow(t, srv, mgr, "run-reset-pending-child")
	// 子 Workflow 启动之后再完成一个 Workflow Task，last_workflow_task 的 reset 点落在子启动之后。
	wftID := appendOpenWorkflowTaskForTest(t, mgr, parentRunID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, parentRunID, wftID), parentRunID, wftID, nil, ""); err != nil {
		t.Fatal(err)
	}

	_, err := srv.ResetWorkflow(ctx, execution.ResetWorkflowRequest{
		RunID: parentRunID, Reason: "retry", ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err == nil || !strings.Contains(err.Error(), "pending child workflows") {
		t.Fatalf("expected the reset to be rejected for a pending child, got %v", err)
	}
	if status, _ := srv.GetRunStatus(parentRunID); status != history.StatusRunning {
		t.Fatalf("a rejected reset must leave the run running, got %s", status)
	}
	if status, _ := srv.GetRunStatus(childRunID); status != history.StatusRunning {
		t.Fatalf("a rejected reset must leave the child running, got %s", status)
	}
	parentHistory, _ := mgr.GetEvents(parentRunID)
	if countEngineEvents(parentHistory, history.EventWorkflowTerminated) != 0 {
		t.Fatalf("a rejected reset must not append WorkflowTerminated: %+v", parentHistory)
	}
}

// Error Case: 没有 reset_type 也没有 reset_event_id → 校验错误，run 不变，不产生新 run。
func TestResetWithoutResetPointIsRejected(t *testing.T) {
	ctx := context.Background()
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{ID: "wf-reset-no-type", Program: programtest.Noop()})

	newRunID, err := srv.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, Reason: "retry"})
	if err == nil || !strings.Contains(err.Error(), "reset_type") {
		t.Fatalf("expected a validation error naming reset_type, got run %q err %v", newRunID, err)
	}
	if status, _ := srv.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("run must stay running, got %s", status)
	}
	if got := runCountForTest(t, mgr); got != 1 {
		t.Fatalf("no run may be created, got %d runs", got)
	}
}

// Error Case: 批量 reset 不带 reset_type → 整个批量操作被拒绝，不处理任何 run，不落操作记录。
func TestStartBatchOperation_ResetWithoutTypeIsRejected(t *testing.T) {
	srv, mgr := newServerForTest(t)
	runID := startReconcileTestRun(t, srv, &program.WorkflowConfig{ID: "wf-batch-reset-no-type", Program: programtest.Noop()})

	op, err := driveBatch(srv, batches.StartRequest{
		Type: batch.TypeEnums.Reset, Filter: execution.RunListFilter{RunIDs: []string{runID}},
	})
	if err == nil || !strings.Contains(err.Error(), "reset_type") {
		t.Fatalf("expected a validation error naming reset_type, got op %+v err %v", op, err)
	}
	if status, _ := srv.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("run must stay running, got %s", status)
	}
	if got := runCountForTest(t, mgr); got != 1 {
		t.Fatalf("no run may be created, got %d runs", got)
	}
	if list, _ := srv.Batches().List(batches.ListFilter{}); list.Total != 0 {
		t.Fatalf("a rejected batch must not be recorded, got %+v", list)
	}
}
