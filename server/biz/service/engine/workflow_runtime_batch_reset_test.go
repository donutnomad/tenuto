package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Batch Reset 透传 first/last reset type 测试（真 reset 路径）
// ============================================
// 测试策略:
// - Happy Path: batch reset 带 ResetType=last，2 个 run 全部成功，每个新 run 含 reset marker（证明真 reset）
// - Happy Path: first vs last batch reset → 各 run 新 run 的截断点不同（事件数不同）
// ============================================

// batchResetRun：注册独立 workflowID 并启动 run，追加 first/last 两个 WFT 边界（中间隔一个非边界事件），返回 runID。
// 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
func batchResetRun(t *testing.T, ctx context.Context, svc *Server, mgr execution.ExecutionManager, wfID string) string {
	t.Helper()
	if _, err := svc.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: wfID, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(svc.StartWorkflow(ctx, "", wfID, wfID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, runID)
	scheduled, err := mgr.Append(runID, runID, runID, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(runID, runID, runID, history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: scheduled}}); err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, runID)
	return runID
}

func resetMarkerCount(t *testing.T, mgr execution.ExecutionManager, runID string) int {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range events {
		if e.EventType() == history.EventWorkflowTaskFailed && e.WorkflowTaskFailed != nil && e.WorkflowTaskFailed.Cause == "reset_workflow" {
			n++
		}
	}
	return n
}

func TestStartBatchOperation_ResetWithType_RealReset(t *testing.T) {
	// Happy Path: batch reset 带 ResetType=last，2 个 run 成功且各 source 的新 run 含 reset marker
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	runA := batchResetRun(t, ctx, server, mgr, "wf-batch-reset-a")
	runB := batchResetRun(t, ctx, server, mgr, "wf-batch-reset-b")

	op, err := driveBatch(server, batches.StartRequest{
		Namespace: "default",
		Type:      batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{runA, runB}, IncludeTerminal: true},
		Reason:    "batch reset test",
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.SucceededCount != 2 {
		t.Fatalf("expected succeeded_count=2, got %d (errors=%+v)", op.SucceededCount, op.Errors)
	}
	if op.Status != batch.StatusEnums.Completed {
		t.Fatalf("expected status=completed, got %s", op.Status)
	}
	for _, id := range []string{runA, runB} {
		run, err := mgr.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if run.ResetRunID == "" {
			t.Fatalf("run %s: expected ResetRunID set", id)
		}
		if got := resetMarkerCount(t, mgr, run.ResetRunID); got != 1 {
			t.Fatalf("new run of %s: expected 1 reset_workflow marker (real reset), got %d", id, got)
		}
	}
}

func TestStartBatchOperation_FirstVsLast_DifferentTruncation(t *testing.T) {
	// Happy Path: first 与 last batch reset 产生的新 run 截断点不同 → 事件数不同
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	runFirst := batchResetRun(t, ctx, server, mgr, "wf-batch-fl-first")
	runLast := batchResetRun(t, ctx, server, mgr, "wf-batch-fl-last")

	if _, err := driveBatch(server, batches.StartRequest{
		Namespace: "default", Type: batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{runFirst}, IncludeTerminal: true},
		ResetType: execution.ResetTypeEnums.FirstWorkflowTask,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := driveBatch(server, batches.StartRequest{
		Namespace: "default", Type: batch.TypeEnums.Reset,
		Filter:    execution.RunListFilter{RunIDs: []string{runLast}, IncludeTerminal: true},
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
	}); err != nil {
		t.Fatal(err)
	}

	rf, _ := mgr.Get(runFirst)
	rl, _ := mgr.Get(runLast)
	firstEvents, err := mgr.GetEvents(rf.ResetRunID)
	if err != nil {
		t.Fatal(err)
	}
	lastEvents, err := mgr.GetEvents(rl.ResetRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstEvents) == len(lastEvents) {
		t.Fatalf("expected first(%d) and last(%d) batch reset to truncate differently", len(firstEvents), len(lastEvents))
	}
}
