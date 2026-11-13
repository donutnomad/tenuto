package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// RunListItem.IsReset 标记测试
// ============================================
// 测试策略:
// - Happy Path: 真 reset 出的新 run，ListRuns 结果里 IsReset=true
// - Edge Case: 普通 run（未经 reset）IsReset=false
// ============================================

func TestListRuns_MarksResetRun(t *testing.T) {
	// Happy Path: 带 ResetType 的真 reset 新 run IsReset=true；源普通 run IsReset=false
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-isreset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, err := startedRunID(server.StartWorkflow(ctx, "", "wf-isreset", "wf-isreset", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, base)
	newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}

	result, err := server.View().ListRuns(execution.RunListFilter{
		WorkflowID:      "wf-isreset",
		RunIDs:          []string{base, newRunID},
		IncludeTerminal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]execution.RunListItem{}
	for _, it := range result.Items {
		byID[it.RunID] = it
	}
	newItem, ok := byID[newRunID]
	if !ok {
		t.Fatalf("new run %s not found in ListRuns result", newRunID)
	}
	if !newItem.IsReset {
		t.Fatalf("expected new run %s IsReset=true (reset-produced), got false", newRunID)
	}
	baseItem, ok := byID[base]
	if !ok {
		t.Fatalf("base run %s not found in ListRuns result", base)
	}
	if baseItem.IsReset {
		t.Fatalf("expected base run %s IsReset=false, got true", base)
	}
}

func TestListRuns_ContinueAsNewRunNotMarkedReset(t *testing.T) {
	// Edge Case: continue-as-new 产出的新 run 有 ContinuedRunID，但不是 reset，IsReset 必须为 false。
	// 钉死判据「有 ContinuedRunID ≠ is_reset」，防止有人把判据改成 ContinuedRunID != "" 而无回归报警。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-can-notreset", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-can-base",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-can-base",
		Status:                history.StatusRunning,
		IsCurrent:             true, // 旧 run 是 current（CAN 切代 expectedOld 需其为 current）
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []history.Command{{
		Type:       history.CmdContinueAsNew,
		WorkflowID: ref.WorkflowID,
		Input:      map[string]any{"page": int64(2)},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	newRunID := ""
	for _, event := range history {
		if event.WorkflowContinuedAsNew != nil {
			newRunID = event.WorkflowContinuedAsNew.NextRunID
		}
	}
	if newRunID == "" {
		t.Fatalf("expected continue-as-new to produce a new run, history=%+v", history)
	}
	newRun, err := mgr.Get(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if newRun.ContinuedRunID != run.ID {
		t.Fatalf("expected continue-as-new run %s ContinuedRunID=%s (precondition), got %q", newRunID, run.ID, newRun.ContinuedRunID)
	}

	result, err := server.View().ListRuns(execution.RunListFilter{
		WorkflowID:      ref.WorkflowID,
		RunIDs:          []string{run.ID, newRunID},
		IncludeTerminal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]execution.RunListItem{}
	for _, it := range result.Items {
		byID[it.RunID] = it
	}
	canItem, ok := byID[newRunID]
	if !ok {
		t.Fatalf("continue-as-new run %s not found in ListRuns result", newRunID)
	}
	if canItem.IsReset {
		t.Fatalf("expected continue-as-new run %s IsReset=false (CAN is not reset), got true", newRunID)
	}
}
