package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// Reset 链式语义测试（Temporal chained reset）
// ============================================
// 测试策略:
// - Happy Path: 对同一条链连续 reset run1→run2→run3，每次返回新 run 且 ContinuedRunID 指向前一个
// - Happy Path: 同一 base run reset 两次返回两个不同的新 run（去掉早返回幂等），base.ResetRunID 指向最新
// ============================================

// resetChainStep 给 run 追加一个 WFT close 边界后带 ResetType=last 走真 reset，返回新 runID。
func resetChainStep(t *testing.T, ctx context.Context, svc *Server, mgr execution.ExecutionManager, runID string) string {
	t.Helper()
	completeOpenWorkflowTask(t, mgr, runID)
	newRunID, err := svc.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: runID, Reason: "chain", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatalf("reset of %s: %v", runID, err)
	}
	return newRunID
}

func TestResetWorkflow_ChainedReset(t *testing.T) {
	// Happy Path: run1 reset→run2 reset→run3，三个 run 互不相同，ContinuedRunID 链正确
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-chain", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	run1, err := startedRunID(server.StartWorkflow(ctx, "", "wf-chain", "wf-chain", "", map[string]any{"k": "v"}))
	if err != nil {
		t.Fatal(err)
	}

	run2 := resetChainStep(t, ctx, server, mgr, run1)
	run3 := resetChainStep(t, ctx, server, mgr, run2)

	if run1 == run2 || run2 == run3 || run1 == run3 {
		t.Fatalf("expected 3 distinct runs, got run1=%s run2=%s run3=%s", run1, run2, run3)
	}
	r2, err := mgr.Get(run2)
	if err != nil {
		t.Fatal(err)
	}
	if r2.ContinuedRunID != run1 {
		t.Fatalf("run2.ContinuedRunID expected %s, got %s", run1, r2.ContinuedRunID)
	}
	r3, err := mgr.Get(run3)
	if err != nil {
		t.Fatal(err)
	}
	if r3.ContinuedRunID != run2 {
		t.Fatalf("run3.ContinuedRunID expected %s, got %s", run2, r3.ContinuedRunID)
	}
}

func TestResetWorkflow_NoIdempotentEarlyReturn(t *testing.T) {
	// Happy Path: 同一 base run reset 两次返回两个不同的新 run，base.ResetRunID 指向最新
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-rereset", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	base, err := startedRunID(server.StartWorkflow(ctx, "", "wf-rereset", "wf-rereset", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, base)

	first, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, Reason: "first", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}
	second, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, Reason: "second", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatalf("second reset of same base should not error, got %v", err)
	}
	if first == second {
		t.Fatalf("expected second reset to produce a new run, both returned %s", first)
	}
	baseRun, err := mgr.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	if baseRun.ResetRunID != second {
		t.Fatalf("base.ResetRunID expected latest reset %s, got %s", second, baseRun.ResetRunID)
	}
}
