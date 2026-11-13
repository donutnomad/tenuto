package engine

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 场景：reset 一个历史代（base != 实际 current），且实际 current 仍在 running
// ============================================
// 测试策略（P1 核心正确性，杜绝孤儿 running 代）：
// - Happy Path: workflow 有 gen1（已终态）+ gen2（running, current）。reset gen1（base, 历史代）。
//   验证 ① gen2 被终止（不再是孤儿 running 代）② current 切到 gen3 ③ gen3 running。
//
// 旧实现 bug：ResetWorkflow 只 terminate base（req.RunID）。reset 历史代时 base 已终态、
// 不会终止任何东西；CAS 又把 current 从 gen2(running) 切到 gen3 —— gen2 留在 running 但不再是
// current，成为孤儿/僵尸代。对标 Temporal workflow_resetter：reset 时终止「实际 current」（若 running）。
// ============================================

// TestResetWorkflow_HistoricalGen_TerminatesRunningCurrent 钉死「reset 历史代时旧 current 被终止」。
func TestResetWorkflow_HistoricalGen_TerminatesRunningCurrent(t *testing.T) {
	// Happy Path: gen1 终态 + gen2 running(current)，reset gen1 → gen2 被终止、gen3 成为 running current。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "wf-hist-gen", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	// gen1：StartWorkflow 创建 running run，并写一个 WFT close 边界（reset 截断点）。
	gen1, err := startedRunID(server.StartWorkflow(ctx, "", "wf-hist-gen", "wf-hist-gen", "", map[string]any{"k": "v"}))
	if err != nil {
		t.Fatal(err)
	}
	completeOpenWorkflowTask(t, mgr, gen1)

	// reset gen1（此刻 gen1 是 running 的 current）→ 产生 gen2：
	//   ResetWorkflow 内部先 terminate gen1（gen1 终态），再 fork gen2（running）并切为 current。
	// 此后状态恰为：gen1 终态 + gen2 running(current) —— 即「历史代 + running current」的前提。
	gen2, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: gen1, Reason: "to-gen2", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatalf("reset gen1 -> gen2: %v", err)
	}

	// 前置确认：gen1 已终态、gen2 running 且为 current。
	assertRunStatus(t, server, mgr, gen1, history.StatusTerminated)
	assertRunStatus(t, server, mgr, gen2, history.StatusRunning)
	if cur, _ := mgr.GetCurrentRunID("", "wf-hist-gen"); cur != gen2 {
		t.Fatalf("setup: expected current=gen2(%s), got %s", gen2, cur)
	}

	// ====== 核心：reset gen1（历史代，base != current）。gen2 仍在 running ======
	gen3, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: gen1, Reason: "reset-historical", ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatalf("reset historical gen1 -> gen3: %v", err)
	}

	// 断言 1：gen3 是一个全新的、不同于 gen1/gen2 的 run。
	if gen3 == "" || gen3 == gen1 || gen3 == gen2 {
		t.Fatalf("expected distinct gen3, got %q (gen1=%q gen2=%q)", gen3, gen1, gen2)
	}

	// 断言 2（P1 核心）：gen2 被终止 —— 不再是孤儿 running 代。
	assertRunStatus(t, server, mgr, gen2, history.StatusTerminated)

	// 断言 3：current 切到 gen3。
	cur, err := mgr.GetCurrentRunID("", "wf-hist-gen")
	if err != nil {
		t.Fatalf("GetCurrentRunID: %v", err)
	}
	if cur != gen3 {
		t.Fatalf("expected current切到 gen3(%s), got %s", gen3, cur)
	}

	// 断言 4：gen3 running。
	assertRunStatus(t, server, mgr, gen3, history.StatusRunning)

	// 断言 5：除 gen3 外，没有「running 但不是 current」的孤儿代。
	runningRuns, err := runsByStatus(mgr, history.StatusRunning)
	if err != nil {
		t.Fatalf("ListByStatus: %v", err)
	}
	for _, r := range runningRuns {
		if r.WorkflowID != "wf-hist-gen" {
			continue
		}
		if r.ID != gen3 {
			t.Fatalf("发现孤儿 running 代: run %s 仍 running 但 current=%s", r.ID, gen3)
		}
		if !r.IsCurrent {
			t.Fatalf("running run %s 不是 current（孤儿）", r.ID)
		}
	}
}

// assertRunStatus 断言 run 经 history reconcile 后的状态等于 want。
func assertRunStatus(t *testing.T, server *Server, mgr execution.ExecutionManager, runID string, want history.Status) {
	t.Helper()
	got, err := server.GetRunStatus(runID)
	if err != nil {
		t.Fatalf("GetRunStatus(%s): %v", runID, err)
	}
	if got != want {
		t.Fatalf("run %s status = %s, want %s", runID, got, want)
	}
}
