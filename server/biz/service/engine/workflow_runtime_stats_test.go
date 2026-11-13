package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// GetRunStats 聚合计数测试
// ============================================
// Happy Path: 多种状态的 Run 正确聚合
// Happy Path: 空 namespace 返回零计数
// Happy Path: 通过 HTTP 端到端验证
// Edge Case: 不同 namespace 的 Run 互不影响
// ============================================

func TestRunStatsAggregatesCountsByStatus(t *testing.T) {
	// Happy Path: 创建多种状态的 Run，验证各状态计数正确
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	// 统计读 Run 行状态，与 ListRuns 的状态过滤同源。
	testRuns := []struct {
		id     string
		status history.Status
	}{
		{"run-running-1", history.StatusRunning},
		{"run-running-2", history.StatusRunning},
		{"run-completed", history.StatusCompleted},
		{"run-failed", history.StatusFailed},
		{"run-cancelled", history.StatusCancelled},
		{"run-terminated", history.StatusTerminated},
	}

	for _, tr := range testRuns {
		run := &execution.Run{
			ID:                    tr.id,
			Namespace:             "default",
			WorkflowID:            "wf-stats",
			WorkflowType:          "wf-stats",
			WorkflowConfigVersion: "v1",
			Status:                tr.status,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := server.View().GetRunStats("default")
	if err != nil {
		t.Fatal(err)
	}

	if stats.Total != 6 {
		t.Fatalf("expected total=6, got %d", stats.Total)
	}
	if stats.Running != 2 {
		t.Fatalf("expected running=2, got %d", stats.Running)
	}
	if stats.Completed != 1 {
		t.Fatalf("expected completed=1, got %d", stats.Completed)
	}
	if stats.Failed != 1 {
		t.Fatalf("expected failed=1, got %d", stats.Failed)
	}
	if stats.Cancelled != 1 {
		t.Fatalf("expected cancelled=1, got %d", stats.Cancelled)
	}
	if stats.Terminated != 1 {
		t.Fatalf("expected terminated=1, got %d", stats.Terminated)
	}
}

func TestRunStatsFiltersNamespace(t *testing.T) {
	// Edge Case: 不同 namespace 的 Run 互不影响
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	for _, ns := range []struct {
		id        string
		namespace string
	}{
		{"run-ns1-1", "ns1"},
		{"run-ns1-2", "ns1"},
		{"run-ns2-1", "ns2"},
	} {
		run := &execution.Run{
			ID:                    ns.id,
			Namespace:             ns.namespace,
			WorkflowID:            "wf-ns",
			WorkflowType:          "wf-ns",
			WorkflowConfigVersion: "v1",
			Status:                history.StatusRunning,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := server.View().GetRunStats("ns1")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 || stats.Running != 2 {
		t.Fatalf("expected ns1: total=2 running=2, got total=%d running=%d", stats.Total, stats.Running)
	}

	stats, err = server.View().GetRunStats("ns2")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 1 || stats.Running != 1 {
		t.Fatalf("expected ns2: total=1 running=1, got total=%d running=%d", stats.Total, stats.Running)
	}
}

func TestRunStatsEmptyNamespace(t *testing.T) {
	// Happy Path: 空 namespace 默认归入 "default"，无 Run 时返回零计数
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	stats, err := server.View().GetRunStats("")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 0 {
		t.Fatalf("expected empty stats, got total=%d", stats.Total)
	}
}

func TestRunStatsServiceRoundTrip(t *testing.T) {
	// 验证运行时服务聚合 Run 状态。
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	for id, status := range map[string]history.Status{"run-http-1": history.StatusRunning, "run-http-2": history.StatusCompleted} {
		run := &execution.Run{
			ID:                    id,
			Namespace:             "default",
			WorkflowID:            "wf-http-stats",
			WorkflowType:          "wf-http-stats",
			WorkflowConfigVersion: "v1",
			Status:                status,
			CreatedAt:             time.Now(),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := server.View().GetRunStats("default")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total=2, got %d", stats.Total)
	}
	if stats.Running != 1 {
		t.Fatalf("expected running=1, got %d", stats.Running)
	}
	if stats.Completed != 1 {
		t.Fatalf("expected completed=1, got %d", stats.Completed)
	}
}
