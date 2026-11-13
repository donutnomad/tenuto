package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// ListRuns 分页测试
// ============================================
// 测试策略:
// - Happy Path: offset=0, limit=2 返回前 2 条，total=5
// - Happy Path: offset=2, limit=2 返回中间 2 条
// - Happy Path: limit=0 使用默认值 50
// - Happy Path: OrderDesc=true 按 CreatedAt 降序排列
// - Edge Case:  offset 超出范围返回空切片
// ============================================

func setupPaginationRuns(t *testing.T, count int) (*Server, []*execution.Run) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	created := make([]*execution.Run, 0, count)
	for i := range count {
		run := &execution.Run{
			ID:                    fmt.Sprintf("run-%d", i),
			WorkflowID:            "wf-page",
			WorkflowType:          "wf-page",
			WorkflowConfigVersion: "v1",
			Status:                history.StatusRunning,
			CreatedAt:             baseTime.Add(time.Duration(i) * time.Minute),
		}
		if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
			t.Fatal(err)
		}
		created = append(created, run)
	}
	return server, created
}

func TestListRunsPagination_FirstPage(t *testing.T) {
	// Happy Path: offset=0, limit=2 应返回前 2 条（按 CreatedAt 升序），total=5
	runtime, created := setupPaginationRuns(t, 5)

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		WorkflowID: "wf-page",
		Statuses:   []history.Status{history.StatusRunning},
		Offset:     0,
		Limit:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0].RunID != created[0].ID || result.Items[1].RunID != created[1].ID {
		t.Fatalf("expected first two runs, got %s and %s", result.Items[0].RunID, result.Items[1].RunID)
	}
}

func TestListRunsPagination_SecondPage(t *testing.T) {
	// Happy Path: offset=2, limit=2 应返回第 3、4 条
	runtime, created := setupPaginationRuns(t, 5)

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		WorkflowID: "wf-page",
		Statuses:   []history.Status{history.StatusRunning},
		Offset:     2,
		Limit:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0].RunID != created[2].ID || result.Items[1].RunID != created[3].ID {
		t.Fatalf("expected runs 2 and 3, got %s and %s", result.Items[0].RunID, result.Items[1].RunID)
	}
}

func TestListRunsPagination_DefaultLimit(t *testing.T) {
	// Happy Path: limit=0 时默认使用 50，5 条数据全部返回
	runtime, _ := setupPaginationRuns(t, 5)

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		WorkflowID: "wf-page",
		Statuses:   []history.Status{history.StatusRunning},
		Offset:     0,
		Limit:      0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 5 {
		t.Fatalf("expected 5 items with default limit, got %d", len(result.Items))
	}
}

func TestListRunsPagination_OrderDesc(t *testing.T) {
	// Happy Path: OrderDesc=true 按 CreatedAt 降序，最新的排最前
	runtime, created := setupPaginationRuns(t, 5)

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		WorkflowID: "wf-page",
		Statuses:   []history.Status{history.StatusRunning},
		Offset:     0,
		Limit:      3,
		OrderDesc:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result.Items))
	}
	if result.Items[0].RunID != created[4].ID {
		t.Fatalf("expected newest run first (run-4), got %s", result.Items[0].RunID)
	}
	if result.Items[1].RunID != created[3].ID {
		t.Fatalf("expected second newest run (run-3), got %s", result.Items[1].RunID)
	}
	if result.Items[2].RunID != created[2].ID {
		t.Fatalf("expected third newest run (run-2), got %s", result.Items[2].RunID)
	}
}

func TestListRunsPagination_OffsetBeyondTotal(t *testing.T) {
	// Edge Case: offset 超出 total 时返回空切片，total 仍为实际过滤后的数量
	runtime, _ := setupPaginationRuns(t, 5)

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		WorkflowID: "wf-page",
		Statuses:   []history.Status{history.StatusRunning},
		Offset:     100,
		Limit:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected total=5, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected 0 items for out-of-range offset, got %d", len(result.Items))
	}
}
