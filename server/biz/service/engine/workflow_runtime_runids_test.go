package engine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// RunListFilter.RunIDs 过滤测试
// ============================================
// 测试策略:
// - Happy Path: 启动 3 个 run，filter.RunIDs 只给其中 2 个，ListRuns 只返回这 2 个，Total=2
// - Edge Case: RunIDs 非空但勾选的 run 落在默认分页窗口外，仍能返回（不分页）
// - Edge Case: RunIDs 为空切片时退回到不按 run_ids 过滤，返回全部
// ============================================

func TestListRuns_FilterByRunIDs(t *testing.T) {
	// Happy Path: filter.RunIDs 给 2 个 run，只返回这 2 个，Total 反映过滤后数量
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	runIDs := make([]string, 3)
	for i := range 3 {
		wfID := fmt.Sprintf("wf-runids-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(ctx, "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		runIDs[i] = id
	}
	want := []string{runIDs[0], runIDs[2]}

	result, err := server.View().ListRuns(execution.RunListFilter{RunIDs: want})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items filtered by run_ids, got %d", len(result.Items))
	}
	if result.Total != 2 {
		t.Fatalf("expected Total=2 (filtered count), got %d", result.Total)
	}
	for _, item := range result.Items {
		if !slices.Contains(want, item.RunID) {
			t.Fatalf("ListRuns returned run %s not in RunIDs filter %v", item.RunID, want)
		}
	}
}

func TestListRuns_FilterByRunIDs_IgnoresPaging(t *testing.T) {
	// Edge Case: 启动 60 个 run（超过默认 50 页），勾选最后一个，RunIDs 过滤不分页仍能返回
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	var lastID string
	for i := range 60 {
		wfID := fmt.Sprintf("wf-runids-page-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		id, err := startedRunID(server.StartWorkflow(ctx, "", wfID, wfID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		lastID = id
	}

	// 默认 Limit=0（底层会取 50）、OrderDesc=false → lastID 排在第 60 位，落在默认页外。
	result, err := server.View().ListRuns(execution.RunListFilter{RunIDs: []string{lastID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].RunID != lastID {
		t.Fatalf("expected the 60th run %s to be returned despite default paging, got %+v", lastID, result.Items)
	}
}

func TestListRuns_EmptyRunIDsReturnsAll(t *testing.T) {
	// Edge Case: RunIDs 为空时退回到不按 run_ids 过滤，返回全部 3 个
	ctx := context.Background()
	server := newTestServer()

	// 每个 run 用独立 workflowID：同一 workflowID 至多一个 current/running（对标 Temporal）。
	for i := range 3 {
		wfID := fmt.Sprintf("wf-runids-empty-%d", i)
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: wfID, Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		if _, err := server.StartWorkflow(ctx, "", wfID, wfID, "", nil); err != nil {
			t.Fatal(err)
		}
	}

	result, err := server.View().ListRuns(execution.RunListFilter{RunIDs: nil})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items when RunIDs empty, got %d", len(result.Items))
	}
}
