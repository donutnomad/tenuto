package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// newBranchTestRun 建一个带 branch token 的 run，并向 (treeID, branchID) 追加 n 个事件。
func newBranchTestRun(t *testing.T, store *Store, runID, treeID, branchID string, n int) {
	t.Helper()

	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{
		Version: 1, TreeID: treeID, BranchID: branchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID: runID, Namespace: "default", WorkflowID: "wf-1",
		WorkflowConfigVersion: "v1", Status: history.StatusRunning,
		BranchToken: token,
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		ev := history.NewMarkerRecordedEvent("m", string(rune('a'+i)), nil)
		if _, err := store.Executions.Append(runID, treeID, branchID, ev); err != nil {
			t.Fatal(err)
		}
	}
}

// 缺陷 A：删除 reset 出来的叶子 branch 时，base run 的历史必须完整保留。
//
// base branch 不是任何其他 branch 的祖先条目（它是自己活着的 leaf），
// 修复前不会进入 usedBranches，被当成无人引用而全段删除。
// 对照 Temporal history_manager.go:171 `usedBranches[branchInfo.BranchId] = LastEventID`。
func TestDeleteBranchKeepsLiveAncestorHistory(t *testing.T) {
	store := newTestStore(t)

	newBranchTestRun(t, store, "run-b0", "tree-1", "root", 10)

	before, err := store.Executions.GetEvents("run-b0")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 10 {
		t.Fatalf("setup: expected 10 base events, got %d", len(before))
	}

	resp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-b0",
		ForkNodeID:  5,
		NewRunID:    "run-b1",
		NewBranchID: "branch-b1",
		Info:        "reset",
	})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := execution.DeserializeBranchToken(resp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Executions.DeleteBranch(forked.TreeID, forked.BranchID); err != nil {
		t.Fatalf("delete forked branch: %v", err)
	}

	after, err := store.Executions.GetEvents("run-b0")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("DATA LOSS: base run events %d -> %d after deleting the reset leaf",
			len(before), len(after))
	}
}

// 缺陷 A'：部分引用场景。
//
// 祖先 branch 被另一个存活 branch 引用到 maxEndNode，删除 target 时
// 只应删除 maxEndNode 之后的部分，[BeginNodeID, maxEndNode) 必须保留。
// 对照 Temporal history_manager.go:184-193 的 findDeleteRanges。
func TestDeleteBranchPartialAncestorReference(t *testing.T) {
	store := newTestStore(t)

	// base: root 上 10 个事件。注意 historyTreeRow 只由 ForkBranch 写入，
	// base 的 root branch 本身没有 tree row。
	newBranchTestRun(t, store, "run-p0", "tree-p", "root", 10)

	// mid：从 root 的 node 8 fork。它有 tree row，且在 root 上追加自己的事件。
	// 删除 target 时，mid 会被 target 以「祖先」形式部分引用。
	midResp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-p0", ForkNodeID: 8,
		NewRunID: "run-mid", NewBranchID: "branch-mid", Info: "mid",
	})
	if err != nil {
		t.Fatal(err)
	}
	mid, err := execution.DeserializeBranchToken(midResp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}
	// ForkBranch 只写 branch token，不建 Run 行；run-mid 需显式创建。
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{
			ID: "run-mid", Namespace: "default", WorkflowID: "wf-1",
			WorkflowConfigVersion: "v1", Status: history.StatusRunning,
			BranchToken: midResp.BranchToken,
		})
	}); err != nil {
		t.Fatal(err)
	}
	// 在 mid 自己的 branch 段上追加事件（node 9..13）。
	for i := range 5 {
		ev := history.NewMarkerRecordedEvent("mid", string(rune('A'+i)), nil)
		if _, err := store.Executions.Append("run-mid", mid.TreeID, mid.BranchID, ev); err != nil {
			t.Fatal(err)
		}
	}
	midBefore, err := store.Executions.GetEvents("run-mid")
	if err != nil {
		t.Fatal(err)
	}

	// target：从 mid 的中间 fork，于是 target 的 ancestors 里含
	// {branch-mid, [9, forkNode)} —— 对 mid 的**部分**引用。
	tgtResp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-mid", ForkNodeID: 11,
		NewRunID: "run-tgt", NewBranchID: "branch-tgt", Info: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	tgt, err := execution.DeserializeBranchToken(tgtResp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}

	// 删除 target。mid 仍存活，其历史必须一条不少；root 的段也必须完整。
	if err := store.Executions.DeleteBranch(tgt.TreeID, tgt.BranchID); err != nil {
		t.Fatalf("delete target branch: %v", err)
	}

	midAfter, err := store.Executions.GetEvents("run-mid")
	if err != nil {
		t.Fatal(err)
	}
	if len(midAfter) != len(midBefore) {
		t.Fatalf("PARTIAL-REFERENCE DATA LOSS: live ancestor branch-mid events %d -> %d after deleting target",
			len(midBefore), len(midAfter))
	}

	base, err := store.Executions.GetEvents("run-p0")
	if err != nil {
		t.Fatal(err)
	}
	if len(base) != 10 {
		t.Fatalf("PARTIAL-REFERENCE DATA LOSS: base run events 10 -> %d after deleting target", len(base))
	}
}

// 缺陷 B：DeleteBranch 必须幂等 —— 重复删除同一 branch 不得报错。
// GC 依赖「删除失败可重试」，非幂等会让重试永久卡死。
func TestDeleteBranchIsIdempotent(t *testing.T) {
	store := newTestStore(t)

	newBranchTestRun(t, store, "run-i0", "tree-i", "root", 5)

	resp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-i0", ForkNodeID: 3,
		NewRunID: "run-i1", NewBranchID: "branch-i1", Info: "reset",
	})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := execution.DeserializeBranchToken(resp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Executions.DeleteBranch(forked.TreeID, forked.BranchID); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := store.Executions.DeleteBranch(forked.TreeID, forked.BranchID); err != nil {
		t.Fatalf("NOT IDEMPOTENT: second delete of the same branch failed: %v", err)
	}

	// 完全不存在的 branch 同样应视为已删除。
	if err := store.Executions.DeleteBranch("tree-i", "never-existed"); err != nil {
		t.Fatalf("NOT IDEMPOTENT: deleting an absent branch failed: %v", err)
	}
}
