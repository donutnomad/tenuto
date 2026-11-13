package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
)

// 分批删除：事件数**跨越多个批次**时，删除必须删干净，且引用计数仍然准确
// （被引用的祖先区间一条都不能少）。
//
// 为什么这条测试非做不可：分批把原来「一条 DELETE 删完」的语义改成了「循环删到
// RowsAffected==0」，并且把事件删除**移出了事务**。任何一处写错都会表现为
// 「删了一半就停」或「把该留的祖先也删了」—— 前者是泄漏，后者是数据丢失。
//
// 批次大小 defaultDeleteEventBatch=1000，故用 2300 个事件保证至少 3 轮。
func TestDeleteBranchBatchedDeletesEverything(t *testing.T) {
	store := newTestStore(t)

	const total = 2300
	newBranchTestRun(t, store, "run-big", "tree-big", "root", total)

	before, err := store.Executions.GetEvents("run-big")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != total {
		t.Fatalf("setup: expected %d events, got %d", total, len(before))
	}

	// 直接删 root：它没有任何其它 branch 引用，应当被整段删除。
	// root 没有 tree row（tree row 只由 ForkBranch 写），先 fork 再删 fork
	// 会引入引用关系，故这里单独用一棵只有 root 的树，并显式建出 tree row。
	forkResp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-big",
		ForkNodeID:  int64(total),
		NewRunID:    "run-big-child",
		NewBranchID: "branch-child",
		Info:        "reset",
	})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := execution.DeserializeBranchToken(forkResp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}

	// 先删掉 fork 出来的叶子（它自身没有事件），root 的历史必须**完整保留** ——
	// 这是引用计数在分批实现下仍然正确的直接证据。
	if err := store.Executions.DeleteBranch(forked.TreeID, forked.BranchID); err != nil {
		t.Fatalf("delete forked leaf: %v", err)
	}
	afterLeaf, err := store.Executions.GetEvents("run-big")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterLeaf) != total {
		t.Fatalf("DATA LOSS: batched delete of the reset leaf dropped ancestor events: %d -> %d",
			total, len(afterLeaf))
	}

	// 再删 root 自己：此时无人引用，应当被**全部**删除，一条不剩。
	if err := store.Executions.DeleteBranch("tree-big", "root"); err != nil {
		t.Fatalf("delete root branch: %v", err)
	}
	remaining := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ?", "tree-big")
	if remaining != 0 {
		t.Fatalf("batched delete stopped early: %d events still present (want 0)", remaining)
	}
}

// 分批删除下的部分引用：祖先只被引用到 maxEndNode，删 target 时
// [BeginNodeID, maxEndNode) 必须留下、maxEndNode 之后必须删光。
//
// 这条盯的是「分批循环用错谓词」这类缺陷：若循环里丢掉了 node_id >= fromNodeID
// 这个下界，被保护的前半段会被一起删掉。
func TestDeleteBranchBatchedPreservesReferencedPrefix(t *testing.T) {
	store := newTestStore(t)

	const total = 1500
	newBranchTestRun(t, store, "run-base", "tree-ref", "root", total)

	// 在 forkNode=600 处 fork 出 b1：b1 把 root 的 [1,600) 列为祖先。
	const forkNode = 600
	if _, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-base",
		ForkNodeID:  forkNode,
		NewRunID:    "run-b1",
		NewBranchID: "branch-b1",
		Info:        "reset",
	}); err != nil {
		t.Fatal(err)
	}

	// 删掉 root：b1 仍引用 root 的前 forkNode+1 个节点，故只能删 node_id >= 601。
	if err := store.Executions.DeleteBranch("tree-ref", "root"); err != nil {
		t.Fatalf("delete root: %v", err)
	}

	// 被引用的前缀必须完整保留。
	kept := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ? AND branch_id = ? AND node_id <= ?",
		"tree-ref", "root", int64(forkNode))
	if kept != forkNode {
		t.Fatalf("DATA LOSS: referenced prefix [1,%d] has %d events, want %d",
			forkNode, kept, forkNode)
	}
	// 未被引用的后缀必须删光。
	dropped := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ? AND branch_id = ? AND node_id > ?",
		"tree-ref", "root", int64(forkNode))
	if dropped != 0 {
		t.Fatalf("batched delete left %d unreferenced events behind (want 0)", dropped)
	}
}
