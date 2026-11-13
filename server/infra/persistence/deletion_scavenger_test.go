package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
)

// appendAt 追加一个指定时间戳的事件到 (treeID, branchID)。用它把事件"做旧"，
// 绕开 min_age 而不必真的等 7 天。
func appendAt(t *testing.T, store *Store, runID, treeID, branchID string, ev history.Event, at time.Time) {
	t.Helper()
	ev.Timestamp = at
	if _, err := store.Executions.Append(runID, treeID, branchID, ev); err != nil {
		t.Fatal(err)
	}
}

// 属于某棵 tree 的 run 归主路径管，兜底扫描一律不碰 —— 否则会绕过整树终态校验，
// 在 reset 场景下删掉仍被引用的历史。
func TestScavengeSkipsRunBelongingToTree(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	old := time.Now().Add(-30 * 24 * time.Hour)
	newRetentionRun(t, store, "run-tree", "wf-tree", "tree-x", "root")
	appendAt(t, store, "run-tree", "tree-x", "root", history.NewWorkflowCompletedEvent(nil), old)

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanEventRuns != 0 {
		t.Fatalf("OrphanEventRuns = %d, want 0: tree-owned runs belong to the main GC path", result.OrphanEventRuns)
	}
	if n := countRows(t, store, &executionrepo.RunPO{}, "id = ?", "run-tree"); n != 1 {
		t.Fatalf("tree-owned run was reclaimed by the scavenger (rows=%d)", n)
	}
}

// ── 孤儿事件：run 已不存在、事件还在 ──

// 整树删除是分步的（Step 2 删 run、Step 3 删 events），中途崩溃就停在这个中间态；
// DeleteBranch 的事件删除也移到了事务外。方案把它列为"可接受的残留"，前提是有兜底能清。
func TestScavengeReclaimsOrphanEvents(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	old := time.Now().Add(-30 * 24 * time.Hour)
	newRetentionRun(t, store, "run-gone", "wf-gone", "tree-gone", "root")
	appendAt(t, store, "run-gone", "tree-gone", "root", history.NewMarkerRecordedEvent("m", "a", nil), old)
	appendAt(t, store, "run-gone", "tree-gone", "root", history.NewMarkerRecordedEvent("m", "b", nil), old)

	// 模拟"Step 2 删完 run 就崩溃"：只删 run 行，事件留着。
	if err := store.db.Where("id = ?", "run-gone").Delete(&executionrepo.RunPO{}).Error; err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "run_id = ?", "run-gone"); n != 2 {
		t.Fatalf("setup: expected 2 orphan events, got %d", n)
	}

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanEventRuns != 1 {
		t.Fatalf("OrphanEventRuns = %d, want 1", result.OrphanEventRuns)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "run_id = ?", "run-gone"); n != 0 {
		t.Fatalf("orphan events still present: %d", n)
	}
}

// 孤儿事件同样受 min_age 保护：run 行与它的首条事件由不同事务写入，读到事件却读不到
// run 行可能只是"run 还没提交可见"。min_age 把这个窗口整个排除掉。
func TestScavengeSkipsRecentOrphanEvents(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	newRetentionRun(t, store, "run-new-orphan", "wf-new-orphan", "tree-new-orphan", "root")
	appendAt(t, store, "run-new-orphan", "tree-new-orphan", "root", history.NewMarkerRecordedEvent("m", "a", nil), time.Now())
	if err := store.db.Where("id = ?", "run-new-orphan").Delete(&executionrepo.RunPO{}).Error; err != nil {
		t.Fatal(err)
	}

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanEventRuns != 0 {
		t.Fatalf("OrphanEventRuns = %d, want 0: recent orphans may be in-flight writes", result.OrphanEventRuns)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "run_id = ?", "run-new-orphan"); n != 1 {
		t.Fatalf("DATA LOSS: recent orphan event was deleted (rows=%d)", n)
	}
}

// 僵尸 history_tree 行：树上一条事件都不剩，但 tree row 还在。它会**继续参与引用计数**
// （DeleteBranch 把仍在表里的 branch 当作活引用），不清掉会让后续删除永远删不干净。
func TestScavengeReclaimsOrphanTreeRows(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	// 直接造一行没有任何事件的 tree row。
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{
		Version: 1, TreeID: "tree-zombie", BranchID: "root",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Create(&executionrepo.HistoryTreePO{
		TreeID: "tree-zombie", BranchID: "root", DataJSON: token,
		Info: "root", CreatedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanTreeRows != 1 {
		t.Fatalf("OrphanTreeRows = %d, want 1", result.OrphanTreeRows)
	}
	if n := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id = ?", "tree-zombie"); n != 0 {
		t.Fatalf("zombie tree row still present: %d", n)
	}
}

// 有事件的 tree row 必须留下 —— 否则会把活树的引用信息删掉，正是第一步修复的
// 那类潜伏型数据丢失。
func TestScavengeKeepsTreeRowsWithEvents(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	newBranchTestRun(t, store, "run-live", "tree-live", "root", 3)
	if _, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-live", ForkNodeID: 2,
		NewRunID: "run-live-2", NewBranchID: "branch-2", Info: "reset",
	}); err != nil {
		t.Fatal(err)
	}
	before := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id = ?", "tree-live")

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanTreeRows != 0 {
		t.Fatalf("OrphanTreeRows = %d, want 0", result.OrphanTreeRows)
	}
	if after := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id = ?", "tree-live"); after != before {
		t.Fatalf("DATA LOSS: live tree rows %d -> %d", before, after)
	}
}

// 幂等 + 可重复跑：连跑两轮，第二轮什么都不该再清，也不该报错。
func TestScavengeIsIdempotent(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	old := time.Now().Add(-30 * 24 * time.Hour)
	newRetentionRun(t, store, "run-idem", "wf-idem", "tree-idem", "root")
	appendAt(t, store, "run-idem", "tree-idem", "root", history.NewMarkerRecordedEvent("m", "a", nil), old)
	if err := store.db.Where("id = ?", "run-idem").Delete(&executionrepo.RunPO{}).Error; err != nil {
		t.Fatal(err)
	}

	first, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("first scavenge: %v", err)
	}
	if first.OrphanEventRuns != 1 {
		t.Fatalf("first OrphanEventRuns = %d, want 1", first.OrphanEventRuns)
	}
	second, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("second scavenge: %v", err)
	}
	if second.OrphanEventRuns != 0 || second.OrphanTreeRows != 0 {
		t.Fatalf("second scavenge reclaimed %+v, want all zero", second)
	}
}

// 单轮上限生效：候选多于 limit 时只处理 limit 个，剩下的留给下一轮。
//
// 上限是防止一轮扫描长时间占住连接的护栏 —— 一个积压了几十万孤儿的库，若不分页
// 会让单轮扫描跑到天荒地老，并把 leader 租约拖过期。
//
// 用僵尸 tree row 这条路径来验：它天然有互不冲突的主键。
func TestScavengeRespectsBatchLimit(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	treeIDs := []string{"z1", "z2", "z3", "z4", "z5"}
	for _, id := range treeIDs {
		token, err := execution.SerializeBranchToken(&execution.HistoryBranch{
			Version: 1, TreeID: id, BranchID: "root",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.db.Create(&executionrepo.HistoryTreePO{
			TreeID: id, BranchID: "root", DataJSON: token,
			Info: "root", CreatedAt: time.Now(),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}

	result, err := store.Executions.ScavengeOrphans(time.Now(), 7*24*time.Hour, 2)
	if err != nil {
		t.Fatalf("scavenge: %v", err)
	}
	if result.OrphanTreeRows != 2 {
		t.Fatalf("OrphanTreeRows = %d, want 2 (batch limit)", result.OrphanTreeRows)
	}
	if n := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id IN ?", treeIDs); n != 3 {
		t.Fatalf("remaining zombie tree rows = %d, want 3", n)
	}
}
