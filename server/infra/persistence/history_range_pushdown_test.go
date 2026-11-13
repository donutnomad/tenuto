package persistence

import (
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"gorm.io/gorm"
)

// 本文件覆盖 GetEventsRange 把范围过滤下推到 SQL 后的**正确性**。
//
// 原实现是 GetEvents 全量加载整个 history 再在 Go 里按 event.ID 过滤——
// 它位于增量激活最热路径上（server_incremental_activation.go 的 GetWorkflowActivation
// 每次都调，只要一小段增量），使单个 run 随自身历史增长呈 O(H²)。
//
// 下推的风险点是 **branch 语义**：history 有祖先分支链，getEventsWithDB 会把
// 可见区间解析成一串 (branch_id, node_id) 的 OR 组。范围条件必须作为独立 AND 项
// 叠在 OR 组之外，否则跨 branch 的范围查询会错。故下面的用例全部把下推结果与
// "全量取出再过滤"的参考实现**逐条比对**。

// referenceEventsRange 是修复前的实现，作为 oracle：全量取出再在 Go 里过滤。
func referenceEventsRange(t *testing.T, store *Store, runID string, afterExclusive, throughInclusive int64) []history.Event {
	t.Helper()
	all, err := store.Executions.GetEvents(runID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	result := make([]history.Event, 0)
	for _, event := range all {
		if event.ID > afterExclusive && (throughInclusive == 0 || event.ID <= throughInclusive) {
			result = append(result, event)
		}
	}
	return result
}

func assertRangeMatchesReference(t *testing.T, store *Store, runID string, afterExclusive, throughInclusive int64) []history.Event {
	t.Helper()
	got, err := store.Executions.GetEventsRange(runID, afterExclusive, throughInclusive)
	if err != nil {
		t.Fatalf("GetEventsRange(%d, %d]: %v", afterExclusive, throughInclusive, err)
	}
	want := referenceEventsRange(t, store, runID, afterExclusive, throughInclusive)
	if len(got) != len(want) {
		t.Fatalf("GetEventsRange(%d, %d] 返回 %d 条，参考实现 %d 条 (got ids=%v want ids=%v)",
			afterExclusive, throughInclusive, len(got), len(want), eventIDs(got), eventIDs(want))
	}
	for i := range got {
		if got[i].ID != want[i].ID {
			t.Fatalf("GetEventsRange(%d, %d] 第 %d 条 id=%d，参考实现 id=%d (got=%v want=%v)",
				afterExclusive, throughInclusive, i, got[i].ID, want[i].ID, eventIDs(got), eventIDs(want))
		}
	}
	return got
}

func eventIDs(events []history.Event) []int64 {
	ids := make([]int64, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

// TestGetEventsRangeRootBranch 覆盖只有 root branch 的 run，
// 含两个边界：afterExclusive=0（不限下界）与 throughInclusive=0（不限上界）。
func TestGetEventsRangeRootBranch(t *testing.T) {
	store := newTestStore(t)
	run := &execution.Run{ID: "plain-run", WorkflowID: "wf", WorkflowConfigVersion: "v1",
		Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		if _, err := store.Executions.Append(run.ID, run.ID, run.ID, history.Event{
			Timestamp:        time.Unix(int64(i+1), 0),
			WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("s%d", i)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// (0, 0] —— 两端都不限，等价于全量。
	all := assertRangeMatchesReference(t, store, run.ID, 0, 0)
	if len(all) != 6 {
		t.Fatalf("全量应有 6 条，实得 %d", len(all))
	}
	if all[0].ID != 1 {
		t.Fatalf("afterExclusive=0 必须包含首个事件 id=1，实得 id=%d", all[0].ID)
	}

	// (2, 0] —— 只限下界（这正是增量激活的调用形态）。
	incremental := assertRangeMatchesReference(t, store, run.ID, 2, 0)
	if len(incremental) != 4 || incremental[0].ID != 3 {
		t.Fatalf("(2,0] 应为 id 3..6，实得 %v", eventIDs(incremental))
	}

	// (0, 3] —— 只限上界。
	prefix := assertRangeMatchesReference(t, store, run.ID, 0, 3)
	if len(prefix) != 3 || prefix[len(prefix)-1].ID != 3 {
		t.Fatalf("(0,3] 应为 id 1..3，实得 %v", eventIDs(prefix))
	}

	// 两端都限、单点区间、空区间。
	assertRangeMatchesReference(t, store, run.ID, 2, 4)
	assertRangeMatchesReference(t, store, run.ID, 3, 4)
	assertRangeMatchesReference(t, store, run.ID, 6, 0)  // 上界之后 → 空
	assertRangeMatchesReference(t, store, run.ID, 99, 0) // 远超尾部 → 空
}

// TestGetEventsRangeAcrossBranches 是本组的核心用例：fork 之后 run 的可见历史
// 由「祖先分支的前缀 + 当前分支的后缀」拼成，范围查询必须**跨越拼接点**依然正确。
func TestGetEventsRangeAcrossBranches(t *testing.T) {
	store := newTestStore(t)

	rootToken, err := execution.SerializeBranchToken(&execution.HistoryBranch{Version: 1, TreeID: "tree", BranchID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	root := &execution.Run{ID: "root-run", WorkflowID: "wf", WorkflowConfigVersion: "v1",
		Status: history.StatusRunning, BranchToken: rootToken, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(root) }); err != nil {
		t.Fatal(err)
	}
	// root 分支写 5 条：node_id 1..5。
	for i := range 5 {
		if _, err := store.Executions.Append(root.ID, "tree", "root", history.Event{
			Timestamp:        time.Unix(int64(i+1), 0),
			WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("root%d", i)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 在 node 3 处 fork：reset run 可见 root 的 1..3，之后接自己分支的 4..
	fork, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: root.ID, NewRunID: "reset-run", NewBranchID: "reset", ForkNodeID: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	reset := &execution.Run{ID: "reset-run", WorkflowID: "wf", WorkflowConfigVersion: "v1",
		Status: history.StatusRunning, BranchToken: fork.BranchToken, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(reset) }); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if _, err := store.Executions.Append(reset.ID, "tree", "reset", history.Event{
			Timestamp:        time.Unix(int64(i+10), 0),
			WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("reset%d", i)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 先确认拼接后的可见历史确实跨了两个 branch，否则本用例是空转。
	visible := referenceEventsRange(t, store, reset.ID, 0, 0)
	if len(visible) < 5 {
		t.Fatalf("可见历史只有 %d 条（%v），不足以覆盖跨 branch 场景", len(visible), eventIDs(visible))
	}
	maxID := visible[len(visible)-1].ID

	// 逐一扫过所有 (after, through] 组合，全部与参考实现比对。
	// 这样必然覆盖到"区间完全落在祖先分支内""完全落在当前分支内""横跨拼接点"三类。
	for after := int64(0); after <= maxID+1; after++ {
		// through=0 表示不限上界。
		assertRangeMatchesReference(t, store, reset.ID, after, 0)
		for through := after; through <= maxID+1; through++ {
			if through == 0 {
				continue // (0,0] 已在上面覆盖
			}
			assertRangeMatchesReference(t, store, reset.ID, after, through)
		}
	}

	// 显式钉住一个横跨拼接点的区间：必须同时含祖先分支与当前分支的事件。
	spanning := assertRangeMatchesReference(t, store, reset.ID, 2, 5)
	if len(spanning) == 0 {
		t.Fatal("横跨 fork 点的区间 (2,5] 不应为空")
	}

	// root run 自身的可见范围不受 fork 影响。
	rootRange := assertRangeMatchesReference(t, store, root.ID, 1, 4)
	if len(rootRange) != 3 || rootRange[0].ID != 2 || rootRange[2].ID != 4 {
		t.Fatalf("root run (1,4] 应为 id 2..4，实得 %v", eventIDs(rootRange))
	}
}

// TestGetEventsRangeRejectsInvalidRange 锁定参数校验没有在重构中丢失。
func TestGetEventsRangeRejectsInvalidRange(t *testing.T) {
	store := newTestStore(t)
	run := &execution.Run{ID: "invalid-range-run", WorkflowID: "wf", WorkflowConfigVersion: "v1",
		Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ after, through int64 }{
		{-1, 0}, {0, -1}, {5, 3},
	} {
		if _, err := store.Executions.GetEventsRange(run.ID, tc.after, tc.through); err == nil {
			t.Fatalf("GetEventsRange(%d, %d] 应当报错", tc.after, tc.through)
		}
	}
}

// TestGetEventsRangeDoesNotLoadWholeHistory 是**性能**断言：证明范围过滤真的下推到了
// SQL，而不是"全量取出再过滤"。用 GORM callback 统计本次查询实际扫回的行数——
// 修复前这个数字等于整个 history 的长度（H），修复后等于区间内的行数。
//
// 只断言"返回值正确"是不够的：假分页的返回值也是正确的，正是它导致 O(H²)。
func TestGetEventsRangeDoesNotLoadWholeHistory(t *testing.T) {
	store := newTestStore(t)
	run := &execution.Run{ID: "pushdown-run", WorkflowID: "wf", WorkflowConfigVersion: "v1",
		Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}
	const historyLen = 50
	for i := range historyLen {
		if _, err := store.Executions.Append(run.ID, run.ID, run.ID, history.Event{
			Timestamp:        time.Unix(int64(i+1), 0),
			WorkflowSignaled: &history.WorkflowSignaledEvent{Name: fmt.Sprintf("s%d", i)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 统计 SELECT 实际返回的行数（RowsAffected 对 Find 即命中行数）。
	// 走 GORM 的 query callback：这是"到底扫回了多少行"的直接证据，
	// 比对着实现读代码更难被后续重构悄悄改坏。
	gormDB, ok := store.db.(*gorm.DB)
	if !ok {
		t.Skipf("底层句柄不是 *gorm.DB（%T），跳过行数探针", store.db)
	}
	var scannedRows int64
	if err := gormDB.Callback().Query().After("gorm:query").
		Register("test:count_scanned_rows", func(db *gorm.DB) {
			if db.Statement != nil && db.Statement.Table == "workflow_events" {
				scannedRows += db.RowsAffected
			}
		}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = gormDB.Callback().Query().Remove("test:count_scanned_rows")
	})

	// 取最后 5 条增量——增量激活的典型调用形态。
	got, err := store.Executions.GetEventsRange(run.ID, historyLen-5, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("应返回 5 条增量，实得 %d 条", len(got))
	}
	if scannedRows > 5 {
		t.Fatalf("GetEventsRange 从 workflow_events 扫回 %d 行，只需要 5 行——"+
			"范围过滤没有下推到 SQL（history 全长 %d，这正是 O(H²) 的来源）",
			scannedRows, historyLen)
	}
}
