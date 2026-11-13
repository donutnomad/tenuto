package persistence

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"gorm.io/gorm"
)

// newTerminalTree 建一棵只有一个终态 run 的树：3 个 marker + WorkflowCompleted。
func newTerminalTree(t *testing.T, store *Store, runID, treeID, branchID string) {
	t.Helper()
	newRetentionRun(t, store, runID, "wf-"+runID, treeID, branchID)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := store.Executions.Append(runID, treeID, branchID, history.NewMarkerRecordedEvent("m", name, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Executions.Append(runID, treeID, branchID, history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
}

// Edge Case: reset 已经 fork 出新 branch、新 run 还没写出首条事件时，DeleteTree 整树跳过，
// 源 run 的历史保持完整，随后提交的 reset run 能读到祖先历史。
func TestDeleteTreeSkipsTreeWithForkInFlight(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newTerminalTree(t, store, "run-base", "tree-fork", "br-base")

	resp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-base", ForkNodeID: 2, NewRunID: "run-reset", NewBranchID: "br-reset", Info: "reset",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Executions.DeleteTree("tree-fork"); !errors.Is(err, execution.ErrTreeNotTerminal) {
		t.Fatalf("delete tree with a fork in flight: err = %v, want ErrTreeNotTerminal", err)
	}
	if _, err := store.Executions.Get("run-base"); err != nil {
		t.Fatalf("source run was deleted under an in-flight fork: %v", err)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ?", "tree-fork"); n != 4 {
		t.Fatalf("tree events = %d, want 4 —— 跳过的整树删除动了历史", n)
	}

	// reset 事务提交：新 run 建在共享的树上，能读到 fork 点之前的祖先事件。
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{
			ID: "run-reset", Namespace: "default", WorkflowID: "wf-run-base", WorkflowConfigVersion: "v1",
			Status: history.StatusRunning, BranchToken: resp.BranchToken,
		})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-reset", "tree-fork", "br-reset", history.NewMarkerRecordedEvent("m", "after-reset", nil)); err != nil {
		t.Fatal(err)
	}
	events, err := store.Executions.GetEvents("run-reset")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("reset run history = %d events, want 2 ancestor events + 1 own", len(events))
	}
	// 新 run 未终态，整树继续跳过。
	if err := store.Executions.DeleteTree("tree-fork"); !errors.Is(err, execution.ErrTreeNotTerminal) {
		t.Fatalf("delete tree with a running reset run: err = %v, want ErrTreeNotTerminal", err)
	}
}

// Edge Case: fork 之后 reset 事务没有提交，空 branch 行超过 forkCommitGrace 后随整树回收。
func TestDeleteTreeCollectsAbandonedFork(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newTerminalTree(t, store, "run-base", "tree-abandoned", "br-base")
	if _, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-base", ForkNodeID: 2, NewRunID: "run-reset", NewBranchID: "br-reset", Info: "reset",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Model(&executionrepo.HistoryTreePO{}).
		Where("tree_id = ? AND branch_id = ?", "tree-abandoned", "br-reset").
		Update("created_at", time.Now().Add(-2*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.Executions.DeleteTree("tree-abandoned"); err != nil {
		t.Fatalf("delete tree with an abandoned fork: %v", err)
	}
	for name, model := range map[string]any{"events": &executionrepo.EventPO{}, "history tree": &executionrepo.HistoryTreePO{}} {
		if n := countRows(t, store, model, "tree_id = ?", "tree-abandoned"); n != 0 {
			t.Errorf("%s rows left after delete: %d", name, n)
		}
	}
}

// Error Case: 整树回收之后再 fork，ForkBranch 返回错误，树上不留下新的 tree 行。
func TestForkBranchFailsAfterTreeDeleted(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newTerminalTree(t, store, "run-base", "tree-deleted", "br-base")
	if err := store.Executions.DeleteTree("tree-deleted"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-base", ForkNodeID: 2, NewRunID: "run-reset", NewBranchID: "br-reset", Info: "reset",
	}); err == nil {
		t.Fatal("fork on a deleted tree succeeded")
	}
	if n := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id = ?", "tree-deleted"); n != 0 {
		t.Fatalf("fork on a deleted tree left %d tree rows", n)
	}
}

// Integration（仅 MySQL，依赖真实行锁）: ForkBranch 在事务外读到了源 run，随后与一个持有 tree 锁、
// 正在删除这棵树的事务相遇。fork 的写入等待删除方提交，提交后复查源 run 失败并整体回滚。
//
// 两个变体对应 lockHistoryTree 的两种加锁形态：树上已有 tree 行（记录锁）与从未 fork 过（间隙锁）。
func TestMySQLForkBranchWaitsForDeleteTreeAndFails(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withTreeRow bool
	}{
		{"tree rows exist", true},
		{"tree never forked", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := requireMySQL(t)
			newTerminalTree(t, store, "run-base", "tree-race", "br-base")
			if tc.withTreeRow {
				// 先做一次已放弃的 fork，让树上带有 tree 行。
				if _, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
					SourceRunID: "run-base", ForkNodeID: 1, NewRunID: "run-old", NewBranchID: "br-old", Info: "reset",
				}); err != nil {
					t.Fatal(err)
				}
			}

			// 删除方：与 DeleteTree 的第一个事务同构——先锁 tree 行，再删 run 与 tree 行，暂不提交。
			gormDB := store.db.(*gorm.DB)
			deleter := gormDB.Begin()
			if deleter.Error != nil {
				t.Fatal(deleter.Error)
			}
			defer deleter.Rollback()
			var locked []executionrepo.HistoryTreePO
			if err := deleter.Raw("SELECT * FROM tenuto_history_tree WHERE tree_id = ? FOR UPDATE", "tree-race").Scan(&locked).Error; err != nil {
				t.Fatal(err)
			}
			if err := deleter.Exec("DELETE FROM tenuto_runs WHERE id = ?", "run-base").Error; err != nil {
				t.Fatal(err)
			}
			if err := deleter.Exec("DELETE FROM tenuto_history_tree WHERE tree_id = ?", "tree-race").Error; err != nil {
				t.Fatal(err)
			}

			forkErr := make(chan error, 1)
			go func() {
				_, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
					SourceRunID: "run-base", ForkNodeID: 2, NewRunID: "run-reset", NewBranchID: "br-reset", Info: "reset",
				})
				forkErr <- err
			}()

			select {
			case err := <-forkErr:
				t.Fatalf("fork returned while the deleting transaction still held the tree lock: %v", err)
			case <-time.After(500 * time.Millisecond):
			}
			if err := deleter.Commit().Error; err != nil {
				t.Fatal(err)
			}

			select {
			case err := <-forkErr:
				if !errors.Is(err, execution.ErrRunNotFound) {
					t.Fatalf("fork after the tree was deleted: err = %v, want ErrRunNotFound", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("fork still blocked after the deleting transaction committed")
			}
			if n := countRows(t, store, &executionrepo.HistoryTreePO{}, "tree_id = ?", "tree-race"); n != 0 {
				t.Fatalf("failed fork left %d tree rows on the deleted tree", n)
			}
		})
	}
}

// Integration（仅 MySQL，依赖真实行锁）: fork 的事务先写下 tree 行且未提交时，DeleteTree 在取 tree 锁处等待；
// fork 提交后 DeleteTree 读到新 branch 并整树跳过。
func TestMySQLDeleteTreeWaitsForForkAndSkips(t *testing.T) {
	store := requireMySQL(t)
	newTerminalTree(t, store, "run-base", "tree-race", "br-base")

	// fork 方：与 ForkBranch 的事务同构——写下新 branch 的 tree 行，暂不提交。
	gormDB := store.db.(*gorm.DB)
	forker := gormDB.Begin()
	if forker.Error != nil {
		t.Fatal(forker.Error)
	}
	defer forker.Rollback()
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{
		Version: 1, TreeID: "tree-race", BranchID: "br-reset",
		Ancestors: []execution.HistoryBranchRange{{BranchID: "br-base", BeginNodeID: 1, EndNodeID: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := forker.Create(&executionrepo.HistoryTreePO{
		TreeID: "tree-race", BranchID: "br-reset", DataJSON: token, Info: "reset", CreatedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	deleteErr := make(chan error, 1)
	go func() { deleteErr <- store.Executions.DeleteTree("tree-race") }()

	select {
	case err := <-deleteErr:
		t.Fatalf("delete tree returned while the fork transaction was still open: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := forker.Commit().Error; err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-deleteErr:
		if !errors.Is(err, execution.ErrTreeNotTerminal) {
			t.Fatalf("delete tree after a fork won the lock: err = %v, want ErrTreeNotTerminal", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("delete tree still blocked after the fork committed")
	}
	if _, err := store.Executions.Get("run-base"); err != nil {
		t.Fatalf("source run was deleted although the fork won the lock: %v", err)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ?", "tree-race"); n != 4 {
		t.Fatalf("tree events = %d, want 4", n)
	}
}
