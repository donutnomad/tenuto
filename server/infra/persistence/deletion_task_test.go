package persistence

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/dispatchrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// newRetentionStore 建一个开启 GC 的测试 store。
func newRetentionStore(t *testing.T, retention, jitter time.Duration) *Store {
	t.Helper()
	return newMySQLStore(t, Config{
		Retention: executionrepo.RetentionConfig{Retention: retention, Jitter: jitter},
	})
}

// newRetentionRun 建一个带 branch token 的 run，绑定到 (treeID, branchID)。
func newRetentionRun(t *testing.T, store *Store, runID, workflowID, treeID, branchID string) {
	t.Helper()
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{
		Version: 1, TreeID: treeID, BranchID: branchID,
	})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID: runID, Namespace: "default", WorkflowID: workflowID,
		WorkflowConfigVersion: "v1", Status: history.StatusRunning,
		BranchToken: token,
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
}

// deletionTasks 读出全部投递任务。
func deletionTasks(t *testing.T, store *Store) []executionrepo.DeletionTaskPO {
	t.Helper()
	var rows []executionrepo.DeletionTaskPO
	if err := store.db.Order("tree_id ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

// countRows 统计某模型在某条件下的行数。
func countRows(t *testing.T, store *Store, model any, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := store.db.Model(model).Where(query, args...).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

// ── 验收 1：终态 tree 在 retention 后被删除，events/states/dispatches 一并清理 ──

func TestRetentionDeletesTerminalTreeWithAllArtifacts(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-1", "wf-1", "tree-1", "br-1")

	if _, err := store.Executions.Append("run-1", "tree-1", "br-1",
		history.NewMarkerRecordedEvent("m", "a", nil)); err != nil {
		t.Fatal(err)
	}
	// 附属状态：dispatch + activity state + timer state。
	if err := store.db.Create(&dispatchrepo.TaskDispatchPO{
		Namespace: "default", TaskType: "activity", RunID: "run-1",
		ScheduledEventID: 1, Attempt: 1, Status: "pending",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.db.Create(&executionrepo.TimerStatePO{
		RunID: "run-1", ScheduledEventID: 1, Status: "pending",
		Deadline: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := store.Executions.Append("run-1", "tree-1", "br-1",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	tasks := deletionTasks(t, store)
	if len(tasks) != 1 || tasks[0].TreeID != "tree-1" {
		t.Fatalf("expected one deletion task for tree-1, got %+v", tasks)
	}

	if err := store.Executions.DeleteTree("tree-1"); err != nil {
		t.Fatalf("delete tree: %v", err)
	}

	for _, check := range []struct {
		name  string
		model any
		query string
		args  []any
	}{
		{"events", &executionrepo.EventPO{}, "tree_id = ?", []any{"tree-1"}},
		{"runs", &executionrepo.RunPO{}, "id = ?", []any{"run-1"}},
		{"execution states", &executionrepo.WorkflowExecutionStatePO{}, "run_id = ?", []any{"run-1"}},
		{"task dispatches", &dispatchrepo.TaskDispatchPO{}, "run_id = ?", []any{"run-1"}},
		{"timer states", &executionrepo.TimerStatePO{}, "run_id = ?", []any{"run-1"}},
		{"history tree", &executionrepo.HistoryTreePO{}, "tree_id = ?", []any{"tree-1"}},
		{"deletion tasks", &executionrepo.DeletionTaskPO{}, "tree_id = ?", []any{"tree-1"}},
	} {
		if n := countRows(t, store, check.model, check.query, check.args...); n != 0 {
			t.Errorf("%s: %d rows remain after DeleteTree, want 0", check.name, n)
		}
	}
}

// ── 验收 2：同事务性 ──
//
// 这是最关键的一条：**不能只测「关闭后查表存在」**，那证明不了同事务。
// 必须注入投递失败，断言终态事件**一起回滚**（run 仍 Running）。
//
// 注入手法：在事务里把 tenuto_deletion_tasks 换成一张不存在的表 —— 用 GORM 的
// 会话级 NamingStrategy 做不到，故直接 DROP 掉该表，让 enqueueDeletionTask 报
// "no such table"。这是真实的失败注入：投递语句真的执行了、真的失败了。
func TestRetentionEnqueueFailureRollsBackTerminalEvent(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-tx", "wf-tx", "tree-tx", "br-tx")

	if _, err := store.Executions.Append("run-tx", "tree-tx", "br-tx",
		history.NewMarkerRecordedEvent("m", "a", nil)); err != nil {
		t.Fatal(err)
	}
	beforeEvents, err := store.Executions.GetEvents("run-tx")
	if err != nil {
		t.Fatal(err)
	}

	// 注入：删掉投递目标表，让 enqueueDeletionTask 必然失败。
	if err := store.db.Exec("DROP TABLE " + executionrepo.DeletionTaskPO{}.TableName()).Error; err != nil {
		t.Fatal(err)
	}

	// 终态事件必须失败 —— 投递失败连带整个事务回滚。
	if _, err := store.Executions.Append("run-tx", "tree-tx", "br-tx",
		history.NewWorkflowCompletedEvent(nil)); err == nil {
		t.Fatal("expected terminal append to fail when deletion task enqueue fails; " +
			"it succeeded, which means the enqueue is NOT in the same transaction")
	}

	// 按 PO 的建表语句把表建回来，才能读后续断言。
	if err := store.db.Exec(executionrepo.DeletionTaskPO{}.MysqlCreateTable()).Error; err != nil {
		t.Fatal(err)
	}

	// 断言回滚：终态事件没落库。
	afterEvents, err := store.Executions.GetEvents("run-tx")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterEvents) != len(beforeEvents) {
		t.Fatalf("terminal event was NOT rolled back: events %d -> %d",
			len(beforeEvents), len(afterEvents))
	}
	for _, ev := range afterEvents {
		if ev.WorkflowCompleted != nil {
			t.Fatal("terminal event persisted despite enqueue failure — not same-transaction")
		}
	}

	// 断言投影状态也回滚：run 仍是 Running。
	state, err := store.Executions.GetWorkflowExecutionState("run-tx")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != history.StatusRunning {
		t.Fatalf("execution state status = %s, want Running (rollback failed)", state.Status)
	}
}

// ── 验收 3：jitter 生效 ──

func TestRetentionJitterSpreadsFireAt(t *testing.T) {
	const jitter = 30 * time.Minute
	store := newRetentionStore(t, time.Hour, jitter)

	closeTime := time.Now()
	const trees = 24
	for i := range trees {
		runID := "run-j" + string(rune('a'+i))
		treeID := "tree-j" + string(rune('a'+i))
		newRetentionRun(t, store, runID, "wf-j", treeID, "br-j"+string(rune('a'+i)))
		if _, err := store.Executions.Append(runID, treeID, "br-j"+string(rune('a'+i)),
			history.NewWorkflowCompletedEvent(nil)); err != nil {
			t.Fatal(err)
		}
	}

	rows := deletionTasks(t, store)
	if len(rows) != trees {
		t.Fatalf("deletion tasks = %d, want %d", len(rows), trees)
	}
	distinct := map[int64]bool{}
	base := closeTime.Add(time.Hour)
	for _, row := range rows {
		offset := row.FireAt.Sub(base)
		// 允许事件时间戳与 closeTime 之间的少量漂移。
		if offset < -time.Minute || offset > jitter+time.Minute {
			t.Errorf("fire_at offset %s outside [0, %s] jitter window", offset, jitter)
		}
		distinct[int64(offset/time.Second)] = true
	}
	// 24 个任务若无 jitter 会全部落在同一秒。要求至少分散到多个秒桶。
	if len(distinct) < 5 {
		t.Fatalf("fire_at spread across only %d distinct seconds; jitter is not effective", len(distinct))
	}
}

// ── 验收 4：顺序正确（参数化）──
//
// 不变式：**不存在可访问但无事件的 run**。分别在 Step 1/2/3 后中断，断言每种
// 中断态下该不变式都成立。反过来的孤儿事件是可接受的。
func TestRetentionDeleteOrderInvariantUnderInterruption(t *testing.T) {
	cases := []struct {
		name      string
		stopAfter int
	}{
		{"interrupted after step 1 (dispatches)", 1},
		{"interrupted after step 2 (runs+states)", 2},
		{"interrupted after step 3 (events+tree)", 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRetentionStore(t, time.Hour, 0)
			newRetentionRun(t, store, "run-o", "wf-o", "tree-o", "br-o")
			if _, err := store.Executions.Append("run-o", "tree-o", "br-o",
				history.NewWorkflowCompletedEvent(nil)); err != nil {
				t.Fatal(err)
			}

			// 手工重放删除的前 N 步，模拟在该步之后崩溃。
			simulateDeleteSteps(t, store, "tree-o", []string{"run-o"}, tc.stopAfter)

			assertNoReachableRunWithoutEvents(t, store)
		})
	}
}

// simulateDeleteSteps 按 DeleteTree 的顺序执行前 stopAfter 步。
// 与 DeleteTree 的步骤划分保持一致——顺序才是本测试要咬住的东西。
func simulateDeleteSteps(t *testing.T, store *Store, treeID string, runIDs []string, stopAfter int) {
	t.Helper()
	if stopAfter >= 1 {
		if err := store.db.Where("run_id IN ?", runIDs).Delete(&dispatchrepo.TaskDispatchPO{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if stopAfter >= 2 {
		for _, model := range []any{
			&executionrepo.WorkflowExecutionStatePO{}, &executionrepo.ActivityStatePO{},
			&executionrepo.TimerStatePO{}, &executionrepo.ChildWorkflowStatePO{},
		} {
			if err := store.db.Where("run_id IN ?", runIDs).Delete(model).Error; err != nil {
				t.Fatal(err)
			}
		}
		// tenuto_runs 的主键列是 id，不是 run_id。
		if err := store.db.Where("id IN ?", runIDs).Delete(&executionrepo.RunPO{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if stopAfter >= 3 {
		if err := store.db.Where("tree_id = ?", treeID).Delete(&executionrepo.EventPO{}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.db.Where("tree_id = ?", treeID).Delete(&executionrepo.HistoryTreePO{}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

// assertNoReachableRunWithoutEvents 是核心不变式的机械断言：
// 任何仍可通过 tenuto_runs 访问到的 run，都必须还有历史事件。
func assertNoReachableRunWithoutEvents(t *testing.T, store *Store) {
	t.Helper()
	var runs []executionrepo.RunPO
	if err := store.db.Find(&runs).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range runs {
		var n int64
		if err := store.db.Model(&executionrepo.EventPO{}).Where("run_id = ?", row.ID).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatalf("INVARIANT VIOLATED: run %s is still reachable but has no events", row.ID)
		}
	}
}

// ── 验收 5 / 9：引用计数——reset 共享祖先不被误删 ──
// （已由 delete_branch_test.go 覆盖，这里验证整树删除路径同样安全，见验收 14。）

// ── 验收 10：DeleteBranch 重复调用幂等；DeleteTree 同样幂等 ──

func TestRetentionDeleteTreeIsIdempotent(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-i", "wf-i", "tree-i", "br-i")
	if _, err := store.Executions.Append("run-i", "tree-i", "br-i",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		if err := store.Executions.DeleteTree("tree-i"); err != nil {
			t.Fatalf("DeleteTree call %d failed: %v —— GC 依赖删除可重试，报错会让重试永久卡死", i+1, err)
		}
	}
}

// ── 验收 11：task_dispatches 被清理 ──
// （已在验收 1 的表清单里逐表断言。）

// ── 验收 13：5 种终态全部触发投递 ──

func TestRetentionAllFiveTerminalEventsEnqueue(t *testing.T) {
	cases := []struct {
		name  string
		event history.Event
	}{
		{"completed", history.NewWorkflowCompletedEvent(nil)},
		{"failed", history.NewWorkflowFailedEvent("boom", "")},
		{"cancelled", history.NewWorkflowCancelledEvent(nil)},
		{"terminated", history.NewWorkflowTerminatedEvent("op")},
		{"continued as new", history.NewWorkflowContinuedAsNewEvent("wf", "run-next", "v1", nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRetentionStore(t, time.Hour, 0)
			treeID := "tree-" + tc.name
			newRetentionRun(t, store, "run-t", "wf-t", treeID, "br-t")

			if _, err := store.Executions.Append("run-t", treeID, "br-t", tc.event); err != nil {
				t.Fatal(err)
			}
			rows := deletionTasks(t, store)
			if len(rows) != 1 || rows[0].TreeID != treeID {
				t.Fatalf("terminal event %q did not enqueue a deletion task for its tree; got %+v",
					tc.name, rows)
			}
		})
	}
}

// ── 验收 7：retention=0 行为与现在完全一致 ──

func TestRetentionDisabledEnqueuesNothing(t *testing.T) {
	store := newTestStore(t) // Config 无 Retention → GC 关闭
	newRetentionRun(t, store, "run-off", "wf-off", "tree-off", "br-off")

	if _, err := store.Executions.Append("run-off", "tree-off", "br-off",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
	if rows := deletionTasks(t, store); len(rows) != 0 {
		t.Fatalf("retention disabled but %d deletion tasks were enqueued: %+v", len(rows), rows)
	}
}

// ── 验收：retention 下界校验（防误设秒删）──

func TestRetentionBelowMinimumIsRejected(t *testing.T) {
	_, err := New(newUnconnectedDB(t, &gorm.Config{TranslateError: true}), Config{
		Retention: executionrepo.RetentionConfig{Retention: time.Second},
	})
	if err == nil {
		t.Fatal("expected sub-minimum retention to be rejected at construction")
	}
}

// ── 验收 14：reset 共享 tree（咬 v2 的 P0）──
//
// reset 出新 run 后，老 run 到期 → **整树不能被删**（新 run 仍活）；
// 新 run 也到期后整树才删，且过程中新 run 的历史始终可读。
//
// 这条咬的是「GC 单位必须是 tree 而不是 run」：按 run 删会删掉老 run 的 tree row，
// 让新 run 的 ancestor 引用永久丢失 —— 潜伏型数据丢失，第二次 GC 才炸。
func TestRetentionResetSharedTreeIsNotCollectedUntilAllRunsTerminal(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	// base run：10 个事件后终态。
	newRetentionRun(t, store, "run-base", "wf-reset", "tree-shared", "br-base")
	for i := range 10 {
		if _, err := store.Executions.Append("run-base", "tree-shared", "br-base",
			history.NewMarkerRecordedEvent("m", string(rune('a'+i)), nil)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Executions.Append("run-base", "tree-shared", "br-base",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	// reset：fork 出新 run，**共享同一棵 tree**。
	resp, err := store.Executions.ForkBranch(execution.ForkBranchRequest{
		SourceRunID: "run-base", ForkNodeID: 5,
		NewRunID: "run-reset", NewBranchID: "br-reset", Info: "reset",
	})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := execution.DeserializeBranchToken(resp.BranchToken)
	if err != nil {
		t.Fatal(err)
	}
	if forked.TreeID != "tree-shared" {
		t.Fatalf("fork produced tree %q, want the shared tree — 前提不成立", forked.TreeID)
	}
	newResetRun(t, store, "run-reset", "wf-reset", resp.BranchToken)
	if _, err := store.Executions.Append("run-reset", "tree-shared", "br-reset",
		history.NewMarkerRecordedEvent("m", "z", nil)); err != nil {
		t.Fatal(err)
	}

	// 老 run 到期 → 整树不能删：新 run 仍在跑。
	err = store.Executions.DeleteTree("tree-shared")
	if !errors.Is(err, execution.ErrTreeNotTerminal) {
		t.Fatalf("DeleteTree with a live reset run returned %v, want ErrTreeNotTerminal —— "+
			"按 run 删除会在这里删掉 base 的 tree row，造成潜伏型数据丢失", err)
	}

	// 过程中新 run 的历史必须始终可读，且含 fork 前的祖先事件。
	resetEvents, err := store.Executions.GetEvents("run-reset")
	if err != nil {
		t.Fatal(err)
	}
	if len(resetEvents) == 0 {
		t.Fatal("reset run history became unreadable after the skipped collection")
	}
	// base run 的历史也必须完好。
	baseEvents, err := store.Executions.GetEvents("run-base")
	if err != nil {
		t.Fatal(err)
	}
	if len(baseEvents) != 11 {
		t.Fatalf("base run events = %d, want 11 (10 markers + terminal) —— 祖先历史被误删",
			len(baseEvents))
	}

	// 新 run 也终态 → 整树可删。
	if _, err := store.Executions.Append("run-reset", "tree-shared", "br-reset",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
	if err := store.Executions.DeleteTree("tree-shared"); err != nil {
		t.Fatalf("DeleteTree after all runs terminal: %v", err)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ?", "tree-shared"); n != 0 {
		t.Fatalf("%d events remain after whole-tree collection", n)
	}
	if n := countRows(t, store, &executionrepo.RunPO{}, "id IN ?", []string{"run-base", "run-reset"}); n != 0 {
		t.Fatalf("%d runs remain after whole-tree collection", n)
	}
}

// newResetRun 建一个使用给定 branch token 的 reset run。
func newResetRun(t *testing.T, store *Store, runID, workflowID string, token []byte) {
	t.Helper()
	run := &execution.Run{
		ID: runID, Namespace: "default", WorkflowID: workflowID,
		WorkflowConfigVersion: "v1", Status: history.StatusRunning,
		BranchToken: token,
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
}

// ── 验收 15：重放不重排（咬决策 1）──
//
// 重放 History 与 run 删除后的迟到回调，断言**不产生新 deletion task**。
//
// 这条咬的是「投递点必须门控在 inserted==true，且绝不能放进
// engine.ApplyEventToExecutionState」—— 后者是纯内存函数，在重放路径上被反复调用，
// 在那里投递会给已删 run 凭空复活删除任务。
func TestRetentionReplayDoesNotReenqueue(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-r", "wf-r", "tree-r", "br-r")
	if _, err := store.Executions.Append("run-r", "tree-r", "br-r",
		history.NewMarkerRecordedEvent("m", "a", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-r", "tree-r", "br-r",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
	if len(deletionTasks(t, store)) != 1 {
		t.Fatal("setup: expected one deletion task")
	}

	// 场景 A：用 BuildWorkflowExecutionState 重放 History，再走一次读改写投影的事务，不得重排任务。
	if err := store.db.Where("tree_id = ?", "tree-r").Delete(&executionrepo.DeletionTaskPO{}).Error; err != nil {
		t.Fatal(err)
	}
	run, err := store.Executions.Get("run-r")
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.Executions.GetEvents("run-r")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.BuildWorkflowExecutionState(run, events); err != nil {
		t.Fatal(err)
	}
	if _, err := beginActivationInTx(store, "run-r", 1, 0, 0, "replay"); err != nil {
		t.Fatal(err)
	}
	if rows := deletionTasks(t, store); len(rows) != 0 {
		t.Fatalf("replay re-enqueued %d deletion tasks: %+v —— 投递点跑到重放路径上了", len(rows), rows)
	}

	// 场景 B：run 已被整树删除后重放，同样不得凭空复活任务。
	if err := store.Executions.DeleteTree("tree-r"); err != nil {
		t.Fatal(err)
	}
	if _, err := beginActivationInTx(store, "run-r", 1, 0, 0, "replay"); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("activation on a collected run: err = %v, want ErrRunNotFound", err)
	}
	if rows := deletionTasks(t, store); len(rows) != 0 {
		t.Fatalf("replay after collection resurrected %d deletion tasks: %+v", len(rows), rows)
	}
}

// Error Case: run 被整树回收后，迟到的回调（append / activation / sticky）返回 ErrRunNotFound，
// 并且不重新写出 execution state 行与事件行。
func TestLateCallbacksOnCollectedRunReturnRunNotFound(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-gone", "wf-gone", "tree-gone", "br-gone")
	if _, err := store.Executions.Append("run-gone", "tree-gone", "br-gone", history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
	if err := store.Executions.DeleteTree("tree-gone"); err != nil {
		t.Fatal(err)
	}

	late := history.NewMarkerRecordedEvent("m", "late", nil)
	calls := map[string]func() error{
		"Append": func() error { _, err := store.Executions.Append("run-gone", "tree-gone", "br-gone", late); return err },
		"AppendIdempotent": func() error {
			_, _, err := store.Executions.AppendIdempotent("run-gone", "tree-gone", "br-gone", late)
			return err
		},
		"AppendWorkflowTaskScheduled": func() error {
			_, _, err := store.Executions.AppendWorkflowTaskScheduled("run-gone", "tree-gone", "br-gone", history.NewWorkflowTaskScheduledEvent(1000))
			return err
		},
		"RunInTx.Append": func() error {
			return store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
				_, err := tx.Append("run-gone", "tree-gone", "br-gone", late)
				return err
			})
		},
		"BeginWorkflowActivation": func() error {
			_, err := beginActivationInTx(store, "run-gone", 1, 1, 1, "late")
			return err
		},
		"AssignStickyExecution": func() error {
			_, _, err := store.Executions.AssignStickyExecution("run-gone", 1, 1, "worker", "build", time.Now().Add(time.Minute))
			return err
		},
		"ClearStickyExecution": func() error {
			_, _, err := store.Executions.ClearStickyExecution("run-gone", 1, 1)
			return err
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, execution.ErrRunNotFound) {
			t.Errorf("%s on a collected run: err = %v, want ErrRunNotFound", name, err)
		}
	}
	if n := countRows(t, store, &executionrepo.WorkflowExecutionStatePO{}, "run_id = ?", "run-gone"); n != 0 {
		t.Errorf("late callbacks recreated %d execution state rows", n)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "run_id = ?", "run-gone"); n != 0 {
		t.Errorf("late callbacks recreated %d event rows", n)
	}
	if rows := deletionTasks(t, store); len(rows) != 0 {
		t.Errorf("late callbacks recreated deletion tasks: %+v", rows)
	}
}

// Error Case: execution state 行与 run 行一起写入；状态行缺失时读取与追加都返回 ErrRunNotFound，
// 不从 History 重建。
func TestMissingExecutionStateReturnsRunNotFound(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-stateless")
	if _, err := store.Executions.Append("run-stateless", "run-stateless", "run-stateless", history.NewMarkerRecordedEvent("m", "a", nil)); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Where("run_id = ?", "run-stateless").Delete(&executionrepo.WorkflowExecutionStatePO{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-stateless", "run-stateless", "run-stateless", history.NewMarkerRecordedEvent("m", "b", nil)); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("append with missing state row: err = %v, want ErrRunNotFound", err)
	}
	if _, err := store.Executions.GetWorkflowExecutionState("run-stateless"); !errors.Is(err, execution.ErrRunNotFound) {
		t.Fatalf("get missing state row: err = %v, want ErrRunNotFound", err)
	}
	if n := countRows(t, store, &executionrepo.WorkflowExecutionStatePO{}, "run_id = ?", "run-stateless"); n != 0 {
		t.Fatalf("missing state row was recreated: %d rows", n)
	}
}

// Error Case: 没有 branch token 的 Run 不能创建。
func TestCreateRunRequiresBranchToken(t *testing.T) {
	store := newTestStore(t)
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{
			ID: "run-tokenless", WorkflowID: "wf", WorkflowConfigVersion: "v1",
			Status: history.StatusRunning, CreatedAt: time.Now(),
		})
	})
	if err == nil || !strings.Contains(err.Error(), "no branch token") {
		t.Fatalf("create without branch token: err = %v", err)
	}
	if n := countRows(t, store, &executionrepo.RunPO{}, "id = ?", "run-tokenless"); n != 0 {
		t.Fatalf("tokenless run row was written: %d rows", n)
	}
}

// ── 同树多 run：fire_at 取最晚 ──

func TestRetentionUpsertKeepsLatestFireAt(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	newRetentionRun(t, store, "run-m1", "wf-m", "tree-m", "br-m1")
	if _, err := store.Executions.Append("run-m1", "tree-m", "br-m1",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}
	first := deletionTasks(t, store)[0].FireAt

	// 第二个 run 在同一棵树上更晚终态 → fire_at 必须推到更晚的那个。
	newRetentionRun(t, store, "run-m2", "wf-m", "tree-m", "br-m2")
	later := history.NewWorkflowCompletedEvent(nil)
	later.Timestamp = time.Now().Add(2 * time.Hour)
	if _, err := store.Executions.Append("run-m2", "tree-m", "br-m2", later); err != nil {
		t.Fatal(err)
	}

	rows := deletionTasks(t, store)
	if len(rows) != 1 {
		t.Fatalf("same tree produced %d deletion tasks, want 1", len(rows))
	}
	if !rows[0].FireAt.After(first) {
		t.Fatalf("fire_at %s did not advance past %s —— 整树会在最后一个 run 到期前被删",
			rows[0].FireAt, first)
	}

	// 反向：更早的终态不得把 fire_at 拉回来。
	newRetentionRun(t, store, "run-m3", "wf-m", "tree-m", "br-m3")
	earlier := history.NewWorkflowCompletedEvent(nil)
	earlier.Timestamp = time.Now().Add(-2 * time.Hour)
	if _, err := store.Executions.Append("run-m3", "tree-m", "br-m3", earlier); err != nil {
		t.Fatal(err)
	}
	latest := deletionTasks(t, store)[0].FireAt
	if latest.Before(rows[0].FireAt) {
		t.Fatalf("an earlier terminal event pulled fire_at back from %s to %s",
			rows[0].FireAt, latest)
	}
}

// ── 验收 17：status 不一致 —— 终态事件已落但 run.Status 仍 Running ──
//
// continue-as-new 与带重试的失败先写终态事件，Run 的终态随接续 Run 提交，存在这个窗口。
// Step 0 若只读 run.Status，落进窗口的 run 会被**永久跳过**。
func TestRetentionStep0UsesHistoryNotRunStatus(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-s", "wf-s", "tree-s", "br-s")
	if _, err := store.Executions.Append("run-s", "tree-s", "br-s",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	// 构造窗口：事件说终态，run.Status 仍是 Running。
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-s").
		Update("status", string(history.StatusRunning)).Error; err != nil {
		t.Fatal(err)
	}
	var row executionrepo.RunPO
	if err := store.db.Where("id = ?", "run-s").First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Status != string(history.StatusRunning) {
		t.Fatalf("setup: run status = %s, want Running", row.Status)
	}

	if err := store.Executions.DeleteTree("tree-s"); err != nil {
		t.Fatalf("DeleteTree skipped a run whose history says terminal: %v —— "+
			"Step 0 读了 run.Status 而非历史事件，这类 run 会永久不回收", err)
	}
	if n := countRows(t, store, &executionrepo.RunPO{}, "id = ?", "run-s"); n != 0 {
		t.Fatal("run was not collected despite terminal history")
	}
}

// ── 验收：Step 0 拒绝仍在运行的 run ──

func TestRetentionStep0RejectsRunningRun(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-live", "wf-live", "tree-live", "br-live")
	if _, err := store.Executions.Append("run-live", "tree-live", "br-live",
		history.NewMarkerRecordedEvent("m", "a", nil)); err != nil {
		t.Fatal(err)
	}

	err := store.Executions.DeleteTree("tree-live")
	if !errors.Is(err, execution.ErrTreeNotTerminal) {
		t.Fatalf("DeleteTree on a live tree returned %v, want ErrTreeNotTerminal", err)
	}
	if n := countRows(t, store, &executionrepo.EventPO{}, "tree_id = ?", "tree-live"); n == 0 {
		t.Fatal("live tree events were deleted")
	}
}

// ── 验收 6：崩溃恢复——重启后 deletion task 仍能触发 ──
//
// 任务持久在表里（不是内存 timer），故"重启"等价于换一个 Store 实例读同一个库。
func TestRetentionTaskSurvivesRestart(t *testing.T) {
	db := mysqltest.NewDB(t)
	cfg := Config{Retention: executionrepo.RetentionConfig{Retention: time.Hour}}

	first, err := New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	newRetentionRun(t, first, "run-c", "wf-c", "tree-c", "br-c")
	if _, err := first.Executions.Append("run-c", "tree-c", "br-c",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	// "重启"。
	second, err := New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := second.Executions.ListDueDeletionTasks(time.Now().Add(2*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].TreeID != "tree-c" {
		t.Fatalf("deletion task did not survive restart; got %+v", tasks)
	}
	if err := second.Executions.DeleteTree("tree-c"); err != nil {
		t.Fatalf("delete tree after restart: %v", err)
	}
}

// ── 验收 16：失败退避——坏任务不得饿死正常任务 ──

func TestRetentionDeferPreventsHeadOfLineBlocking(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)

	now := time.Now()
	// bad 排在最前（fire_at 最早），good 稍后。
	for _, seed := range []struct {
		treeID string
		fireAt time.Time
	}{
		{"tree-bad", now.Add(-time.Hour)},
		{"tree-good", now.Add(-time.Minute)},
	} {
		if err := store.db.Create(&executionrepo.DeletionTaskPO{
			TreeID: seed.treeID, Namespace: "default", FireAt: seed.fireAt,
			CreatedAt: now, UpdatedAt: now,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}

	// 窗口只有 1 —— 不推后的话 bad 会永远独占它。
	due, err := store.Executions.ListDueDeletionTasks(now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].TreeID != "tree-bad" {
		t.Fatalf("expected tree-bad first, got %+v", due)
	}

	// 模拟失败：推后。
	if err := store.Executions.DeferDeletionTask("tree-bad", now.Add(time.Hour), true, errors.New("boom")); err != nil {
		t.Fatal(err)
	}

	// 下一轮同样窗口=1，正常任务必须能排上。
	due, err = store.Executions.ListDueDeletionTasks(now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].TreeID != "tree-good" {
		t.Fatalf("good task starved by the failing one: %+v —— 活锁", due)
	}

	attempts, ok, err := store.Executions.DeletionTaskAttempts("tree-bad")
	if err != nil || !ok {
		t.Fatalf("attempts lookup: %v ok=%v", err, ok)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

// ── 验收 18：current 语义——GC 掉 is_current 的终态 run 后可重新 start ──

func TestRetentionFreesCurrentRunSlot(t *testing.T) {
	store := newRetentionStore(t, time.Hour, 0)
	newRetentionRun(t, store, "run-cur", "wf-cur", "tree-cur", "br-cur")

	marker := "default/wf-cur"
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-cur").
		Updates(map[string]any{"is_current": true, "current_marker": marker}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append("run-cur", "tree-cur", "br-cur",
		history.NewWorkflowCompletedEvent(nil)); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Executions.GetCurrentRunID("default", "wf-cur"); err != nil {
		t.Fatalf("setup: current run should resolve: %v", err)
	}

	if err := store.Executions.DeleteTree("tree-cur"); err != nil {
		t.Fatal(err)
	}

	// current 槽位已释放：查询返回 ErrNoCurrentRun。
	// ⚠️ 语义扩大：ErrNoCurrentRun 原本只表示「从未启动过任何 run」，启用 GC 后
	// 也涵盖「跑过但已被回收」。调用方无法区分两者。第二步会引入可区分的哨兵。
	if _, err := store.Executions.GetCurrentRunID("default", "wf-cur"); !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("GetCurrentRunID after collection = %v, want ErrNoCurrentRun", err)
	}

	// 同 workflowID 可以重新占用 current 槽（唯一约束随行消失）。
	newRetentionRun(t, store, "run-cur2", "wf-cur", "tree-cur2", "br-cur2")
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-cur2").
		Updates(map[string]any{"is_current": true, "current_marker": marker}).Error; err != nil {
		t.Fatalf("re-claiming the current marker failed: %v —— 唯一约束未随行释放", err)
	}
	got, err := store.Executions.GetCurrentRunID("default", "wf-cur")
	if err != nil || got != "run-cur2" {
		t.Fatalf("current run = %q err=%v, want run-cur2", got, err)
	}
}

// beginActivationInTx 在一个只开始激活的事务里调用 BeginWorkflowActivation。
func beginActivationInTx(store *Store, runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error) {
	begun := false
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		begun, err = tx.BeginWorkflowActivation(runID, scheduledEventID, executionGeneration, stickyGeneration, activationID)
		return err
	})
	return begun, err
}
