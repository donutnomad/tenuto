package persistence

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"gorm.io/gorm"
)

// createRunWF 在 store 中建一条指定 workflowID/status 的 run。
func createRunWF(t *testing.T, store *Store, runID, workflowID string, status history.Status) {
	t.Helper()
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(rootBranch(t, &execution.Run{
			ID:         runID,
			WorkflowID: workflowID,
			Status:     status,
			CreatedAt:  time.Now(),
		}))
	}); err != nil {
		t.Fatal(err)
	}
}

// setCurrent 在事务内首次把 runID 设为 current（expectedOld="")。
func setCurrent(t *testing.T, store *Store, namespace, workflowID, runID string) {
	t.Helper()
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS(namespace, workflowID, "", runID)
		if e != nil {
			return e
		}
		if !ok {
			t.Fatalf("expected first SetCurrentRunCAS to succeed for %s", runID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Happy Path: 存一个 is_current run，GetCurrentRunID 返回它。
func TestGormGetCurrentRunIDRoundTrip(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	setCurrent(t, store, "", "wf-1", "run-1")

	got, err := store.Executions.GetCurrentRunID("", "wf-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "run-1" {
		t.Fatalf("GetCurrentRunID = %q, want run-1", got)
	}

	// is_current 持久化：重新 Get run 也应带 IsCurrent=true（崩溃恢复正确性）。
	run, err := store.Executions.Get("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !run.IsCurrent {
		t.Fatal("persisted run.IsCurrent = false, want true")
	}
}

// Error Case: 该 workflow 从未设过 current → GetCurrentRunID 返回 ErrNoCurrentRun。
func TestGormGetCurrentRunIDNoCurrent(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	_, err := store.Executions.GetCurrentRunID("", "wf-1")
	if !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("err = %v, want ErrNoCurrentRun", err)
	}
}

// Happy Path: 终态 run 也能作为 current 被查到（complete 的 run is_current=true 仍返回它）。
func TestGormGetCurrentRunIDTerminalIsCurrent(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	setCurrent(t, store, "", "wf-1", "run-1")

	// run complete（终态），但 current 不清——current 指向最后一代。
	executiontest.UpdateRun(t, store.Executions, "run-1", func(run *execution.Run) error {
		return run.MarkTerminal(history.StatusCompleted, nil)
	})

	got, err := store.Executions.GetCurrentRunID("", "wf-1")
	if err != nil {
		t.Fatalf("err = %v, terminal run should still be current", err)
	}
	if got != "run-1" {
		t.Fatalf("GetCurrentRunID = %q, want run-1 (terminal current)", got)
	}
}

// Happy Path: ① 首次设置 ② 正确 expectedOld 切换成功（旧 false 新 true）③ 只有一个 is_current。
func TestGormSetCurrentRunCASSwitch(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-2", "wf-1", history.StatusRunning)

	setCurrent(t, store, "", "wf-1", "run-1")

	// 正确 expectedOld=run-1，切到 run-2。
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "run-1", "run-2")
		if e != nil {
			return e
		}
		if !ok {
			t.Fatal("CAS with correct expectedOld should succeed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	got, _ := store.Executions.GetCurrentRunID("", "wf-1")
	if got != "run-2" {
		t.Fatalf("after switch GetCurrentRunID = %q, want run-2", got)
	}

	// 旧 current=false, 新 current=true：只有一个 is_current。
	r1, _ := store.Executions.Get("run-1")
	r2, _ := store.Executions.Get("run-2")
	if r1.IsCurrent {
		t.Fatal("old run-1 should no longer be current")
	}
	if !r2.IsCurrent {
		t.Fatal("new run-2 should be current")
	}
	assertSingleCurrent(t, store, "wf-1")
}

// Error Case: 错误 expectedOld（模拟被切走）→ CAS 返回 false，不改 current。
func TestGormSetCurrentRunCASMismatch(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-2", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-3", "wf-1", history.StatusRunning)

	setCurrent(t, store, "", "wf-1", "run-1")

	// 用错误的 expectedOld=run-99（current 实际是 run-1）→ compare 失败。
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "run-99", "run-3")
		if e != nil {
			return e
		}
		if ok {
			t.Fatal("CAS with wrong expectedOld should fail (false)")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// current 未被改动，仍是 run-1，run-3 不应变成 current。
	got, _ := store.Executions.GetCurrentRunID("", "wf-1")
	if got != "run-1" {
		t.Fatalf("current changed on failed CAS: got %q, want run-1", got)
	}
	r3, _ := store.Executions.Get("run-3")
	if r3.IsCurrent {
		t.Fatal("run-3 should NOT be current after failed CAS")
	}
	assertSingleCurrent(t, store, "wf-1")
}

// Error Case: 已有 current 时，再次 expectedOld="" 首次设置 → 返回 false（不能重复首设）。
func TestGormSetCurrentRunCASFirstSetTwice(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-2", "wf-1", history.StatusRunning)
	setCurrent(t, store, "", "wf-1", "run-1")

	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "", "run-2")
		if e != nil {
			return e
		}
		if ok {
			t.Fatal("first-set with existing current should fail")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.Executions.GetCurrentRunID("", "wf-1")
	if got != "run-1" {
		t.Fatalf("current changed: got %q, want run-1", got)
	}
}

// Integration: 唯一索引 uniq_run_current_marker 真生效校验。
// 两个 run 写入相同 current_marker（namespace+"/"+workflowID），第二个必须被唯一约束拒绝；
// 同时 NULL marker（is_current=false）不参与唯一约束，多行 NULL 可共存。
func TestGormCurrentMarkerUniqueConstraint(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-2", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-3", "wf-1", history.StatusRunning)

	marker := "default/wf-1"

	// 给 run-1 写 marker：成功。
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-1").
		Update("current_marker", marker).Error; err != nil {
		t.Fatalf("first marker write should succeed: %v", err)
	}

	// 给 run-2 写相同 marker：必须撞唯一约束。
	err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-2").
		Update("current_marker", marker).Error
	if err == nil {
		t.Fatal("second identical current_marker write should violate UNIQUE constraint")
	}

	// run-2、run-3 marker 仍为 NULL，多行 NULL 不冲突。
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-3").
		Update("current_marker", gorm.Expr("NULL")).Error; err != nil {
		t.Fatalf("NULL marker write should never conflict: %v", err)
	}
}

// Error Case: 模拟首设 TOCTOU 竞态——run-1 残留 marker（Count 自检看不到，因 is_current=false），
// run-2 走 SetCurrentRunCAS 首设通过 Count，但置 current 写相同 marker 撞唯一约束 → 返回 false。
// 验证 DB 唯一约束兜底：无锁首设路径即便 Count 漏判，最终仍只一个 current。
func TestGormSetCurrentRunCASFirstSetRejectedByConstraint(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	createRunWF(t, store, "run-2", "wf-1", history.StatusRunning)

	// run-1 正常首设为 current（marker 写好）。
	setCurrent(t, store, "", "wf-1", "run-1")

	// 制造 TOCTOU 脏态：is_current 置回 false 但保留 marker（模拟另一个首设事务尚未提交清 marker）。
	// 此时 SetCurrentRunCAS 的 Count 自检（按 is_current=true 统计）看不到 run-1，会放行 run-2 首设。
	if err := store.db.Model(&executionrepo.RunPO{}).Where("id = ?", "run-1").
		Update("is_current", false).Error; err != nil {
		t.Fatal(err)
	}

	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		ok, e := tx.SetCurrentRunCAS("", "wf-1", "", "run-2")
		if e != nil {
			return e
		}
		if ok {
			t.Fatal("first-set should be rejected by UNIQUE constraint (returns false)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("constraint violation must be swallowed as CAS-false, not propagated: %v", err)
	}

	// run-2 未成为 current。
	r2, _ := store.Executions.Get("run-2")
	if r2.IsCurrent {
		t.Fatal("run-2 must NOT be current after constraint rejection")
	}
	assertSingleCurrent(t, store, "wf-1")
}

// Error Case: Run 更新不改动 is_current。is_current=true 的 run 改 status 后，
// 断言 is_current 仍 true、marker 仍在，current 只能经 SetCurrentRunCAS 切换。
func TestGormUpdateRunDoesNotTouchCurrent(t *testing.T) {
	store := newTestStore(t)
	createRunWF(t, store, "run-1", "wf-1", history.StatusRunning)
	setCurrent(t, store, "", "wf-1", "run-1")

	executiontest.UpdateRun(t, store.Executions, "run-1", func(run *execution.Run) error {
		return run.MarkTerminal(history.StatusCompleted, nil)
	})

	// status 已更新。
	got, _ := store.Executions.Get("run-1")
	if got.Status != history.StatusCompleted {
		t.Fatalf("status not updated: got %q, want completed", got.Status)
	}
	// is_current 仍 true。
	if !got.IsCurrent {
		t.Fatal("UpdateRun must NOT clear is_current")
	}
	// marker 仍存在：GetCurrentRunID 仍能查到 run-1。
	cur, err := store.Executions.GetCurrentRunID("", "wf-1")
	if err != nil || cur != "run-1" {
		t.Fatalf("current lost after UpdateRun: cur=%q err=%v", cur, err)
	}
}

// assertSingleCurrent 断言一个 workflow 至多一个 is_current run。
func assertSingleCurrent(t *testing.T, store *Store, workflowID string) {
	t.Helper()
	res, err := store.Executions.List(execution.StoreListFilter{WorkflowID: workflowID, IncludeTerminal: true, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, r := range res.Runs {
		if r.IsCurrent {
			count++
		}
	}
	if count > 1 {
		t.Fatalf("workflow %s has %d current runs, want at most 1", workflowID, count)
	}
}
