package persistence

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
)

// ============================================
// MySQL 真并发测试（只在 TENUTO_TEST_MYSQL=1 下跑）
// ============================================
// 连接池留足连接让 goroutine 物理并行，SELECT ... FOR UPDATE 是 InnoDB 行锁，
// 本文件在这两个前提下验证 TOCTOU 防护。
//
// 死锁观察：InnoDB 行锁未命中索引会升级为间隙锁乃至表锁 → 生产死锁。
// 各用例统一用 isDeadlockErr 甄别并 t.Errorf 显性报告，不吞掉。
// ============================================

// isDeadlockErr 识别 InnoDB 死锁 / 锁等待超时。
func isDeadlockErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadlock found when trying to get lock") ||
		strings.Contains(msg, "Error 1213") ||
		strings.Contains(msg, "Lock wait timeout exceeded") ||
		strings.Contains(msg, "Error 1205")
}

// TestMySQLConcurrentAppend_NodeIDConvergence 验证 nextNodeID 的无锁
// SELECT MAX(node_id)+1 在 MySQL REPEATABLE READ 下的收敛性。
//
// 风险：REPEATABLE READ 下并发事务可能读到同一个 MAX，各自 +1 得到相同 node_id。
// 设计依赖 (branch_id,node_id) 唯一索引把静默重复变成 duplicate-key，再由
// retryOnDuplicatedKey 重开事务重读 MAX 收敛。本用例检验该收敛是否真的成立、
// 重试次数（5）是否够用、以及会不会退化成死锁。
func TestMySQLConcurrentAppend_NodeIDConvergence(t *testing.T) {
	store := requireMySQL(t)
	createTestRun(t, store, "run-conc-node")

	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for i := range workers {
		wg.Go(func() {
			ev := history.Event{Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{
				Name: fmt.Sprintf("sig-%d", i),
			}}
			_, err := store.Executions.Append("run-conc-node", "run-conc-node", "run-conc-node", ev)
			errs[i] = err
		})
	}
	wg.Wait()

	okCount := 0
	for i, err := range errs {
		switch {
		case err == nil:
			okCount++
		case isDeadlockErr(err):
			t.Errorf("worker %d hit InnoDB deadlock/lock-timeout (真问题，非测试瑕疵): %v", i, err)
		case strings.Contains(err.Error(), "node_id conflict persisted"):
			t.Errorf("worker %d retries did not converge with %d concurrent appends: %v", i, workers, err)
		default:
			t.Errorf("worker %d append failed: %v", i, err)
		}
	}

	history, err := store.Executions.GetEvents("run-conc-node")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != okCount {
		t.Errorf("history 条数 = %d, 成功 append = %d（丢事件或重复落库）", len(history), okCount)
	}
	// 核心不变式：node_id 无重复且严格单调递增。
	seen := make(map[int64]bool, len(history))
	var prev int64
	for i := range history {
		id := history[i].ID
		if seen[id] {
			t.Errorf("node_id %d 重复——唯一索引 + 重试未能收敛", id)
		}
		seen[id] = true
		if i > 0 && id <= prev {
			t.Errorf("node_id 非单调: history[%d].ID=%d <= 前一条 %d", i, id, prev)
		}
		prev = id
	}
	t.Logf("并发 %d 路 append：成功 %d，最终 history %d 条，node_id 唯一且单调",
		workers, okCount, len(history))
}

// TestMySQLConcurrentSetCurrentRunCAS_FirstSet 验证 SetCurrentRunCAS 首设路径。
//
// 风险：首设走 Count + Update（store.go:667-678）无锁，存在 TOCTOU 窗口——
// 多个事务可能同时 Count 到 0 都认为"可以设"，靠 current_marker 唯一索引兜底。
// 本用例并发首设同一 workflow 的不同 run，断言最终**恰好一个**成为 current。
func TestMySQLConcurrentSetCurrentRunCAS_FirstSet(t *testing.T) {
	store := requireMySQL(t)

	const workers = 8
	const wfID = "wf-cas-first"
	runIDs := make([]string, workers)
	for i := range workers {
		runIDs[i] = fmt.Sprintf("run-cas-%d", i)
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			return tx.CreateRun(rootBranch(t, &execution.Run{
				ID: runIDs[i], Namespace: "default", WorkflowID: wfID,
				Status: history.StatusRunning, CreatedAt: time.Now(),
			}))
		}); err != nil {
			t.Fatal(err)
		}
	}

	var wonCount atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for i := range workers {
		wg.Go(func() {
			err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
				ok, casErr := tx.SetCurrentRunCAS("default", wfID, "", runIDs[i])
				if casErr != nil {
					return casErr
				}
				if ok {
					wonCount.Add(1)
				}
				return nil
			})
			errs[i] = err
		})
	}
	wg.Wait()

	for i, err := range errs {
		if isDeadlockErr(err) {
			t.Errorf("worker %d hit InnoDB deadlock/lock-timeout (真问题): %v", i, err)
		} else if err != nil {
			t.Errorf("worker %d CAS tx failed: %v", i, err)
		}
	}

	// DB 层不变式：该 workflow 至多一个 is_current run。
	var currentCount int64
	if err := store.db.Model(&executionrepo.RunPO{}).
		Where("namespace = ? AND workflow_id = ? AND is_current = ?", "default", wfID, true).
		Count(&currentCount).Error; err != nil {
		t.Fatal(err)
	}
	if currentCount != 1 {
		t.Errorf("is_current run 数 = %d, want 1——首设 TOCTOU 未被唯一索引兜住", currentCount)
	}
	if got := wonCount.Load(); got != 1 {
		t.Errorf("CAS 返回 true 的次数 = %d, want 1（多个 winner 意味着 CAS 不是真 CAS）", got)
	}
	t.Logf("并发 %d 路首设 CAS：winner=%d，DB is_current=%d", workers, wonCount.Load(), currentCount)
}

// TestMySQLConcurrentAppendWorkflowTaskScheduled_SingleOpenWFT 验证
// "一个 run 同时只有一个 open WFT"（靠 dedupe_key 唯一索引）。
//
// 风险：AppendWorkflowTaskScheduled 先查 openWorkflowTaskScheduledID 再插入，
// 查与插之间有窗口；并发下多路可能都查到"无 open WFT"而各自插入。
func TestMySQLConcurrentAppendWorkflowTaskScheduled_SingleOpenWFT(t *testing.T) {
	store := requireMySQL(t)
	createTestRun(t, store, "run-wft")

	const workers = 12
	var wg sync.WaitGroup
	ids := make([]int64, workers)
	inserts := make([]bool, workers)
	errs := make([]error, workers)

	for i := range workers {
		wg.Go(func() {
			ev := history.Event{Timestamp: time.Now(), WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}}
			id, inserted, err := store.Executions.AppendWorkflowTaskScheduled("run-wft", "run-wft", "run-wft", ev)
			ids[i], inserts[i], errs[i] = id, inserted, err
		})
	}
	wg.Wait()

	insertedCount := 0
	for i, err := range errs {
		if isDeadlockErr(err) {
			t.Errorf("worker %d hit InnoDB deadlock/lock-timeout (真问题): %v", i, err)
			continue
		}
		if err != nil {
			t.Errorf("worker %d AppendWorkflowTaskScheduled failed: %v", i, err)
			continue
		}
		if inserts[i] {
			insertedCount++
		}
	}
	if insertedCount != 1 {
		t.Errorf("inserted=true 次数 = %d, want 1——同一 run 出现多个 open WFT", insertedCount)
	}

	// 所有调用应收敛到同一个 scheduledEventID。
	var first int64
	for i, id := range ids {
		if errs[i] != nil {
			continue
		}
		if first == 0 {
			first = id
			continue
		}
		if id != first {
			t.Errorf("worker %d 返回 eventID=%d, 与首个 %d 不一致——未收敛到同一 open WFT", i, id, first)
		}
	}

	// 落库层面 WorkflowTaskScheduled 事件只应有一条。
	var scheduledRows int64
	if err := store.db.Model(&executionrepo.EventPO{}).
		Where("run_id = ? AND type = ?", "run-wft", string(history.EventWorkflowTaskScheduled)).
		Count(&scheduledRows).Error; err != nil {
		t.Fatal(err)
	}
	if scheduledRows != 1 {
		t.Errorf("WorkflowTaskScheduled 落库 %d 条, want 1", scheduledRows)
	}
	t.Logf("并发 %d 路 schedule WFT：inserted=%d，落库 %d 条，全部收敛到 eventID=%d",
		workers, insertedCount, scheduledRows, first)
}

// TestMySQLConcurrentActivityStateForUpdate 验证 GetActivityStateForUpdate 的
// SELECT ... FOR UPDATE 真行锁。
//
// 模式：并发"读 + 校验 + 写"递增心跳里的计数。若行锁生效，事务串行化，最终
// Attempt 恰好等于并发数；若锁失效，会出现丢失更新。
func TestMySQLConcurrentActivityStateForUpdate(t *testing.T) {
	store := requireMySQL(t)
	createTestRun(t, store, "run-act-lock")

	const scheduledEventID int64 = 1
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.UpsertActivityState(&execution.ActivityState{
			RunID: "run-act-lock", ScheduledEventID: scheduledEventID,
			Status: execution.ActivityStatusEnums.Started, Attempt: 1,
			ScheduledTime: time.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 10
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for i := range workers {
		wg.Go(func() {
			errs[i] = store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
				st, ok, err := tx.GetActivityStateForUpdate("run-act-lock", scheduledEventID)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("activity state missing")
				}
				// 放大读改写窗口：无行锁时必然丢失更新。
				time.Sleep(2 * time.Millisecond)
				// 写入走领域函数：state 更新只落 setter 跟踪到的字段，直接改字段不会写库。
				seen, _ := strconv.Atoi(fmt.Sprint(st.LastHeartbeat["n"]))
				recorded, err := execution.RecordActivityHeartbeat(tx, execution.ActivityTransition{
					RunID: "run-act-lock", TreeID: "run-act-lock", BranchID: "run-act-lock",
					ScheduledEventID: scheduledEventID, Attempt: 1, Now: time.Now(),
				}, map[string]any{"n": strconv.Itoa(seen + 1)})
				if err != nil {
					return err
				}
				if recorded == nil {
					return fmt.Errorf("heartbeat was not recorded")
				}
				return nil
			})
		})
	}
	wg.Wait()

	applied := 0
	for i, err := range errs {
		switch {
		case err == nil:
			applied++
		case isDeadlockErr(err):
			t.Errorf("worker %d hit InnoDB deadlock/lock-timeout (真问题): %v", i, err)
		default:
			t.Errorf("worker %d tx failed: %v", i, err)
		}
	}

	var final *execution.ActivityState
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		st, ok, err := tx.GetActivityStateForUpdate("run-act-lock", scheduledEventID)
		if err != nil || !ok {
			return fmt.Errorf("read back: ok=%v err=%v", ok, err)
		}
		final = st
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	counted, _ := strconv.Atoi(fmt.Sprint(final.LastHeartbeat["n"]))
	if counted != applied {
		t.Errorf("heartbeat 计数 = %d, want %d（丢失更新——FOR UPDATE 行锁未生效）", counted, applied)
	}
	t.Logf("并发 %d 路读改写 activity state：成功 %d，最终计数=%d（无丢失更新）",
		workers, applied, counted)
}

// TestMySQLConcurrentTimerStateForUpdate 同上，覆盖 GetTimerStateForUpdate。
// TimerFired 在 RunInTx 内"读 + 校验非终态 + 写"，行锁失效会导致重复 fire。
func TestMySQLConcurrentTimerStateForUpdate(t *testing.T) {
	store := requireMySQL(t)
	createTestRun(t, store, "run-timer-lock")

	const scheduledEventID int64 = 1
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.UpsertTimerState(&execution.TimerState{
			RunID: "run-timer-lock", ScheduledEventID: scheduledEventID,
			Kind:   execution.TimerKindEnums.UserTimer,
			Status: execution.TimerStatusEnums.Scheduled, Deadline: time.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 10
	var firedCount atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for i := range workers {
		wg.Go(func() {
			errs[i] = store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
				st, ok, err := tx.GetTimerStateForUpdate("run-timer-lock", scheduledEventID)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("timer state missing")
				}
				// 校验非终态：只有第一个观察到 Scheduled 的事务能 fire。
				if st.Status != execution.TimerStatusEnums.Scheduled {
					return nil
				}
				time.Sleep(2 * time.Millisecond)
				// 写入走领域函数：state 更新只落 setter 跟踪到的字段，直接改字段不会写库。
				fired, err := execution.FireTimer(tx, execution.TimerFire{
					RunID: "run-timer-lock", TreeID: "run-timer-lock", BranchID: "run-timer-lock",
					ScheduledEventID: scheduledEventID, Kind: execution.TimerKindEnums.UserTimer, Now: time.Now(),
				})
				if err != nil {
					return err
				}
				if fired {
					firedCount.Add(1)
				}
				return nil
			})
		})
	}
	wg.Wait()

	for i, err := range errs {
		if isDeadlockErr(err) {
			t.Errorf("worker %d hit InnoDB deadlock/lock-timeout (真问题): %v", i, err)
		} else if err != nil {
			t.Errorf("worker %d tx failed: %v", i, err)
		}
	}
	if got := firedCount.Load(); got != 1 {
		t.Errorf("fire 次数 = %d, want 1（重复 fire——FOR UPDATE 行锁未生效）", got)
	}
	t.Logf("并发 %d 路 timer fire：实际 fire %d 次（无重复 fire）", workers, firedCount.Load())
}

// TestMySQLRunReadUnderLockContract 在 MySQL 的真并发事务上运行 ExecutionTx.GetRunForUpdate 的契约：第二个事务在第一个
// 提交之前开始，在 execution state 行锁上等待，拿到锁后读到第一个事务提交的 Run 字段；并发的读改写一个都不丢。
// REPEATABLE READ 下第二个事务的一致性读快照建立在取得行锁之后。
func TestMySQLRunReadUnderLockContract(t *testing.T) {
	requireMySQL(t)
	executiontest.RunReadUnderLockContract(t, func(t *testing.T) execution.ExecutionManager { return requireMySQL(t).Executions })
}

// TestMySQLRunReadIsFreshAfterEarlierRead 验证 GetRunForUpdate 读到已提交的最新值，与本事务的一致性读快照
// 何时建立无关：事务先做别的读写，另一个事务随后提交了对 Run 的修改，本事务再读 Run 时看到这次修改。
func TestMySQLRunReadIsFreshAfterEarlierRead(t *testing.T) {
	manager := requireMySQL(t).Executions
	newRun := func(id string) *execution.Run {
		run := execution.NewRun(execution.NewRunOptions{
			ID: id, WorkflowID: "wf-" + id, WorkflowConfigVersion: "v1",
			CreatedAt: time.Now(), BranchToken: executiontest.BranchToken(id),
		})
		if err := run.ApplyStartPolicy(execution.RunPolicyAutoUpgrade, ""); err != nil {
			t.Fatal(err)
		}
		return run
	}
	const runID = "run-fresh-after-earlier-read"
	if err := manager.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(newRun(runID)) }); err != nil {
		t.Fatal(err)
	}

	err := manager.RunInTx(func(tx execution.ExecutionTx) error {
		// 先读一次这个 Run（普通读路径）并新建另一个 Run，让本事务在取 runID 的锁之前就有了读写。
		if _, err := manager.Get(runID); err != nil {
			return err
		}
		if err := tx.CreateRun(newRun("run-fresh-other")); err != nil {
			return err
		}
		if _, err := tx.GetWorkflowExecutionStateForUpdate("run-fresh-other"); err != nil {
			return err
		}
		executiontest.UpdateRun(t, manager, runID, func(run *execution.Run) error {
			return run.OverridePolicy(execution.RunPolicyPinned)
		})
		got, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		if got.Policy != execution.RunPolicyPinned {
			t.Errorf("policy read in the tx = %q, want %q committed by the other transaction", got.Policy, execution.RunPolicyPinned)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
