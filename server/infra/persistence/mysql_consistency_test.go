package persistence

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func startTestRunWithHistory(t *testing.T, store *Store, runID, workflowID string) *execution.Run {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{
		ID: runID, WorkflowID: workflowID, WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: time.Now(),
	})
	started := historypkg.NewWorkflowStartedEvent(nil, runID, "", workflowID, "Workflow", 0)
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		if err := tx.CreateRun(run); err != nil {
			return err
		}
		_, err := tx.Append(runID, runID, runID, started)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return run
}

// lockOrderRace 先让事务 B 取到 Run 的 execution state 行锁，再启动事务 A，等 A 排上锁之后 B 继续执行。
// 两边按同一顺序（先 execution state、后 activity / timer 行）加锁时 A 只是等 B 提交，两个事务都成功；
// 顺序相反时 InnoDB 检测到死锁并回滚其中一个（Error 1213）。
func lockOrderRace(t *testing.T, store *Store, runID string, txA func(tx execution.ExecutionTx) error, txB func(tx execution.ExecutionTx, run *execution.Run) error) {
	t.Helper()
	stateLocked := make(chan struct{})
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			close(stateLocked)
			time.Sleep(300 * time.Millisecond) // 让事务 A 先排上锁
			return txB(tx, run)
		})
	}()
	<-stateLocked
	go func() { errA <- store.Executions.RunInTx(txA) }()
	for name, ch := range map[string]chan error{"A": errA, "B": errB} {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("事务 %s 失败: %v", name, err)
			}
		case <-time.After(90 * time.Second):
			t.Fatalf("事务 %s 没有返回", name)
		}
	}
}

// Worker 上报 Activity 完成（execution.CompleteActivity）与取消这个 Activity 的 Workflow Task 提交并发。
func TestActivityCompletionAndCancelCommandLockInSameOrder(t *testing.T) {
	store := requireMySQL(t)
	const runID = "run-lock-order-activity"
	startTestRunWithHistory(t, store, runID, "wf-lock-order-activity")
	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := store.Executions.UpsertActivityState(&execution.ActivityState{
		RunID: runID, ScheduledEventID: scheduledID, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started, StartedEventID: execution.TransientEventID,
		ScheduledTime: now, StartedTime: &now, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	lockOrderRace(t, store, runID, func(tx execution.ExecutionTx) error {
		_, err := execution.CompleteActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: scheduledID, Attempt: 1, Stamp: 1, Now: time.Now(),
		}, map[string]any{"ok": true})
		return err
	}, func(tx execution.ExecutionTx, run *execution.Run) error {
		// 与 completeWorkflowTaskWithStore 的顺序相同：先 GetRunForUpdate，再 CommitWorkflowTask。
		_, err := execution.CommitWorkflowTask(tx, execution.WorkflowTaskCommit{
			Run: run, TreeID: runID, BranchID: runID,
			CloseEvent:    historypkg.NewWorkflowTaskCompletedEvent(1, "worker-1"),
			Commands:      []historypkg.Command{{Type: historypkg.CmdRequestCancelActivity, ActivityID: "charge"}},
			CommandEvents: []historypkg.Event{historypkg.NewActivityCancelRequestedEvent("charge", 0)},
			Now:           time.Now(),
		})
		return err
	})
}

// 用户 timer 触发（execution.FireTimer）与终止 Run（关闭未触发的 timer）并发。
func TestTimerFireAndRunCloseLockInSameOrder(t *testing.T) {
	store := requireMySQL(t)
	const runID = "run-lock-order-timer"
	startTestRunWithHistory(t, store, runID, "wf-lock-order-timer")
	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.Event{TimerScheduled: &historypkg.TimerScheduledEvent{TimerID: "t1"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := store.Executions.UpsertTimerState(&execution.TimerState{
		RunID: runID, ScheduledEventID: scheduledID, TimerID: "t1", Kind: execution.TimerKindEnums.UserTimer,
		Deadline: now, Status: execution.TimerStatusEnums.Scheduled, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	lockOrderRace(t, store, runID, func(tx execution.ExecutionTx) error {
		_, err := execution.FireTimer(tx, execution.TimerFire{
			RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: scheduledID, Kind: execution.TimerKindEnums.UserTimer, Now: time.Now(),
		})
		return err
	}, func(tx execution.ExecutionTx, run *execution.Run) error {
		_, err := execution.Terminate(tx, run, runID, runID, "terminated")
		return err
	})
}

// 已结束的 Run 不清 open Workflow Task 与 sticky 投影：结束时 WFT 已 started 的 Run 永久满足
// ListExpiredStickyExecutions 的条件，回落扫描对它只 continue、不清除。扫描按 run_id 升序取前 limit 行，
// Run ID 是 UUIDv7（按时间递增），这些旧 Run 排在最前，积累到 limit（256）行之后扫描结果里只剩它们。
func TestClosedRunsLeaveStickyFallbackScan(t *testing.T) {
	store := requireMySQL(t)
	const limit = 3
	expires := time.Now().Add(-time.Minute)
	prepare := func(runID string, startWFT bool) {
		startTestRunWithHistory(t, store, runID, "wf-"+runID)
		scheduledID, _, err := store.Executions.AppendWorkflowTaskScheduled(runID, runID, runID, historypkg.NewWorkflowTaskScheduledEvent(0))
		if err != nil {
			t.Fatal(err)
		}
		if startWFT {
			if _, _, err := store.Executions.AppendIdempotent(runID, runID, runID, historypkg.NewWorkflowTaskStartedEvent(scheduledID, "worker-1", "build-1", 0)); err != nil {
				t.Fatal(err)
			}
		}
		state, err := store.Executions.GetWorkflowExecutionState(runID)
		if err != nil {
			t.Fatal(err)
		}
		if _, assigned, err := store.Executions.AssignStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration, "worker-1", "build-1", expires); err != nil || !assigned {
			t.Fatalf("assign sticky: %v %v", assigned, err)
		}
	}
	// limit 个 Run 在 WFT 执行期间被终止（Terminate API、Workflow 超时等）。
	for i := range limit {
		runID := fmt.Sprintf("run-sticky-0%d-closed", i)
		prepare(runID, true)
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			_, err = execution.Terminate(tx, run, runID, runID, "terminated")
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 之后的一个运行中的 Run：sticky Worker 已下线，WFT 等着回落。
	const live = "run-sticky-99-live"
	prepare(live, false)

	states, err := store.Executions.ListExpiredStickyExecutions(time.Now(), limit)
	if err != nil {
		t.Fatal(err)
	}
	foundLive := false
	for _, state := range states {
		if state.IsTerminal() {
			t.Errorf("回落扫描返回了已结束的 Run %s（status=%s）", state.RunID, state.Status)
		}
		foundLive = foundLive || state.RunID == live
	}
	if !foundLive {
		t.Errorf("需要回落的运行中 Run %s 不在扫描结果里：limit 被已结束的 Run 占满", live)
	}
}

// Run 结束时只关 timer state（CloseScheduledTimers），Activity state 保持 scheduled / started。
// ListRecoveryRunIDs 的两条 Activity 查询从 idx_activity_states_status 读出这些行再逐行关联 tenuto_runs 过滤，
// 每 30 秒一轮，代价随「结束时还有未完成 Activity 的 Run」的历史累计数线性增长（保留期关闭时永不回落）。
func TestClosedRunClosesOpenActivityStates(t *testing.T) {
	store := requireMySQL(t)
	const runs = 50
	now := time.Now()
	started := now.Add(-time.Hour)
	for i := range runs {
		runID := fmt.Sprintf("run-leftover-%03d", i)
		startTestRunWithHistory(t, store, runID, "wf-"+runID)
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			for eventID, status := range map[int64]execution.ActivityStatus{100: execution.ActivityStatusEnums.Scheduled, 101: execution.ActivityStatusEnums.Started} {
				state := &execution.ActivityState{
					RunID: runID, ScheduledEventID: eventID, ActivityID: fmt.Sprint("a", eventID), ActivityType: "charge",
					Attempt: 1, Stamp: 1, Status: status, ScheduledTime: started, StartToCloseTimeoutMs: 1000,
					CreatedAt: started, UpdatedAt: started,
				}
				if status == execution.ActivityStatusEnums.Started {
					state.StartedTime = &started
				}
				if err := tx.UpsertActivityState(state); err != nil {
					return err
				}
			}
			run, err := tx.GetRunForUpdate(runID)
			if err != nil {
				return err
			}
			_, err = execution.Terminate(tx, run, runID, runID, "terminated")
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := store.Executions.ListRecoveryRunIDs(now)
	if err != nil || len(ids) != 0 {
		t.Fatalf("recovery candidates = %v, %v", ids, err)
	}
	var leftover int64
	if err := store.db.Raw(`SELECT COUNT(*) FROM tenuto_activity_states a JOIN tenuto_runs r ON r.id = a.run_id
		WHERE a.status IN ('scheduled','started') AND r.status <> 'Running'`).Scan(&leftover).Error; err != nil {
		t.Fatal(err)
	}
	type explainRow struct {
		Explain string `gorm:"column:EXPLAIN"`
	}
	var plan []explainRow
	if err := store.db.Raw(`EXPLAIN ANALYZE SELECT DISTINCT a.run_id FROM tenuto_activity_states a
		INNER JOIN tenuto_runs r ON r.id = a.run_id AND r.status = 'Running'
		WHERE a.status = 'started' AND a.timeout_at < ?`, now).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range plan {
		t.Logf("ListRecoveryRunIDs 的 Started Activity 查询在 %d 个已结束 Run 上的执行计划:\n%s", runs, row.Explain)
	}
	if leftover != 0 {
		t.Errorf("%d 个已结束的 Run 留下 %d 条 scheduled/started 的 activity state；周期补偿每轮都要读出它们再逐行关联 Run 过滤", runs, leftover)
	}
}

// activity state 的时间列是 DATETIME(3)，MySQL 写入时把亚毫秒部分四舍五入；进程内的超时 timer 按纳秒精度的
// started+timeout 到期。约一半的 attempt 的持久化截止时间比进程内 timer 晚（最多 0.5ms）：timer 触发后
// execution.TimeoutActivity 在锁下按持久化截止时间复核，判定「尚未到期」并原样返回，这个进程内 timer 已消耗、不再重挂，
// 超时改由周期补偿在 grace（30s）之后接管。触发到复核之间有 3 次查询的延迟，库在本机或同机房低延迟网络时落进这个窗口。
func TestActivityTimeoutSurvivesSubMillisecondRounding(t *testing.T) {
	store := requireMySQL(t)
	const runID = "run-rounding"
	startTestRunWithHistory(t, store, runID, "wf-rounding")
	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Truncate(time.Millisecond).Add(600 * time.Microsecond) // 亚毫秒部分 0.6ms
	const timeout = time.Second
	if err := store.Executions.UpsertActivityState(&execution.ActivityState{
		RunID: runID, ScheduledEventID: scheduledID, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Started, StartedEventID: execution.TransientEventID,
		ScheduledTime: started, StartedTime: &started, StartToCloseTimeoutMs: timeout.Milliseconds(),
		CreatedAt: started, UpdatedAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	persisted, _, err := store.Executions.GetActivityState(runID, scheduledID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("写入 started_time=%s，读回 %s（偏移 %s）", started.Format("15:04:05.000000"), persisted.StartedTime.UTC().Format("15:04:05.000000"), persisted.StartedTime.Sub(started))

	// 进程内 timer 的到期时刻（startActivityAttempt：s.timers.Arm(ref, now.Add(timeout))），加 300µs 的处理延迟。
	firedAt := started.Add(timeout).Add(300 * time.Microsecond)
	var result execution.ActivityFailureResult
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = execution.TimeoutActivity(tx, execution.ActivityTransition{
			RunID: runID, TreeID: runID, BranchID: runID, ScheduledEventID: scheduledID, Attempt: 1, Stamp: 1, Now: firedAt,
		}, protocol.TimeoutTypeStartToClose)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !result.Applied {
		t.Errorf("StartToClose 超时在进程内截止时间之后 300µs 触发，被持久化截止时间（四舍五入后晚 %s）判为未到期而丢弃", persisted.StartedTime.Sub(started))
	}
}

// 仓储用 RowsAffected 区分「INSERT 新写入」与「撞唯一键没有写入」（appendEvent 的去重、insertOrPatch 的 upsert）。
// DSN 带 clientFoundRows=true 时，MySQL 对 ON DUPLICATE KEY UPDATE col = col 的撞键返回 1：去重命中被当成新写入。
// ConfigureMySQL 把它固定为 false：DSN 里写成 true 也不生效，去重与 upsert 照常工作。
func TestClientFoundRowsIsForcedOff(t *testing.T) {
	base := requireMySQL(t)
	var dbName string
	if err := base.db.Raw("SELECT DATABASE()").Scan(&dbName).Error; err != nil {
		t.Fatal(err)
	}
	cfg, err := mysqldriver.ParseDSN(os.Getenv(mysqltest.DSNEnvVar))
	if err != nil {
		t.Skipf("需要 %s 才能另开一个带 clientFoundRows=true 的连接: %v", mysqltest.DSNEnvVar, err)
	}
	cfg.DBName = dbName
	cfg.ClientFoundRows = true
	cfg.Loc = time.UTC
	if err := infradb.ConfigureMySQL(cfg); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if pool, err := db.DB(); err == nil {
			_ = pool.Close()
		}
	})
	store, err := New(db, Config{})
	if err != nil {
		t.Fatalf("ConfigureMySQL 之后的句柄被拒绝: %v", err)
	}

	const runID = "run-found-rows"
	startTestRunWithHistory(t, store, runID, "wf-found-rows")
	signal := func() (int64, bool) {
		event := historypkg.NewWorkflowSignaledEvent("pay", nil)
		event.WorkflowSignaled.RequestID = "req-1"
		id, inserted, err := store.Executions.AppendIdempotent(runID, runID, runID, event)
		if err != nil {
			t.Fatal(err)
		}
		return id, inserted
	}
	firstID, _ := signal()
	secondID, secondInserted := signal() // 同一个 requestID 的重发
	events, err := store.Executions.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Executions.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if secondInserted || secondID != firstID || state.LastEventID != events[len(events)-1].ID || state.HistoryEventCount != int64(len(events)) {
		t.Errorf("重发的 Signal 被当成新事件：inserted=%v，事件 ID %d→%d；History 实际 %d 条、末事件 ID %d，execution state 记为 %d 条、LastEventID=%d（指向不存在的事件）",
			secondInserted, firstID, secondID, len(events), events[len(events)-1].ID, state.HistoryEventCount, state.LastEventID)
	}

	// insertOrPatch：行已存在时把撞键当成「已插入」，patch 不再写入。
	now := time.Now()
	child := &execution.Run{ID: "run-found-rows-child", ParentRunID: runID, ParentChildID: "c1", ParentScheduledEventID: 50, WorkflowType: "child", WorkflowConfigVersion: "v1"}
	if err := store.Executions.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID: runID, ScheduledEventID: 50, ChildID: "c1", ChildRunID: child.ID, Status: execution.ChildWorkflowStatusEnums.Started, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		_, err := execution.PropagateChildClose(tx, execution.ChildClose{
			Child: child, ParentTreeID: runID, ParentBranchID: runID, Type: historypkg.EventChildWorkflowCompleted, Now: now,
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	link, _, err := store.Executions.GetChildWorkflowState(runID, 50)
	if err != nil {
		t.Fatal(err)
	}
	if !link.IsTerminal() {
		t.Errorf("子 Workflow 关闭已写进父 History，但 ChildWorkflowState 仍是 %s：upsert 的 patch 没有写入，全量补偿会反复补传播", link.Status)
	}
}

// 不经过 ConfigureMySQL、带 clientFoundRows=true 打开的句柄（app.WithDatabase 注入）由 New 的探测拒绝。
func TestNewRejectsClientFoundRowsHandle(t *testing.T) {
	base := requireMySQL(t)
	var dbName string
	if err := base.db.Raw("SELECT DATABASE()").Scan(&dbName).Error; err != nil {
		t.Fatal(err)
	}
	cfg, err := mysqldriver.ParseDSN(os.Getenv(mysqltest.DSNEnvVar))
	if err != nil {
		t.Skipf("需要 %s 才能另开一个带 clientFoundRows=true 的连接: %v", mysqltest.DSNEnvVar, err)
	}
	cfg.DBName = dbName
	cfg.ParseTime = true
	cfg.ClientFoundRows = true
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if pool, err := db.DB(); err == nil {
			_ = pool.Close()
		}
	})
	if _, err := New(db, Config{}); err == nil || !strings.Contains(err.Error(), "clientFoundRows") {
		t.Fatalf("New 接受了带 clientFoundRows=true 的句柄: %v", err)
	}
	var probes int64
	if err := base.db.Raw("SELECT COUNT(*) FROM tenuto_deletion_tasks").Scan(&probes).Error; err != nil || probes != 0 {
		t.Fatalf("探测行留在表里: %d, %v", probes, err)
	}
}

// InnoDB 选中的死锁牺牲者由 inTx 用新事务把整个闭包再执行一次，两个事务都成功。两个事务按相反顺序加锁，
// 只在第一次执行时互相等待，构造出一次确定的死锁。
func TestRunInTxRetriesDeadlockVictim(t *testing.T) {
	store := requireMySQL(t)
	const runID = "run-deadlock-retry"
	startTestRunWithHistory(t, store, runID, "wf-deadlock-retry")
	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "charge", ActivityType: "charge"}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := store.Executions.UpsertActivityState(&execution.ActivityState{
		RunID: runID, ScheduledEventID: scheduledID, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	var attemptsA, attemptsB atomic.Int32
	activityLocked := make(chan struct{})
	stateLocked := make(chan struct{})
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() {
		errA <- store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			first := attemptsA.Add(1) == 1
			if _, _, err := tx.GetActivityStateForUpdate(runID, scheduledID); err != nil {
				return err
			}
			if first {
				close(activityLocked)
				<-stateLocked
			}
			_, err := tx.Append(runID, runID, runID, historypkg.NewWorkflowSignaledEvent("a", nil))
			return err
		})
	}()
	go func() {
		errB <- store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			first := attemptsB.Add(1) == 1
			if _, err := tx.GetRunForUpdate(runID); err != nil {
				return err
			}
			if first {
				<-activityLocked
				close(stateLocked)
				time.Sleep(300 * time.Millisecond) // 让 A 排到 execution state 行锁上
			}
			_, _, err := tx.GetActivityStateForUpdate(runID, scheduledID)
			if err != nil {
				return err
			}
			_, err = tx.Append(runID, runID, runID, historypkg.NewWorkflowSignaledEvent("b", nil))
			return err
		})
	}()
	for name, ch := range map[string]chan error{"A": errA, "B": errB} {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("事务 %s 失败: %v", name, err)
			}
		case <-time.After(90 * time.Second):
			t.Fatalf("事务 %s 没有返回", name)
		}
	}
	if total := attemptsA.Load() + attemptsB.Load(); total != 3 {
		t.Errorf("两个闭包共执行 %d 次（A=%d B=%d），期望死锁牺牲者重放一次共 3 次", total, attemptsA.Load(), attemptsB.Load())
	}
	events, err := store.Executions.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	signals := 0
	for _, event := range events {
		if event.WorkflowSignaled != nil {
			signals++
		}
	}
	if signals != 2 {
		t.Errorf("History 里有 %d 条 Signal，期望两个事务各 1 条", signals)
	}
}
