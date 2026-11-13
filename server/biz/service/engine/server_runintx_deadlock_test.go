package engine_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"

	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newSingleConnStore 在独立 MySQL 库上建 GORM Store，连接池只留一条连接。没有开启 MySQL 后端测试时跳过 t。
// RunInTx 占住这条连接后，闭包内任何走非事务句柄的读都等不到连接而自我死锁；
// 连接池耗尽的生产实例上是同一种死锁。
func newSingleConnStore(t *testing.T) *persistence.Store {
	t.Helper()
	db := mysqltest.NewDB(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	store, err := persistence.New(db, persistence.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// Integration（死锁回归）：连接池只有一条连接时，RunInTx 经 db.Transaction 占用它，
// 闭包内任何走「新连接」的非事务读（runBranchInfo → mgr.Get → s.db）都会永久阻塞，
// 形成自我死锁。本测试驱动 HandleActivityCompleted（其 RunInTx 闭包内原本调 runBranchInfo），
// 用 channel + select 超时守护：修复前该路径在事务内死锁，超时即 Fatal；修复后秒级返回。
func TestServerHandleActivityCompletedNoDeadlockUnderSingleConn(t *testing.T) {
	store := newSingleConnStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})

	const runID = "run-deadlock-completed"
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "deadlock-wf"})
	run.Status = historypkg.StatusRunning
	run.CreatedAt = time.Now()
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "step-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	if err := store.Executions.UpsertActivityState(&execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: scheduledID,
		ActivityID:       "step-1",
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   scheduledID, // 已写 history 的真实 started（非 transient）
		Status:           execution.ActivityStatusEnums.Started,
		ScheduledTime:    time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- server.HandleActivityCompleted(context.Background(), runID, scheduledID, 1, 1, map[string]any{"ok": true})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleActivityCompleted returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HandleActivityCompleted deadlocked under MaxOpenConns(1): RunInTx closure called runBranchInfo (non-tx Get) while holding the only connection")
	}

	history, err := store.Executions.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countCompletedEvents(history) != 1 {
		t.Fatalf("expected exactly one ActivityCompleted event, got history=%+v", history)
	}
}

// Integration（死锁回归）：HandleActivityFailed 的 RunInTx 闭包内同样调 runBranchInfo，
// 且 backfillTransientStarted 也内嵌 runBranchInfo。覆盖终态失败路径不死锁。
func TestServerHandleActivityFailedNoDeadlockUnderSingleConn(t *testing.T) {
	store := newSingleConnStore(t)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})

	const runID = "run-deadlock-failed"
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "deadlock-wf"})
	run.Status = historypkg.StatusRunning
	run.CreatedAt = time.Now()
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	scheduledID, err := store.Executions.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "step-1"}))
	if err != nil {
		t.Fatal(err)
	}
	// StartedEventID = TransientEventID 触发 backfillTransientStarted（其内部也调 runBranchInfo）。
	if err := store.Executions.UpsertActivityState(&execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: scheduledID,
		ActivityID:       "step-1",
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   execution.TransientEventID,
		Status:           execution.ActivityStatusEnums.Started,
		ScheduledTime:    time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- server.HandleActivityFailed(context.Background(), runID, scheduledID, 1, 1, protocol.Failure{Message: "boom"})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleActivityFailed returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HandleActivityFailed deadlocked under MaxOpenConns(1): RunInTx closure called runBranchInfo (non-tx Get) while holding the only connection")
	}
}

func countCompletedEvents(history []historypkg.Event) int {
	n := 0
	for i := range history {
		if history[i].EventType() == historypkg.EventActivityCompleted {
			n++
		}
	}
	return n
}
