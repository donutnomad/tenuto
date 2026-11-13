package persistence

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: gorm *Store 实现 ExecutionManager；RunInTx 提交后 event + state 都落库。
func TestStoreRunInTxCommits(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		if _, _, e := tx.AppendIdempotent("run-1", "run-1", "run-1",
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "s"})); e != nil {
			return e
		}
		return tx.UpsertActivityState(&execution.ActivityState{RunID: "run-1", ScheduledEventID: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now()})
	})
	if err != nil {
		t.Fatal(err)
	}
	if events, _ := store.Executions.GetEvents("run-1"); len(events) == 0 {
		t.Fatal("event not committed")
	}
	if _, ok, _ := store.Executions.GetActivityState("run-1", 1); !ok {
		t.Fatal("state not committed")
	}
}

// Error Case: fn 返回 error → DB 事务回滚，event 与 state 都不落库。
func TestStoreRunInTxRollsBack(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "run-1")
	wantErr := errors.New("boom")
	err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		_, _, _ = tx.AppendIdempotent("run-1", "run-1", "run-1",
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "s"}))
		_ = tx.UpsertActivityState(&execution.ActivityState{RunID: "run-1", ScheduledEventID: 1, Stamp: 1, ScheduledTime: time.Now()})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v", err)
	}
	if events, _ := store.Executions.GetEvents("run-1"); len(events) != 0 {
		t.Fatalf("event leaked on rollback: %d", len(events))
	}
	if _, ok, _ := store.Executions.GetActivityState("run-1", 1); ok {
		t.Fatal("state leaked on rollback")
	}
}
