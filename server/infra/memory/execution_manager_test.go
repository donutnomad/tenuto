package memory

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

// Happy Path: RunInTx 内 append event + upsert state 全部成功，提交后两者都可见。
func TestExecutionManagerRunInTxCommits(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), Status: history.StatusRunning})
	})
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if _, _, e := tx.AppendIdempotent("run-1", "run-1", "run-1",
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "s"})); e != nil {
			return e
		}
		return tx.UpsertActivityState(&execution.ActivityState{RunID: "run-1", ScheduledEventID: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled})
	})
	if err != nil {
		t.Fatal(err)
	}
	events, _ := mgr.GetEvents("run-1")
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if _, ok, _ := mgr.GetActivityState("run-1", 1); !ok {
		t.Fatal("state not committed")
	}
}

// Error Case: RunInTx 内 fn 返回 error → event 与 state 都必须回滚（互不残留）。
func TestExecutionManagerRunInTxRollsBack(t *testing.T) {
	mgr := NewExecutionManager()
	_ = mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{ID: "run-1", BranchToken: executiontest.BranchToken("run-1"), Status: history.StatusRunning})
	})
	wantErr := errors.New("boom")
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if _, _, e := tx.AppendIdempotent("run-1", "run-1", "run-1",
			history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "s"})); e != nil {
			return e
		}
		_ = tx.UpsertActivityState(&execution.ActivityState{RunID: "run-1", ScheduledEventID: 1, Stamp: 1})
		return wantErr // 触发回滚
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want boom", err)
	}
	events, _ := mgr.GetEvents("run-1")
	if len(events) != 0 {
		t.Fatalf("event leaked on rollback: %d", len(events))
	}
	if _, ok, _ := mgr.GetActivityState("run-1", 1); ok {
		t.Fatal("state leaked on rollback")
	}
}
