package persistence

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/stretchr/testify/require"
)

func TestHistoryPageMissingRun(t *testing.T) {
	// Error Case: 缺失的 Run 返回查询错误，调用方可按错误类型处理。
	store := newTestStore(t)
	_, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: "missing-run"})
	require.ErrorIs(t, err, execution.ErrRunNotFound)
}

func TestGORMExecutionProjection_AtomicAppendAndRollback(t *testing.T) {
	// Integration: event 与 projection 同事务提交，重复与回滚保持统计及 ID 稳定。
	store := newTestStore(t)
	createTestRun(t, store, "projection-run")
	event := history.Event{Timestamp: time.Unix(1, 0), WorkflowCompleted: &history.WorkflowCompletedEvent{}}
	id, inserted, err := store.Executions.AppendIdempotent("projection-run", "tree", "projection-branch", event)
	if err != nil || !inserted || id != 1 {
		t.Fatalf("first append = id:%d inserted:%v err:%v", id, inserted, err)
	}
	if _, inserted, err := store.Executions.AppendIdempotent("projection-run", "tree", "projection-branch", event); err != nil || inserted {
		t.Fatalf("duplicate append = inserted:%v err:%v", inserted, err)
	}
	state, err := store.Executions.GetWorkflowExecutionState("projection-run")
	if err != nil {
		t.Fatal(err)
	}
	if state.HistoryEventCount != 1 || state.Status != history.StatusCompleted || state.NextEventID != 2 {
		t.Fatalf("unexpected projection: %+v", state)
	}
	rollbackErr := errors.New("rollback")
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		if _, err := tx.Append("projection-run", "tree", "projection-branch", history.Event{Timestamp: time.Unix(2, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "discard"}}); err != nil {
			return err
		}
		return rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback error = %v", err)
	}
	nextID, err := store.Executions.Append("projection-run", "tree", "projection-branch", history.Event{Timestamp: time.Unix(3, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "keep"}})
	if err != nil || nextID != 2 {
		t.Fatalf("append after rollback = id:%d err:%v", nextID, err)
	}
}

func TestGORMHistoryPagination_ResetBranchSnapshot(t *testing.T) {
	// Integration: reset branch 分页稳定拼接 ancestor prefix 与新 branch，snapshot 隔离后续 append。
	store := newTestStore(t)
	rootToken, err := execution.SerializeBranchToken(&execution.HistoryBranch{Version: 1, TreeID: "tree", BranchID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	root := &execution.Run{ID: "root-run", WorkflowID: "wf", WorkflowConfigVersion: "v1", Status: history.StatusRunning, BranchToken: rootToken, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(root) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := store.Executions.Append(root.ID, "tree", "root", history.Event{Timestamp: time.Unix(int64(i+1), 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "root"}}); err != nil {
			t.Fatal(err)
		}
	}
	fork, err := store.Executions.ForkBranch(execution.ForkBranchRequest{SourceRunID: root.ID, NewRunID: "reset-run", NewBranchID: "reset", ForkNodeID: 3})
	if err != nil {
		t.Fatal(err)
	}
	reset := &execution.Run{ID: "reset-run", WorkflowID: "wf", WorkflowConfigVersion: "v1", Status: history.StatusRunning, BranchToken: fork.BranchToken, CreatedAt: time.Now()}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(reset) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.Executions.Append(reset.ID, "tree", "reset", history.Event{Timestamp: time.Unix(int64(i+10), 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "reset"}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: reset.ID, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Executions.Append(reset.ID, "tree", "reset", history.Event{Timestamp: time.Unix(20, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "later"}}); err != nil {
		t.Fatal(err)
	}
	all := append([]history.Event(nil), first.Events...)
	token := first.NextPageToken
	for token != "" {
		page, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: reset.ID, PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Events...)
		token = page.NextPageToken
	}
	if len(all) != 6 {
		t.Fatalf("snapshot history length = %d, events=%+v", len(all), all)
	}
	for i, event := range all {
		if event.ID != int64(i+1) {
			t.Fatalf("event[%d].ID = %d", i, event.ID)
		}
	}
	desc, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: reset.ID, PageSize: 3, Direction: execution.HistoryDirectionDescending})
	if err != nil || len(desc.Events) != 3 || desc.Events[0].ID != 7 || desc.Events[2].ID != 5 {
		t.Fatalf("descending page = %+v, %v", desc, err)
	}
}
