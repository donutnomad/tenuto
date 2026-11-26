package memory

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func TestExecutionProjection_AtomicAppendDedupeAndRollback(t *testing.T) {
	// Integration: commit 同步推进 projection，幂等与回滚均保持 count、bytes 和连续 event ID。
	mgr := NewExecutionManager()
	run := &execution.Run{ID: "projection-run", BranchToken: executiontest.BranchToken("projection-run"), WorkflowID: "wf", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	event := history.Event{Timestamp: time.Unix(1, 0), TimerScheduled: &history.TimerScheduledEvent{TimerID: "t1"}}
	id, inserted, err := mgr.AppendIdempotent(run.ID, "tree", "branch", event)
	if err != nil || !inserted || id != 1 {
		t.Fatalf("first append = id:%d inserted:%v err:%v", id, inserted, err)
	}
	if _, inserted, err := mgr.AppendIdempotent(run.ID, "tree", "branch", event); err != nil || inserted {
		t.Fatalf("duplicate append = inserted:%v err:%v", inserted, err)
	}
	state, err := mgr.GetWorkflowExecutionState(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.HistoryEventCount != 1 || state.NextEventID != 2 {
		t.Fatalf("unexpected projection: %+v", state)
	}
	rollbackErr := errors.New("rollback")
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		if _, err := tx.Append(run.ID, "tree", "branch", history.Event{Timestamp: time.Unix(2, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "discard"}}); err != nil {
			return err
		}
		return rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback error = %v", err)
	}
	nextID, err := mgr.Append(run.ID, "tree", "branch", history.Event{Timestamp: time.Unix(3, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "keep"}})
	if err != nil || nextID != 2 {
		t.Fatalf("append after rollback = id:%d err:%v", nextID, err)
	}
}

func TestHistoryPagination_SnapshotAndDirections(t *testing.T) {
	// Integration: 首页固定 snapshot，新 append 留给 refresh，正反向页均无重页。
	mgr := NewExecutionManager()
	run := &execution.Run{ID: "page-run", BranchToken: executiontest.BranchToken("page-run"), WorkflowID: "wf", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := mgr.Append(run.ID, "tree", "branch", history.Event{Timestamp: time.Unix(int64(i+1), 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "signal"}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := mgr.GetEventPage(execution.HistoryPageRequest{RunID: run.ID, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, "tree", "branch", history.Event{Timestamp: time.Unix(6, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "later"}}); err != nil {
		t.Fatal(err)
	}
	second, err := mgr.GetEventPage(execution.HistoryPageRequest{RunID: run.ID, PageSize: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	third, err := mgr.GetEventPage(execution.HistoryPageRequest{RunID: run.ID, PageSize: 2, PageToken: second.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	got := append(append(first.Events, second.Events...), third.Events...)
	if len(got) != 5 || got[0].ID != 1 || got[4].ID != 5 || third.NextPageToken != "" {
		t.Fatalf("snapshot pages = %+v", got)
	}
	desc, err := mgr.GetEventPage(execution.HistoryPageRequest{RunID: run.ID, PageSize: 2, Direction: execution.HistoryDirectionDescending})
	if err != nil || len(desc.Events) != 2 || desc.Events[0].ID != 6 || desc.Events[1].ID != 5 {
		t.Fatalf("descending page = %+v, %v", desc, err)
	}
}
