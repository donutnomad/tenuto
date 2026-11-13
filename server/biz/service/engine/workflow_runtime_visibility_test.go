package engine

import (
	"context"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowRuntimeListsRunsWithSearchAttributesAndWaitingPoint(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-visible",
		WorkflowID:            "wf-visible",
		WorkflowType:          "wf-visible",
		WorkflowConfigVersion: "v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.Event{
		Timestamp:         time.Now(),
		ActivityScheduled: &historypkg.ActivityScheduledEvent{ActivityID: "capture", ActivityType: "custodian.Capture", TaskQueue: "custodian"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.UpsertSearchAttributes(ctx, "", "", run.ID, map[string]string{
		"business_id": "biz-1",
		"order_id":    "order-1",
	}); err != nil {
		t.Fatal(err)
	}

	result, err := server.View().ListRuns(execution.RunListFilter{
		WorkflowID:       "wf-visible",
		Statuses:         []historypkg.Status{historypkg.StatusRunning},
		SearchAttributes: map[string]string{"business_id": "biz-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected one visible run, got %+v", result)
	}
	item := result.Items[0]
	if item.RunID != run.ID || item.SearchAttributes["order_id"] != "order-1" {
		t.Fatalf("unexpected visible run item: %+v", item)
	}
	if item.CurrentWaiting.Type != execution.WaitingActivity || item.CurrentWaiting.ActivityID != "capture" || item.CurrentWaiting.ScheduledEventID != scheduledID {
		t.Fatalf("unexpected waiting point: %+v", item.CurrentWaiting)
	}
}

func TestWorkflowRuntimeQueryReturnsFailureReason(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-visible-failed",
		WorkflowID:            "wf-visible",
		WorkflowType:          "wf-visible",
		WorkflowConfigVersion: "v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowFailedEvent("boom", "")); err != nil {
		t.Fatal(err)
	}

	result, err := server.QueryWorkflow(ctx, "", "", run.ID, "visibility", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.FailureReason != "boom" {
		t.Fatalf("expected failure reason, got %+v", result)
	}
}

func TestWorkflowRuntimeUpdateOnlyAcceptsAndSchedulesWorkflowTask(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-update-accepted",
		WorkflowID:            "wf-update",
		WorkflowType:          "wf-update",
		WorkflowConfigVersion: "v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	result, err := server.UpdateWorkflow(ctx, "", "", run.ID, "set_priority", "", "", updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.UpdateID == "" || result.AcceptedEventID == 0 {
		t.Fatalf("unexpected update accept result: %+v", result)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowUpdateAccepted) != 1 || countEngineEvents(history, historypkg.EventWorkflowUpdateCompleted) != 0 {
		t.Fatalf("expected accepted-only update history, got %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 || len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected update to schedule one workflow task, history=%+v queue=%d", history, len(workflowTasks.tasks))
	}
}

func TestWorkflowRuntimeListsRunsAndUpsertsSearchAttributes(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{
		ID:                    "run-visible-http",
		WorkflowID:            "wf-visible-http",
		WorkflowType:          "approval-workflow",
		WorkflowConfigVersion: "v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if err := server.UpsertSearchAttributes(ctx, "", "", run.ID, map[string]string{"business_id": "biz-http"}); err != nil {
		t.Fatal(err)
	}

	result, err := server.View().ListRuns(execution.RunListFilter{
		SearchAttributes: map[string]string{"business_id": "biz-http"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].RunID != run.ID || result.Items[0].WorkflowType != run.WorkflowType {
		t.Fatalf("unexpected HTTP list result: %+v", result)
	}
}

func TestWorkflowRuntimeHistoryPageAndLegacySnapshotParity(t *testing.T) {
	// Integration: page endpoint 固定快照，v1 history 通过 iterator 返回同一有序内容。
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "history-page-http", WorkflowID: "wf", WorkflowType: "wf", WorkflowConfigVersion: "v1", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := mgr.Append(run.ID, "tree", "branch", historypkg.Event{Timestamp: time.Unix(int64(i+1), 0), WorkflowSignaled: &historypkg.WorkflowSignaledEvent{Name: "signal"}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := server.View().GetEventPage("", "", execution.HistoryPageRequest{RunID: run.ID, PageSize: 2})
	if err != nil || len(first.Events) != 2 || first.NextPageToken == "" {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	legacy, err := server.View().GetEvents("", "", run.ID)
	if err != nil || len(legacy) != 5 {
		t.Fatalf("legacy history = %+v, %v", legacy, err)
	}
	for i, event := range legacy {
		if event.ID != int64(i+1) {
			t.Fatalf("legacy event[%d].ID = %d", i, event.ID)
		}
	}
}
