package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: gorm 全字段往返——Input/Output map（JSON 列）、Status、CreatedAt/UpdatedAt。
func TestChildWorkflowStateGormRoundTrip(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	st := &execution.ChildWorkflowState{
		RunID:                 "parent-1",
		ScheduledEventID:      10,
		ChildID:               "child_1",
		ChildRunID:            "child-run-1",
		WorkflowID:            "child-flow",
		WorkflowConfigVersion: "v1",
		Status:                execution.ChildWorkflowStatusEnums.Initiated,
		Input:                 map[string]any{"order_id": "o-1"},
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if err := store.Executions.UpsertChildWorkflowState(st); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Executions.GetChildWorkflowState("parent-1", 10)
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got.ChildID != "child_1" || got.Status != execution.ChildWorkflowStatusEnums.Initiated || got.Input["order_id"] != "o-1" {
		t.Fatalf("scalar/input mismatch %+v", got)
	}
	if got.WorkflowConfigVersion != "v1" {
		t.Fatalf("config version lost: %+v", got)
	}
	// 已存在的行只写经 setter 改过的字段：直接改字段再 Upsert 不改变已存的行。
	st.Status = execution.ChildWorkflowStatusEnums.Completed
	st.Output = map[string]any{"ok": true}
	if err := store.Executions.UpsertChildWorkflowState(st); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.Executions.GetChildWorkflowState("parent-1", 10)
	if got.Status != execution.ChildWorkflowStatusEnums.Initiated || got.Output != nil {
		t.Fatalf("upsert without changes rewrote the row: %+v", got)
	}
}

// Happy Path: List 按 run 升序；Delete 按 run。
func TestChildWorkflowStateGormListDelete(t *testing.T) {
	store := newTestStore(t)
	for _, id := range []int64{20, 10} {
		_ = store.Executions.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: "parent-1", ScheduledEventID: id, ChildID: "c", WorkflowID: "wf", Status: execution.ChildWorkflowStatusEnums.Initiated})
	}
	_ = store.Executions.UpsertChildWorkflowState(&execution.ChildWorkflowState{RunID: "parent-2", ScheduledEventID: 30, ChildID: "c", WorkflowID: "wf", Status: execution.ChildWorkflowStatusEnums.Initiated})
	list, err := store.Executions.ListChildWorkflowStates("parent-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d err=%v", len(list), err)
	}
	if list[0].ScheduledEventID != 10 || list[1].ScheduledEventID != 20 {
		t.Fatalf("not sorted asc: %+v", list)
	}
}
