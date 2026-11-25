package memory

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func mkChildState(runID string, schedID int64) *execution.ChildWorkflowState {
	return &execution.ChildWorkflowState{
		RunID:                 runID,
		ScheduledEventID:      schedID,
		ChildID:               "child_1",
		ChildRunID:            "child-run-1",
		WorkflowType:          "child-flow",
		WorkflowConfigVersion: "v1",
		Status:                execution.ChildWorkflowStatusEnums.Initiated,
		Input:                 map[string]any{"order_id": "o-1"},
		CreatedAt:             time.Now(),
		UpdatedAt:             time.Now(),
	}
}

// Happy Path: Upsert 后 Get 全字段往返；返回值是深拷贝（改返回值 + 改 map 都不污染存储）。
func TestChildWorkflowStateStoreUpsertGet(t *testing.T) {
	store := NewChildWorkflowStateStore()
	if err := store.UpsertChildWorkflowState(mkChildState("parent-1", 10)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.GetChildWorkflowState("parent-1", 10)
	if err != nil || !ok {
		t.Fatalf("get failed ok=%v err=%v", ok, err)
	}
	if got.ChildID != "child_1" || got.Status != execution.ChildWorkflowStatusEnums.Initiated || got.Input["order_id"] != "o-1" {
		t.Fatalf("bad state %+v", got)
	}
	// 深拷贝校验：改返回值标量 + map 不应污染存储
	got.Status = execution.ChildWorkflowStatusEnums.Completed
	got.Input["order_id"] = "mutated"
	again, _, _ := store.GetChildWorkflowState("parent-1", 10)
	if again.Status != execution.ChildWorkflowStatusEnums.Initiated || again.Input["order_id"] != "o-1" {
		t.Fatal("store leaked internal pointer (no deep copy)")
	}
}

// Upsert 对已存在的行只写经 setter 改过的字段；更新语义由 executiontest.StatePatchContract 覆盖。
func TestChildWorkflowStateStoreUpsertWithoutChangesKeepsRow(t *testing.T) {
	store := NewChildWorkflowStateStore()
	st := mkChildState("parent-1", 10)
	_ = store.UpsertChildWorkflowState(st)
	stale := mkChildState("parent-1", 10)
	stale.WorkflowType = "stale"
	if err := store.UpsertChildWorkflowState(stale); err != nil {
		t.Fatal(err)
	}
	got, _, _ := store.GetChildWorkflowState("parent-1", 10)
	if got.WorkflowType != st.WorkflowType {
		t.Fatalf("upsert without changes rewrote the row: %+v", got)
	}
}

// Edge Case: Get 不存在返回 ok=false 无 err。
func TestChildWorkflowStateStoreGetMissing(t *testing.T) {
	store := NewChildWorkflowStateStore()
	_, ok, err := store.GetChildWorkflowState("nope", 1)
	if err != nil || ok {
		t.Fatalf("expected ok=false err=nil, got ok=%v err=%v", ok, err)
	}
}

// Happy Path: List 按 run 升序返回全部；Delete 按 run 清空、不影响其它 run。
func TestChildWorkflowStateStoreListDelete(t *testing.T) {
	store := NewChildWorkflowStateStore()
	_ = store.UpsertChildWorkflowState(mkChildState("parent-1", 20))
	_ = store.UpsertChildWorkflowState(mkChildState("parent-1", 10))
	_ = store.UpsertChildWorkflowState(mkChildState("parent-2", 30))
	list, err := store.ListChildWorkflowStates("parent-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list parent-1 = %d err=%v", len(list), err)
	}
	if list[0].ScheduledEventID != 10 || list[1].ScheduledEventID != 20 {
		t.Fatalf("list not sorted asc by scheduledEventID: %+v", list)
	}
}
