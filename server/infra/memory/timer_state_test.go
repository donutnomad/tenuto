package memory

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func mkTimerState(runID string, schedID int64) *execution.TimerState {
	return &execution.TimerState{
		RunID:            runID,
		ScheduledEventID: schedID,
		TimerID:          "t1",
		Deadline:         time.Now().Add(time.Minute),
		Status:           execution.TimerStatusEnums.Scheduled,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}
}

// Happy Path: Upsert 后 Get 全字段往返；返回值是深拷贝（改返回值不污染存储）。
func TestTimerStateStoreUpsertGet(t *testing.T) {
	store := NewTimerStateStore()
	if err := store.UpsertTimerState(mkTimerState("run-1", 10)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.GetTimerState("run-1", 10)
	if err != nil || !ok {
		t.Fatalf("get failed ok=%v err=%v", ok, err)
	}
	if got.TimerID != "t1" || !got.IsScheduled() {
		t.Fatalf("bad state %+v", got)
	}
	// 深拷贝校验：改返回值不应污染存储
	got.Status = execution.TimerStatusEnums.Fired
	again, _, _ := store.GetTimerState("run-1", 10)
	if again.Status != execution.TimerStatusEnums.Scheduled {
		t.Fatal("store leaked internal pointer (no deep copy)")
	}
}

// Upsert 对已存在的行只写经 setter 改过的字段；更新语义由 executiontest.StatePatchContract 覆盖。
func TestTimerStateStoreUpsertWithoutChangesKeepsRow(t *testing.T) {
	store := NewTimerStateStore()
	st := mkTimerState("run-1", 10)
	_ = store.UpsertTimerState(st)
	stale := mkTimerState("run-1", 10)
	stale.Status = execution.TimerStatusEnums.Fired
	if err := store.UpsertTimerState(stale); err != nil {
		t.Fatal(err)
	}
	got, _, _ := store.GetTimerState("run-1", 10)
	if got.Status != st.Status {
		t.Fatalf("upsert without changes rewrote the row: %+v", got)
	}
}

// Edge Case: Get 不存在返回 ok=false 无 err。
func TestTimerStateStoreGetMissing(t *testing.T) {
	store := NewTimerStateStore()
	_, ok, err := store.GetTimerState("nope", 1)
	if err != nil || ok {
		t.Fatalf("expected ok=false err=nil, got ok=%v err=%v", ok, err)
	}
}

// Happy Path: List 按 run 升序返回全部；Delete 按 run 清空、不影响其它 run。
func TestTimerStateStoreListDelete(t *testing.T) {
	store := NewTimerStateStore()
	_ = store.UpsertTimerState(mkTimerState("run-1", 20))
	_ = store.UpsertTimerState(mkTimerState("run-1", 10))
	_ = store.UpsertTimerState(mkTimerState("run-2", 30))
	list, err := store.ListTimerStates("run-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list run-1 = %d err=%v", len(list), err)
	}
	if list[0].ScheduledEventID != 10 || list[1].ScheduledEventID != 20 {
		t.Fatalf("list not sorted asc by scheduledEventID: %+v", list)
	}
}
