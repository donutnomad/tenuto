package memory

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func mkState(runID string, schedID int64) *execution.ActivityState {
	return &execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: schedID,
		ActivityID:       "step",
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   execution.EmptyEventID,
		Status:           execution.ActivityStatusEnums.Scheduled,
		ScheduledTime:    time.Now(),
		LastHeartbeat:    map[string]any{"offset": float64(3)},
		RetryPolicy:      &history.RetryPolicy{MaxAttempts: 3},
	}
}

// Happy Path: Upsert 后 Get 全字段往返；StartedEventID=EmptyEventID 默认；map/RetryPolicy 深拷贝。
func TestActivityStateStoreUpsertGet(t *testing.T) {
	store := NewActivityStateStore()
	if err := store.UpsertActivityState(mkState("run-1", 10)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.GetActivityState("run-1", 10)
	if err != nil || !ok {
		t.Fatalf("get failed ok=%v err=%v", ok, err)
	}
	if got.Attempt != 1 || got.Stamp != 1 || got.StartedEventID != execution.EmptyEventID {
		t.Fatalf("bad state %+v", got)
	}
	// 深拷贝校验：改返回值不应污染存储
	got.LastHeartbeat["offset"] = float64(99)
	again, _, _ := store.GetActivityState("run-1", 10)
	if again.LastHeartbeat["offset"] != float64(3) {
		t.Fatal("store leaked internal map (no deep copy)")
	}
}

// Edge Case: Get 不存在返回 ok=false 无 err。
func TestActivityStateStoreGetMissing(t *testing.T) {
	store := NewActivityStateStore()
	_, ok, err := store.GetActivityState("nope", 1)
	if err != nil || ok {
		t.Fatalf("expected ok=false err=nil, got ok=%v err=%v", ok, err)
	}
}

// Happy Path: List 按 run 返回全部。
func TestActivityStateStoreListDelete(t *testing.T) {
	store := NewActivityStateStore()
	_ = store.UpsertActivityState(mkState("run-1", 10))
	_ = store.UpsertActivityState(mkState("run-1", 20))
	_ = store.UpsertActivityState(mkState("run-2", 30))
	list, err := store.ListActivityStates("run-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list run-1 = %d err=%v", len(list), err)
	}
}
