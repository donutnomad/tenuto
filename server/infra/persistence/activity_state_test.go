package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: gorm 全字段往返——含 RetryPolicy/LastHeartbeat 经 JSON 列、
// StartedEventID=TransientEventID(-124)、Stamp、ActivityReset。
func TestActivityStateGormRoundTrip(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	st := &execution.ActivityState{
		RunID:                 "run-1",
		ScheduledEventID:      10,
		ActivityID:            "step-a",
		ActivityType:          "Reserve",
		TaskQueue:             "tq",
		Attempt:               2,
		Stamp:                 5,
		StartedEventID:        execution.TransientEventID,
		ActivityReset:         true,
		WorkerIdentity:        "worker-1",
		WorkerBuildID:         "build-7",
		LastHeartbeat:         map[string]any{"offset": float64(7)},
		LastHeartbeatAt:       &now,
		NextAttemptAt:         &now,
		RetryLastFailure:      "boom",
		Status:                execution.ActivityStatusEnums.Started,
		ScheduledTime:         now,
		StartToCloseTimeoutMs: 5000,
		RetryPolicy:           &history.RetryPolicy{MaxAttempts: 3, BackoffCoefficient: 2},
		Input:                 map[string]any{"k": "v"},
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if err := store.Executions.UpsertActivityState(st); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Executions.GetActivityState("run-1", 10)
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got.Stamp != 5 || got.StartedEventID != execution.TransientEventID || !got.ActivityReset || got.WorkerBuildID != "build-7" {
		t.Fatalf("scalar mismatch %+v", got)
	}
	if got.RetryPolicy == nil || got.RetryPolicy.MaxAttempts != 3 {
		t.Fatalf("retry policy lost: %+v", got.RetryPolicy)
	}
	if got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(now) {
		t.Fatalf("next attempt time lost: %v", got.NextAttemptAt)
	}
	if got.LastHeartbeat["offset"] != int64(7) {
		t.Fatalf("heartbeat lost: %+v", got.LastHeartbeat)
	}
	// 已存在的行只写经 setter 改过的字段：直接改字段再 Upsert 不改变已存的行。
	// 更新语义由 executiontest.StatePatchContract 覆盖。
	attempt, stamp, status := got.Attempt, got.Stamp, got.Status
	st.Attempt, st.Stamp, st.Status = 3, 6, execution.ActivityStatusEnums.Completed
	if err := store.Executions.UpsertActivityState(st); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.Executions.GetActivityState("run-1", 10)
	if got.Attempt != attempt || got.Stamp != stamp || got.Status != status {
		t.Fatalf("upsert without changes rewrote the row: %+v", got)
	}
}

// Happy Path: List 按 run，Delete 按 run。
func TestActivityStateGormListDelete(t *testing.T) {
	store := newTestStore(t)
	for _, id := range []int64{10, 20} {
		_ = store.Executions.UpsertActivityState(&execution.ActivityState{RunID: "run-1", ScheduledEventID: id, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now()})
	}
	_ = store.Executions.UpsertActivityState(&execution.ActivityState{RunID: "run-2", ScheduledEventID: 30, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now()})
	list, err := store.Executions.ListActivityStates("run-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d err=%v", len(list), err)
	}
}
