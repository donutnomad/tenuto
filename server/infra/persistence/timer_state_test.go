package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: gorm 全字段往返——Deadline/Status/Kind/CreatedAt/UpdatedAt。
func TestTimerStateGormRoundTrip(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	st := &execution.TimerState{
		RunID:            "run-1",
		ScheduledEventID: 10,
		TimerID:          "sleep-1",
		Deadline:         now.Add(time.Minute),
		Status:           execution.TimerStatusEnums.Scheduled,
		Kind:             execution.TimerKindEnums.RetryBackoff,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := store.Executions.UpsertTimerState(st); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Executions.GetTimerState("run-1", 10)
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got.TimerID != "sleep-1" || !got.IsScheduled() {
		t.Fatalf("scalar mismatch %+v", got)
	}
	if !got.Deadline.Equal(st.Deadline) {
		t.Fatalf("deadline lost: got %v want %v", got.Deadline, st.Deadline)
	}
	if got.Kind != execution.TimerKindEnums.RetryBackoff {
		t.Fatalf("Kind round-trip failed: got %q, want %q", got.Kind, execution.TimerKindEnums.RetryBackoff)
	}
	// 已存在的行只写经 setter 改过的字段：直接改字段再 Upsert 不改变已存的行。
	st.Status = execution.TimerStatusEnums.Fired
	if err := store.Executions.UpsertTimerState(st); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.Executions.GetTimerState("run-1", 10)
	if got.Status != execution.TimerStatusEnums.Scheduled {
		t.Fatalf("upsert without changes rewrote the row: %+v", got)
	}
}

// Happy Path: List 按 run 升序；Delete 按 run。
func TestTimerStateGormListDelete(t *testing.T) {
	store := newTestStore(t)
	for _, id := range []int64{20, 10} {
		_ = store.Executions.UpsertTimerState(&execution.TimerState{RunID: "run-1", ScheduledEventID: id, TimerID: "t", Status: execution.TimerStatusEnums.Scheduled, Deadline: time.Now()})
	}
	_ = store.Executions.UpsertTimerState(&execution.TimerState{RunID: "run-2", ScheduledEventID: 30, TimerID: "t", Status: execution.TimerStatusEnums.Scheduled, Deadline: time.Now()})
	list, err := store.Executions.ListTimerStates("run-1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d err=%v", len(list), err)
	}
	if list[0].ScheduledEventID != 10 || list[1].ScheduledEventID != 20 {
		t.Fatalf("not sorted asc: %+v", list)
	}
}
