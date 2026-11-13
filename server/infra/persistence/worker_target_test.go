package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestActivityWorkerTargetPersistenceAndRetryTask(t *testing.T) {
	store := newTestStore(t)
	createTestRun(t, store, "target-persist")
	state := &execution.ActivityState{RunID: "target-persist", ScheduledEventID: 7, ActivityID: "charge", TargetWorkerIdentity: "worker-A", Attempt: 2, Stamp: 3, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: time.Now()}
	if err := store.Executions.UpsertActivityState(state); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Executions.GetActivityState(state.RunID, state.ScheduledEventID)
	if err != nil || !ok {
		t.Fatalf("get=%v %v", ok, err)
	}
	task := execution.ActivityTaskFromState(&execution.Run{ID: got.RunID}, got)
	if task.TargetWorkerIdentity != "worker-A" || task.Attempt != 2 || task.Stamp != 3 {
		t.Fatalf("retry task=%+v", task)
	}
}
