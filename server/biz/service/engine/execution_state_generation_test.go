package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestActivityResetInvalidatesExecutionAndStickyGenerations(t *testing.T) {
	state := execution.NewWorkflowExecutionState(&execution.Run{ID: "reset-run", Status: history.StatusRunning})
	state.StickyWorkerIdentity = "worker-a"
	state.StickyWorkerBuildID = "build-a"
	stickyExpiry := time.Now().Add(time.Minute)
	state.StickyExpiresAt = &stickyExpiry
	event := history.NewActivityResetEvent(history.ActivityResetEvent{ScheduledEventID: 3, Attempt: 2})
	event.ID = 8
	size, err := history.CanonicalEventSize(event)
	if err != nil {
		t.Fatal(err)
	}
	execution.ApplyEventToExecutionState(state, event, size)
	if state.ExecutionGeneration != 2 || state.StickyGeneration != 2 {
		t.Fatalf("generations = execution:%d sticky:%d", state.ExecutionGeneration, state.StickyGeneration)
	}
	if state.StickyWorkerIdentity != "" || state.StickyWorkerBuildID != "" || state.StickyExpiresAt != nil {
		t.Fatalf("sticky assignment survived reset: %+v", state)
	}
}
