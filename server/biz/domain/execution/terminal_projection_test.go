package execution_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Run 在 Workflow Task 执行期间结束：终态事件清掉 sticky 路由与 open WFT 投影，其它 open 项保持原样。
func TestTerminalEventClearsStickyAndOpenWorkflowTask(t *testing.T) {
	events := []history.Event{
		history.NewWorkflowStartedEvent(nil, "run", "", "wf", "Workflow", 0),
		{TimerScheduled: &history.TimerScheduledEvent{TimerID: "t1"}},
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(3, "worker-1", "build-1", 0),
	}
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	state, err := execution.BuildWorkflowExecutionState(&execution.Run{ID: "run", Status: history.StatusRunning}, events)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute)
	state.StickyWorkerIdentity, state.StickyWorkerBuildID, state.StickyExpiresAt = "worker-1", "build-1", &expires
	if state.OpenWorkflowTaskEventID != 3 {
		t.Fatalf("OpenWorkflowTaskEventID = %d, want 3", state.OpenWorkflowTaskEventID)
	}

	terminated := history.NewWorkflowTerminatedEvent("stop")
	terminated.ID = 5
	terminated.Timestamp = time.Now()
	execution.ApplyEventToExecutionState(state, terminated, 0)

	if state.OpenWorkflowTaskEventID != 0 || state.StickyWorkerIdentity != "" || state.StickyWorkerBuildID != "" || state.StickyExpiresAt != nil {
		t.Fatalf("closed run keeps WFT %d and sticky %q/%q/%v", state.OpenWorkflowTaskEventID, state.StickyWorkerIdentity, state.StickyWorkerBuildID, state.StickyExpiresAt)
	}
	if _, ok := state.OpenWaits[2]; !ok || len(state.OpenWaits) != 1 {
		t.Fatalf("OpenWaits = %+v, want only the timer", state.OpenWaits)
	}
}
