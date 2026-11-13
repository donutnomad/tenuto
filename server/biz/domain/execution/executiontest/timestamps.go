package executiontest

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// EventTimestampContract 断言事件时间来自存储的时钟：没有时间的事件以追加时存储时钟的时刻落库，
// 终态事件的这个时刻同时投影为 execution state 的 ClosedAt；调用方给出的时间原样保留。
// newManager 用给定的时钟构造存储。
func EventTimestampContract(t *testing.T, newManager func(t *testing.T, clk clock.Clock) execution.ExecutionManager) {
	start := contractNow.Add(time.Hour)

	t.Run("ZeroTimestampTakesStoreClock", func(t *testing.T) {
		clk := clocktest.NewFake(start)
		manager := newManager(t, clk)
		const runID = "run-stamp"
		createContractRun(t, manager, runID)
		explicit := contractNow.Add(-time.Minute)
		signaled := history.NewWorkflowSignaledEvent("explicit", nil)
		signaled.Timestamp = explicit
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			if _, err := tx.Append(runID, runID, runID, history.NewWorkflowStartedEvent(nil, runID, "", "wf-"+runID, "flow", 0)); err != nil {
				return err
			}
			_, err := tx.Append(runID, runID, runID, signaled)
			return err
		})
		clk.Advance(time.Minute)
		closedAt := start.Add(time.Minute)
		if _, err := manager.Append(runID, runID, runID, history.NewWorkflowCompletedEvent(nil)); err != nil {
			t.Fatal(err)
		}

		events, err := manager.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		want := []time.Time{start, explicit, closedAt}
		if len(events) != len(want) {
			t.Fatalf("events = %d, want %d", len(events), len(want))
		}
		for i, event := range events {
			if !event.Timestamp.Equal(want[i]) {
				t.Errorf("event %d %s timestamp = %v, want %v", event.ID, event.EventType(), event.Timestamp, want[i])
			}
		}
		state, err := manager.GetWorkflowExecutionState(runID)
		if err != nil {
			t.Fatal(err)
		}
		if state.ClosedAt == nil || !state.ClosedAt.Equal(closedAt) {
			t.Fatalf("state.ClosedAt = %v, want %v", state.ClosedAt, closedAt)
		}
	})

	t.Run("WorkflowTaskCompletionBatchTakesStoreClock", func(t *testing.T) {
		clk := clocktest.NewFake(start)
		manager := newManager(t, clk)
		const runID = "run-stamp-batch"
		createContractRun(t, manager, runID)
		if _, err := manager.Append(runID, runID, runID, history.NewWorkflowStartedEvent(nil, runID, "", "wf-"+runID, "flow", 0)); err != nil {
			t.Fatal(err)
		}
		scheduledID, _, err := manager.AppendWorkflowTaskScheduled(runID, runID, runID, history.NewWorkflowTaskScheduledEvent(10_000))
		if err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Second)
		result, err := manager.AppendWorkflowTaskCompletedWithEvents(runID, runID, runID,
			history.NewWorkflowTaskCompletedEvent(scheduledID, "worker"),
			[]history.Event{history.NewMarkerRecordedEvent("m", "m-1", nil), history.NewWorkflowCompletedEvent(nil)})
		if err != nil || !result.ClosedInserted {
			t.Fatalf("complete workflow task: inserted=%v err=%v", result.ClosedInserted, err)
		}
		events, err := manager.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 5 {
			t.Fatalf("events = %d, want started, scheduled, completed task, marker and workflow completed", len(events))
		}
		completedAt := start.Add(time.Second)
		for _, event := range events[2:] {
			if !event.Timestamp.Equal(completedAt) {
				t.Errorf("event %d %s timestamp = %v, want %v", event.ID, event.EventType(), event.Timestamp, completedAt)
			}
		}
	})
}
