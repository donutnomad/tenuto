package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// History 事件的时间来自 Server 的时钟：假时钟停住时事件时间恰为该时刻，推进后新事件带新时刻。
func TestHistoryEventTimestampsComeFromServerClock(t *testing.T) {
	start := time.Date(2026, 11, 11, 8, 30, 0, 0, time.UTC)
	clk := clocktest.NewFake(start)
	mgr := memory.NewExecutionManager()
	server := assembleServerWithClock(mgr, newTestTaskQueue(), newTestTaskQueue(), clk)
	t.Cleanup(server.Stop)

	const workflowID = "event-clock"
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: workflowID, Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	startTypes := assertEventTimes(t, mgr, runID, 0, start)
	if !containsEventType(startTypes, history.EventWorkflowStarted) || !containsEventType(startTypes, history.EventWorkflowTaskScheduled) {
		t.Fatalf("start appended %v, want WorkflowStarted and WorkflowTaskScheduled", startTypes)
	}

	later := start.Add(90 * time.Second)
	clk.Advance(later.Sub(start))
	if _, err := server.SignalWorkflow(context.Background(), "", workflowID, runID, "poke", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := server.TerminateWorkflow(context.Background(), "", workflowID, runID, "done"); err != nil {
		t.Fatal(err)
	}
	laterTypes := assertEventTimes(t, mgr, runID, len(startTypes), later)
	if !containsEventType(laterTypes, history.EventWorkflowSignaled) || !containsEventType(laterTypes, history.EventWorkflowTerminated) {
		t.Fatalf("signal and terminate appended %v, want WorkflowSignaled and WorkflowTerminated", laterTypes)
	}
}

// assertEventTimes 断言 runID 前 skip 个之后的事件都带时间 want，返回这些事件的类型。
func assertEventTimes(t *testing.T, mgr *memory.ExecutionManager, runID string, skip int, want time.Time) []history.EventType {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var types []history.EventType
	for _, event := range events[skip:] {
		types = append(types, event.EventType())
		if !event.Timestamp.Equal(want) {
			t.Errorf("event %d %s timestamp = %v, want %v", event.ID, event.EventType(), event.Timestamp, want)
		}
	}
	return types
}

func containsEventType(types []history.EventType, want history.EventType) bool {
	for _, typ := range types {
		if typ == want {
			return true
		}
	}
	return false
}
