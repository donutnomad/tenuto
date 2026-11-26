package execution

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func TestRuntimeCheckCancellation(t *testing.T) {
	store := &statusTestStore{events: []history.Event{
		history.NewWorkflowStartedEvent(nil, "run-1", "", "workflow", "Workflow", 0),
		history.NewWorkflowCancellationRequestedEvent("stop"),
	}}
	runtime := NewRuntime(store)
	already, err := runtime.CheckCancellation("run-1")
	if err != nil || !already {
		t.Fatalf("already=%t err=%v", already, err)
	}
	store.events = append(store.events, history.NewWorkflowCompletedEvent(nil))
	if _, err := runtime.CheckCancellation("run-1"); err == nil {
		t.Fatal("terminal Run accepted a cancellation request")
	}
}

func TestChildCloseRecoveryActionUsesTerminalHistory(t *testing.T) {
	store := &statusTestStore{events: []history.Event{history.NewWorkflowCompletedEvent(map[string]any{"value": "history"})}}
	runtime := NewRuntime(store)
	current := &Run{
		ID: "child", ParentRunID: "parent", ParentScheduledEventID: 7,
		Output: map[string]any{"value": "projection"},
	}
	action, needed, err := runtime.ChildCloseRecoveryActionFor(current, history.StatusCompleted)
	if err != nil || !needed || action.EventType != history.EventChildWorkflowCompleted || action.Output["value"] != "history" {
		t.Fatalf("action=%+v needed=%t err=%v", action, needed, err)
	}
	current.ParentRunID = ""
	if _, needed, err := runtime.ChildCloseRecoveryActionFor(current, history.StatusCompleted); err != nil || needed {
		t.Fatalf("unparented Run: needed=%t err=%v", needed, err)
	}
}

func TestTerminationChecks(t *testing.T) {
	store := &statusTestStore{events: []history.Event{history.NewWorkflowCompletedEvent(nil)}}
	runtime := NewRuntime(store)
	if err := runtime.CheckTermination("run-1"); err == nil {
		t.Fatal("completed Run accepted termination")
	}
	if err := EnsureTerminable(&Run{ID: "run-1", Status: history.StatusContinuedAsNew}); err == nil {
		t.Fatal("continued Run accepted lease termination")
	}
	if got := TerminationReason(""); got != "terminated" {
		t.Fatalf("termination reason = %q", got)
	}
}

// Error Case：子 run 失败后由重试 run 接续时，子工作流尚未结束，恢复不向父 run 传播失败。
func TestChildCloseRecoveryActionSkipsFailureContinuedByRetry(t *testing.T) {
	failed, err := history.NewWorkflowFailedFailureEvent(protocol.Failure{Message: "boom"}, "")
	if err != nil {
		t.Fatal(err)
	}
	store := &statusTestStore{events: []history.Event{failed}}
	runtime := NewRuntime(store)
	child := &Run{ID: "child", ParentRunID: "parent", ParentScheduledEventID: 7}

	action, needed, err := runtime.ChildCloseRecoveryActionFor(child, history.StatusFailed)
	if err != nil || !needed || action.EventType != history.EventChildWorkflowFailed {
		t.Fatalf("final failure: action=%+v needed=%t err=%v", action, needed, err)
	}

	failed.WorkflowFailed.NewExecutionRunID = "child-retry"
	store.events = []history.Event{failed}
	if _, needed, err := runtime.ChildCloseRecoveryActionFor(child, history.StatusFailed); err != nil || needed {
		t.Fatalf("failure continued by retry: needed=%t err=%v", needed, err)
	}
}

// Happy Path：子工作流的重试 run 与 continue-as-new 接续 run 沿用父关联，接续链最后一个 run 的终态才能传播给父 run。
func TestRetryAndContinuedRunsKeepParentLink(t *testing.T) {
	child := &Run{
		ID: "child", WorkflowID: "orders", Attempt: 1,
		ParentRunID: "parent", ParentChildID: "step", ParentScheduledEventID: 7,
	}
	for name, next := range map[string]*Run{
		"retry":           NewRetryRun(child, "child-2", time.Now(), nil),
		"continue-as-new": NewContinuedRun(child, "child-3", "default", "orders", "v1", nil, time.Now()),
	} {
		if next.ParentRunID != "parent" || next.ParentChildID != "step" || next.ParentScheduledEventID != 7 {
			t.Fatalf("%s run lost its parent link: %+v", name, next)
		}
	}
}
