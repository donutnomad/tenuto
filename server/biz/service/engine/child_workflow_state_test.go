package engine

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: 新建一个 initiated ChildWorkflowState，约定初始值 Status=Initiated、非终态。
func TestNewChildWorkflowStateDefaults(t *testing.T) {
	st := &execution.ChildWorkflowState{
		RunID:            "parent-1",
		ScheduledEventID: 10,
		ChildID:          "child_1",
		WorkflowType:     "child-flow",
		Status:           execution.ChildWorkflowStatusEnums.Initiated,
	}
	if st.Status != execution.ChildWorkflowStatusEnums.Initiated {
		t.Fatalf("want initiated, got %q", st.Status)
	}
	if st.IsTerminal() {
		t.Fatal("initiated child must not be terminal")
	}
}

// Happy Path: started 是非终态（已建子 Run、等待子终态）。
func TestChildWorkflowStateStartedNotTerminal(t *testing.T) {
	st := &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Started, ChildRunID: "child-run-1"}
	if st.IsTerminal() {
		t.Fatal("started child must not be terminal")
	}
	if st.Status == execution.ChildWorkflowStatusEnums.Initiated {
		t.Fatal("started child must not report initiated")
	}
}

// Happy Path / Edge Case: completed/failed/cancelled/terminated 都是终态。
func TestChildWorkflowStateTerminalPredicates(t *testing.T) {
	for _, s := range []execution.ChildWorkflowStatus{
		execution.ChildWorkflowStatusEnums.Completed,
		execution.ChildWorkflowStatusEnums.Failed,
		execution.ChildWorkflowStatusEnums.Cancelled,
		execution.ChildWorkflowStatusEnums.Terminated,
	} {
		st := &execution.ChildWorkflowState{Status: s}
		if !st.IsTerminal() || st.Status == execution.ChildWorkflowStatusEnums.Initiated {
			t.Fatalf("status %q: terminal=%v initiated=%v", s, st.IsTerminal(), st.Status == execution.ChildWorkflowStatusEnums.Initiated)
		}
	}
}
