package execution_test

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestParentCloseCandidates(t *testing.T) {
	states := []*execution.ChildWorkflowState{
		{ChildRunID: "default", Status: execution.ChildWorkflowStatusEnums.Started},
		{ChildRunID: "cancel", Status: execution.ChildWorkflowStatusEnums.Started, ParentClosePolicy: protocol.ParentClosePolicyRequestCancel},
		{ChildRunID: "abandon", Status: execution.ChildWorkflowStatusEnums.Started, ParentClosePolicy: protocol.ParentClosePolicyAbandon},
		{ChildRunID: "closed", Status: execution.ChildWorkflowStatusEnums.Completed},
		{ChildRunID: "initiated", Status: execution.ChildWorkflowStatusEnums.Initiated},
		{Status: execution.ChildWorkflowStatusEnums.Initiated},
		nil,
	}
	got := execution.ParentCloseCandidates(states)
	want := []string{"default", "cancel", "initiated"}
	if len(got) != len(want) {
		t.Fatalf("ParentCloseCandidates() = %d states, want %v", len(got), want)
	}
	for i, state := range got {
		if state.ChildRunID != want[i] {
			t.Fatalf("ParentCloseCandidates()[%d] = %s, want %s", i, state.ChildRunID, want[i])
		}
	}
}

func TestParentCloseActionFor(t *testing.T) {
	closed := &execution.WorkflowExecutionState{Status: history.StatusContinuedAsNew}
	running := &execution.WorkflowExecutionState{Status: history.StatusRunning}
	cancelRequested := &execution.WorkflowExecutionState{Status: history.StatusRunning, CancelRequested: true}
	link := func(policy protocol.ParentClosePolicy) *execution.ChildWorkflowState {
		return &execution.ChildWorkflowState{ChildRunID: "child", Status: execution.ChildWorkflowStatusEnums.Started, ParentClosePolicy: policy}
	}
	tests := []struct {
		name   string
		parent *execution.WorkflowExecutionState
		link   *execution.ChildWorkflowState
		child  *execution.WorkflowExecutionState
		want   execution.ParentCloseAction
	}{
		{name: "parent running", parent: running, link: link(""), child: running, want: execution.ParentCloseActionNone},
		{name: "default terminates", parent: closed, link: link(""), child: running, want: execution.ParentCloseActionTerminate},
		{name: "terminate", parent: closed, link: link(protocol.ParentClosePolicyTerminate), child: running, want: execution.ParentCloseActionTerminate},
		{name: "request cancel", parent: closed, link: link(protocol.ParentClosePolicyRequestCancel), child: running, want: execution.ParentCloseActionRequestCancel},
		{name: "cancel already requested", parent: closed, link: link(protocol.ParentClosePolicyRequestCancel), child: cancelRequested, want: execution.ParentCloseActionNone},
		{name: "abandon", parent: closed, link: link(protocol.ParentClosePolicyAbandon), child: running, want: execution.ParentCloseActionNone},
		{name: "child closed", parent: closed, link: link(""), child: &execution.WorkflowExecutionState{Status: history.StatusCompleted}, want: execution.ParentCloseActionNone},
		{name: "link closed", parent: closed, link: &execution.ChildWorkflowState{Status: execution.ChildWorkflowStatusEnums.Completed}, child: running, want: execution.ParentCloseActionNone},
		{name: "link missing", parent: closed, child: running, want: execution.ParentCloseActionNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := execution.ParentCloseActionFor(test.parent, test.link, test.child); got != test.want {
				t.Fatalf("ParentCloseActionFor() = %v, want %v", got, test.want)
			}
		})
	}
}
