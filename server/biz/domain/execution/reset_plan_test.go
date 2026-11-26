package execution_test

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// resetPlanEvents 是两个 Workflow Task 之间调度并完成了一个 Activity 的 History：
//
//	1 WorkflowTaskScheduled  2 WorkflowTaskStarted  3 WorkflowTaskCompleted
//	4 ActivityScheduled      5 ActivityCompleted
//	6 WorkflowTaskScheduled  7 WorkflowTaskStarted  8 WorkflowTaskCompleted
func resetPlanEvents() []history.Event {
	events := []history.Event{
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(1, "w", "b", 0),
		history.NewWorkflowTaskCompletedEvent(1, ""),
		history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "a", ActivityType: "T"}),
		{ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4}},
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(6, "w", "b", 0),
		history.NewWorkflowTaskCompletedEvent(6, ""),
	}
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	return events
}

// PlanReset 依次校验 reset 位置与未结束的子 Workflow，通过时返回分叉位置；未结束的 Activity 不阻止 reset。
func TestPlanReset(t *testing.T) {
	openChild := func(scheduledEventID int64) *execution.ChildWorkflowState {
		return &execution.ChildWorkflowState{ScheduledEventID: scheduledEventID, Status: execution.ChildWorkflowStatusEnums.Started}
	}
	pendingActivity := append(resetPlanEvents()[:4:4], resetPlanEvents()[5:]...)
	for _, test := range []struct {
		name     string
		events   []history.Event
		children []*execution.ChildWorkflowState
		req      execution.ResetWorkflowRequest
		want     execution.ResetPoint
		wantErr  string
	}{
		{
			name:   "Happy Path: the last workflow task replays from its started event",
			events: resetPlanEvents(),
			req:    execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			want:   execution.ResetPoint{NodeID: 7, ScheduledEventID: 6},
		},
		{
			name:   "Happy Path: a close event resolves to its started event",
			events: resetPlanEvents(),
			req:    execution.ResetWorkflowRequest{ResetEventID: 3},
			want:   execution.ResetPoint{NodeID: 2, ScheduledEventID: 1},
		},
		{
			name:     "Edge Case: a child started after the reset point does not block",
			events:   resetPlanEvents(),
			children: []*execution.ChildWorkflowState{openChild(8)},
			req:      execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			want:     execution.ResetPoint{NodeID: 7, ScheduledEventID: 6},
		},
		{
			name:    "Error Case: a reset point that is not a workflow task boundary",
			events:  resetPlanEvents(),
			req:     execution.ResetWorkflowRequest{ResetEventID: 4},
			wantErr: "is not a valid reset point",
		},
		{
			name:   "Edge Case: an activity still pending at the reset point does not block",
			events: pendingActivity,
			req:    execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			want:   execution.ResetPoint{NodeID: 7, ScheduledEventID: 6},
		},
		{
			name:     "Error Case: a child still open at the reset point",
			events:   resetPlanEvents(),
			children: []*execution.ChildWorkflowState{openChild(5)},
			req:      execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			wantErr:  "pending child workflows",
		},
		{
			name:     "Error Case: a pending activity does not hide a pending child",
			events:   pendingActivity,
			children: []*execution.ChildWorkflowState{openChild(5)},
			req:      execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			wantErr:  "pending child workflows",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := execution.PlanReset(test.events, test.children, test.req)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("PlanReset() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("PlanReset() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestCheckPendingChildWorkflows(t *testing.T) {
	for _, test := range []struct {
		name    string
		child   execution.ChildWorkflowState
		blocked bool
	}{
		{name: "Error Case: open child before the reset point", child: execution.ChildWorkflowState{ScheduledEventID: 5, Status: execution.ChildWorkflowStatusEnums.Started}, blocked: true},
		{name: "Error Case: open child at the reset point", child: execution.ChildWorkflowState{ScheduledEventID: 10, Status: execution.ChildWorkflowStatusEnums.Initiated}, blocked: true},
		{name: "Edge Case: closed child before the reset point", child: execution.ChildWorkflowState{ScheduledEventID: 5, Status: execution.ChildWorkflowStatusEnums.Completed}},
		{name: "Edge Case: open child after the reset point", child: execution.ChildWorkflowState{ScheduledEventID: 11, Status: execution.ChildWorkflowStatusEnums.Started}},
	} {
		t.Run(test.name, func(t *testing.T) {
			child := test.child
			err := execution.CheckPendingChildWorkflows([]*execution.ChildWorkflowState{&child}, 10)
			if (err != nil) != test.blocked {
				t.Fatalf("CheckPendingChildWorkflows() error = %v, blocked %v", err, test.blocked)
			}
		})
	}
}
