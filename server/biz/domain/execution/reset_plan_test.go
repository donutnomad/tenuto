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

// childEvents 是第一个 Workflow Task 启动了一个子 Workflow（事件 4）的 History；closed 时子 Workflow 在第二个
// Workflow Task 之前关闭。
func childEvents(closed bool) []history.Event {
	events := []history.Event{
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(1, "w", "b", 0),
		history.NewWorkflowTaskCompletedEvent(1, ""),
		history.NewChildWorkflowStartedEvent("child-flow", "child-1", "child-run", "v1", nil),
	}
	if closed {
		events = append(events, history.Event{ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "child-1", ScheduledEventID: 4}})
	}
	events = append(events,
		history.NewWorkflowTaskScheduledEvent(0),
		history.NewWorkflowTaskStartedEvent(0, "w", "b", 0),
		history.NewWorkflowTaskCompletedEvent(0, ""),
	)
	for i := range events {
		events[i].ID = int64(i + 1)
	}
	n := len(events)
	events[n-2].WorkflowTaskStarted.ScheduledEventID = int64(n - 2)
	events[n-1].WorkflowTaskCompleted.ScheduledEventID = int64(n - 2)
	return events
}

// PlanReset 依次校验 reset 位置与分叉点上未结束的子 Workflow，通过时返回分叉位置；未结束的 Activity 不阻止 reset。
func TestPlanReset(t *testing.T) {
	pendingActivity := append(resetPlanEvents()[:4:4], resetPlanEvents()[5:]...)
	for _, test := range []struct {
		name    string
		events  []history.Event
		req     execution.ResetWorkflowRequest
		want    execution.ResetPoint
		wantErr string
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
			name:   "Edge Case: a child started after the reset point does not block",
			events: childEvents(false),
			req:    execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.FirstWorkflowTask},
			want:   execution.ResetPoint{NodeID: 2, ScheduledEventID: 1},
		},
		{
			name:   "Edge Case: a child closed before the reset point does not block",
			events: childEvents(true),
			req:    execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			want:   execution.ResetPoint{NodeID: 7, ScheduledEventID: 6},
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
			name:    "Error Case: a child still open at the reset point",
			events:  childEvents(false),
			req:     execution.ResetWorkflowRequest{ResetType: execution.ResetTypeEnums.LastWorkflowTask},
			wantErr: "pending child workflows",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := execution.PlanReset(test.events, test.req)
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

// CheckPendingChildWorkflows 按保留的 History 判定：分叉点之前启动、分叉点上还没有关闭事件的子 Workflow 拦下 reset。
func TestCheckPendingChildWorkflows(t *testing.T) {
	started := func(id int64) history.Event {
		event := history.NewChildWorkflowStartedEvent("child-flow", "child-1", "child-run", "v1", nil)
		event.ID = id
		return event
	}
	closeEvents := map[string]func(scheduledEventID int64) history.Event{
		"completed": func(id int64) history.Event {
			return history.Event{ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ScheduledEventID: id}}
		},
		"failed": func(id int64) history.Event {
			return history.Event{ChildWorkflowFailed: &history.ChildWorkflowFailedEvent{ScheduledEventID: id}}
		},
		"cancelled": func(id int64) history.Event {
			return history.Event{ChildWorkflowCancelled: &history.ChildWorkflowCancelledEvent{ScheduledEventID: id}}
		},
		"terminated": func(id int64) history.Event {
			return history.Event{ChildWorkflowTerminated: &history.ChildWorkflowTerminatedEvent{ScheduledEventID: id}}
		},
	}
	withID := func(event history.Event, id int64) history.Event { event.ID = id; return event }

	for _, test := range []struct {
		name    string
		events  []history.Event
		blocked bool
	}{
		{name: "Error Case: open child before the reset point", events: []history.Event{started(5)}, blocked: true},
		{name: "Error Case: open child at the reset point", events: []history.Event{started(10)}, blocked: true},
		{name: "Error Case: child closed only after the reset point", events: []history.Event{started(5), withID(closeEvents["completed"](5), 11)}, blocked: true},
		{name: "Edge Case: open child after the reset point", events: []history.Event{started(11)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := execution.CheckPendingChildWorkflows(test.events, 10)
			if (err != nil) != test.blocked {
				t.Fatalf("CheckPendingChildWorkflows() error = %v, blocked %v", err, test.blocked)
			}
		})
	}
	for kind, closeEvent := range closeEvents {
		t.Run("Edge Case: child "+kind+" before the reset point", func(t *testing.T) {
			if err := execution.CheckPendingChildWorkflows([]history.Event{started(5), withID(closeEvent(5), 6)}, 10); err != nil {
				t.Fatalf("CheckPendingChildWorkflows() error = %v, want nil", err)
			}
		})
	}
}
