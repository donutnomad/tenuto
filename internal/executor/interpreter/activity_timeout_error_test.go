package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// Activity 因超时失败时，Workflow 用 workflow.TimeoutTypeOf 读出超时种类；其他失败返回空串。
func TestWorkflowReadsActivityTimeoutType(t *testing.T) {
	const src = `package flows
import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	kind := workflow.TimeoutTypeOf(err)
	if kind == workflow.TimeoutTypeStartToClose {
		return map[string]any{"timeout": "start-to-close"}, nil
	}
	if kind == "" {
		return map[string]any{"timeout": "none"}, nil
	}
	return map[string]any{"timeout": "other"}, nil
}
`
	for name, tc := range map[string]struct {
		failure protocol.Failure
		want    string
	}{
		"timeout": {failure: protocol.Failure{Message: "activity StartToClose timeout", TimeoutType: protocol.TimeoutTypeStartToClose}, want: "start-to-close"},
		"failure": {failure: protocol.Failure{Message: "boom"}, want: "none"},
	} {
		t.Run(name, func(t *testing.T) {
			started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
			sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"}}
			failure := tc.failure
			failed := history.Event{ID: 3, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 2, Error: failure.Message, Failure: &failure}}
			cmds, err := probeExecFix(t, src, []history.Event{started, sched, failed})
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			for _, cmd := range cmds {
				if cmd.Type == history.CmdCompleteWorkflow {
					if cmd.Output["timeout"] != tc.want {
						t.Fatalf("output = %v, want timeout %q", cmd.Output, tc.want)
					}
					return
				}
			}
			t.Fatalf("want CompleteWorkflow, got %+v", cmds)
		})
	}
}
