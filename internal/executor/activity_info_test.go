package executor

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// startedActivityRuntime 在 begin 时返回 Server 记下的开始时间。
type startedActivityRuntime struct {
	recordingActivityRuntime
	startedTime time.Time
}

func (r *startedActivityRuntime) BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{Execute: true, StartedTime: r.startedTime}, nil
}

// Activity 从 activity.GetInfo 读到所属 Workflow、Activity ID、时间点与 Task Token；Deadline 取
// StartToClose 与 ScheduleToClose 中先到的一个。
func TestRunActivityTaskProvidesActivityInfo(t *testing.T) {
	scheduled := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	started := scheduled.Add(50 * time.Second)
	for name, tc := range map[string]struct {
		startToClose, scheduleToClose time.Duration
		deadline                      time.Time
	}{
		"start to close first":    {startToClose: 30 * time.Second, scheduleToClose: time.Hour, deadline: started.Add(30 * time.Second)},
		"schedule to close first": {startToClose: 30 * time.Second, scheduleToClose: time.Minute, deadline: scheduled.Add(time.Minute)},
		"no close timeout":        {},
	} {
		t.Run(name, func(t *testing.T) {
			var got activity.Info
			registry := activity.NewRegistry()
			if err := registry.Register("charge", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				got = activity.GetInfo(ctx)
				return map[string]any{}, nil
			}); err != nil {
				t.Fatal(err)
			}
			task := protocol.Task{
				Namespace: "payments", WorkflowID: "order-7", WorkflowType: "order-flow", RunID: "run-1",
				ActivityID: "#a2", ActivityType: "charge", TaskQueue: "billing", Attempt: 3,
				ScheduledTime: scheduled, HeartbeatTimeout: 10 * time.Second, TaskToken: "token-1",
				StartToCloseTimeout: tc.startToClose, ScheduleToCloseTimeout: tc.scheduleToClose,
			}
			runtime := &startedActivityRuntime{startedTime: started}
			if outcome, executionErr, reportErr := RunActivityTask(context.Background(), task, runtime, registry, nil); outcome != "success" || executionErr != nil || reportErr != nil {
				t.Fatalf("outcome=%q executionErr=%v reportErr=%v", outcome, executionErr, reportErr)
			}
			want := activity.Info{
				Namespace: "payments", WorkflowID: "order-7", WorkflowType: "order-flow", RunID: "run-1",
				ActivityID: "#a2", ActivityType: "charge", TaskQueue: "billing", Attempt: 3,
				ScheduledTime: scheduled, StartedTime: started, Deadline: tc.deadline,
				HeartbeatTimeout: 10 * time.Second, TaskToken: "token-1",
			}
			if got != want {
				t.Fatalf("activity.Info = %+v\nwant %+v", got, want)
			}
		})
	}
}
