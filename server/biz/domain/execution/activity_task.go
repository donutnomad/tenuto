package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// ActivityTaskFromState rebuilds a dispatchable task using the authoritative
// attempt, stamp, and routing fields stored with the Activity, and the
// identity of the Run that scheduled it.
func ActivityTaskFromState(run *Run, state *ActivityState) protocol.Task {
	return protocol.Task{
		Namespace:              protocol.NormalizeNamespace(run.Namespace),
		Type:                   protocol.TaskTypeActivity,
		WorkflowID:             run.WorkflowID,
		WorkflowType:           run.WorkflowType,
		RunID:                  state.RunID,
		ScheduledTime:          state.ScheduledTime,
		ActivityID:             state.ActivityID,
		ScheduledEventID:       state.ScheduledEventID,
		Attempt:                state.Attempt,
		Stamp:                  state.Stamp,
		ActivityType:           state.ActivityType,
		TaskQueue:              state.TaskQueue,
		TargetWorkerIdentity:   state.TargetWorkerIdentity,
		Input:                  state.Input,
		ScheduleToStartTimeout: time.Duration(state.ScheduleToStartTimeoutMs) * time.Millisecond,
		StartToCloseTimeout:    time.Duration(state.StartToCloseTimeoutMs) * time.Millisecond,
		ScheduleToCloseTimeout: time.Duration(state.ScheduleToCloseTimeoutMs) * time.Millisecond,
		HeartbeatTimeout:       time.Duration(state.HeartbeatTimeoutMs) * time.Millisecond,
		LastHeartbeatDetails:   state.LastHeartbeat,
		TraceContext:           cloneStringMap(state.TraceContext),
	}
}
