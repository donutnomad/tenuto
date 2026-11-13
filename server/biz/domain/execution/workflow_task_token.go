package execution

import (
	"github.com/donutnomad/tenuto/protocol"
)

// ValidateWorkflowTaskToken 校验 token 与请求一致，并按 execution state 的 generation、activation 与
// sticky owner 做 fence。
func ValidateWorkflowTaskToken(task protocol.Task, runID string, scheduledEventID int64, states interface {
	GetWorkflowExecutionState(runID string) (*WorkflowExecutionState, error)
}) error {
	if task.Type != protocol.TaskTypeWorkflow {
		return protocol.InvalidRequestf("task token type %s is invalid for workflow task", task.Type)
	}
	if task.RunID != runID || task.ScheduledEventID != scheduledEventID {
		return protocol.InvalidRequestf("workflow task token does not match request")
	}
	if task.ExecutionGeneration <= 0 {
		return protocol.InvalidRequestf("workflow task token has no execution generation")
	}
	state, err := states.GetWorkflowExecutionState(runID)
	if err != nil {
		return err
	}
	if state.ExecutionGeneration != task.ExecutionGeneration || state.StickyGeneration != task.StickyGeneration {
		return protocol.InvalidRequestf("workflow task token generation fence is stale")
	}
	if task.ActivationID != "" && state.CurrentActivationID != task.ActivationID {
		return protocol.InvalidRequestf("workflow task activation fence is stale")
	}
	if task.WorkflowTaskRoute == protocol.WorkflowTaskRouteSticky &&
		(state.StickyWorkerIdentity != task.WorkerIdentity || state.StickyWorkerBuildID != task.WorkerBuildID) {
		return protocol.InvalidRequestf("workflow task sticky owner fence is stale: state=%s/%s task=%s/%s",
			state.StickyWorkerIdentity, state.StickyWorkerBuildID, task.WorkerIdentity, task.WorkerBuildID)
	}
	return nil
}
