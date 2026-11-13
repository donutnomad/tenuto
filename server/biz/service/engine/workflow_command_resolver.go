package engine

import (
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// workflowCommandResolver adapts routing and definition lookup to Run's
// command preparation port before the storage transaction begins.
type workflowCommandResolver struct {
	server *Server
	// programVersion 是本次 Workflow Task 选中的版本，升级边界上领先于 Run 的 current。
	programVersion string
}

var _ execution.WorkflowCommandResolver = workflowCommandResolver{}

func (r workflowCommandResolver) ResolveActivityCommand(runID string, command history.Command) (history.Command, error) {
	return r.server.resolveActivityCommand(runID, r.programVersion, command)
}

func (r workflowCommandResolver) ChildWorkflowRef(runID, workflowID string) (execution.WorkflowTargetRef, error) {
	ref, err := r.server.childWorkflowConfigRef(r.server.runNamespace(runID), workflowID)
	return execution.WorkflowTargetRef(ref), err
}

func (r workflowCommandResolver) ContinuedWorkflowRef(runID, workflowID string) (execution.WorkflowTargetRef, error) {
	workflowType, err := r.server.continuedWorkflowType(runID, workflowID)
	if err != nil {
		return execution.WorkflowTargetRef{}, err
	}
	ref, err := r.server.workflowConfigRef(r.server.runNamespace(runID), workflowType, "")
	return execution.WorkflowTargetRef(ref), err
}

func (r workflowCommandResolver) CheckWorkflowInput(ref execution.WorkflowTargetRef, input map[string]any) error {
	return r.server.checkEntryInput(definition.WorkflowConfigRef(ref), input)
}
