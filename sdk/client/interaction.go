package client

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

func (c *workflowClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error {
	if signalName == "" {
		return errors.New("client: signal name is required")
	}
	input, err := encodeObject("signal argument", []any{arg})
	if err != nil {
		return err
	}
	// request ID 让重试的重发在 Server 上只记一条 Signal。
	return mapError(callOther, c.workflows().SignalWorkflow(ctx, runtimeapi.WorkflowRuntimeInteractionRequest{
		Namespace: c.namespace, WorkflowID: workflowID, RunID: runID, Name: signalName, Input: input, RequestID: newID(),
	}))
}

func (c *workflowClient) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
	options StartWorkflowOptions, workflowType string, workflowArgs ...any,
) (WorkflowRun, error) {
	if signalName == "" {
		return nil, errors.New("client: signal name is required")
	}
	if workflowType == "" {
		return nil, errors.New("client: workflow type is required")
	}
	if options.ID != "" && options.ID != workflowID {
		return nil, errors.New("client: StartWorkflowOptions.ID must be empty or equal to workflowID")
	}
	if options.WorkflowIDConflictPolicy != WorkflowIDConflictPolicyUnspecified {
		return nil, errors.New("client: SignalWithStartWorkflow always signals the running execution; leave WorkflowIDConflictPolicy unspecified")
	}
	signalInput, err := encodeObject("signal argument", []any{signalArg})
	if err != nil {
		return nil, err
	}
	startInput, err := encodeObject("workflow input", workflowArgs)
	if err != nil {
		return nil, err
	}
	if workflowID == "" {
		workflowID = newID()
	}
	resp, err := c.workflows().SignalWithStart(ctx, runtimeapi.WorkflowRuntimeSignalWithStartRequest{
		Namespace: c.namespace, WorkflowType: workflowType, WorkflowID: workflowID,
		StartInput: startInput, SignalName: signalName, SignalInput: signalInput, RequestID: newID(),
	})
	if err != nil {
		return nil, mapError(callStart, err)
	}
	return &workflowRun{client: c, workflowID: resp.WorkflowID, runID: resp.RunID}, nil
}

func (c *workflowClient) CancelWorkflow(ctx context.Context, workflowID, runID string) error {
	return mapError(callOther, c.workflows().CancelWorkflow(ctx, runtimeapi.WorkflowRuntimeCancelRequest{
		Namespace: c.namespace, WorkflowID: workflowID, RunID: runID,
	}))
}

func (c *workflowClient) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error {
	return mapError(callOther, c.workflows().TerminateWorkflow(ctx, runtimeapi.WorkflowRuntimeTerminateRequest{
		Namespace: c.namespace, WorkflowID: workflowID, RunID: runID, Reason: reason,
	}))
}

func (c *workflowClient) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (EncodedValue, error) {
	if queryType == "" {
		return nil, errors.New("client: query type is required")
	}
	input, err := encodeObject("query argument", args)
	if err != nil {
		return nil, err
	}
	result, err := c.workflows().QueryWorkflow(ctx, runtimeapi.WorkflowRuntimeInteractionRequest{
		Namespace: c.namespace, WorkflowID: workflowID, RunID: runID, Name: queryType, Input: input,
	})
	if err != nil {
		return nil, mapError(callOther, err)
	}
	if result.QueryRejected {
		return nil, &QueryFailedError{QueryType: queryType, Message: result.QueryError}
	}
	return objectValue{value: result.QueryResult}, nil
}
