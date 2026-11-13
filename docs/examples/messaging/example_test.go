package messaging_test

import (
	"context"

	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

type ChangeLimitResult struct {
	Previous int64 `json:"previous"`
	Current  int64 `json:"current"`
}

func LimitWorkflow(ctx workflow.Context, input map[string]any) error {
	limit := int64(100)
	if err := workflow.SetUpdateHandler(ctx, "change-limit", func(ctx workflow.Context, next int64) (ChangeLimitResult, error) {
		previous := limit
		limit = next
		return ChangeLimitResult{Previous: previous, Current: limit}, nil
	}); err != nil {
		return err
	}
	return workflow.SetQueryHandler(ctx, "current-limit", func(ctx workflow.Context, input map[string]any) map[string]any {
		return map[string]any{"limit": limit}
	})
}

func updateLimit(ctx context.Context, c client.Client, workflowID, updateID string) (ChangeLimitResult, error) {
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: workflowID, UpdateID: updateID, UpdateName: "change-limit",
		Args: []any{int64(500)}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return ChangeLimitResult{}, err
	}
	var result ChangeLimitResult
	err = handle.Get(ctx, &result)
	return result, err
}
