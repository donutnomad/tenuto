package client

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

// BatchOperationType 是批量操作对每个选中 Run 执行的动作。
type BatchOperationType string

const (
	// BatchOperationTypeCancel 请求取消每个 Run。
	BatchOperationTypeCancel BatchOperationType = "cancel"
	// BatchOperationTypeTerminate 终止每个 Run。
	BatchOperationTypeTerminate BatchOperationType = "terminate"
	// BatchOperationTypeReset 按 StartBatchOperationOptions.ResetType 重置每个 Run。
	BatchOperationTypeReset BatchOperationType = "reset"
)

// BatchOperationState 是批量操作本身的状态。
type BatchOperationState string

const (
	// BatchOperationStatePending 已受理，尚未开始处理。
	BatchOperationStatePending BatchOperationState = "pending"
	// BatchOperationStateRunning 正在分页处理。
	BatchOperationStateRunning BatchOperationState = "running"
	// BatchOperationStateCompleted 全部 Run 已处理，至少一个成功或没有失败。
	BatchOperationStateCompleted BatchOperationState = "completed"
	// BatchOperationStateFailed 全部 Run 已处理，有失败且没有一个成功。
	BatchOperationStateFailed BatchOperationState = "failed"
	// BatchOperationStateCanceled 在处理完之前被 StopBatchOperation 取消。
	BatchOperationStateCanceled BatchOperationState = "cancelled"
)

// StartBatchOperationOptions 选出一批 Run 并指定动作。
type StartBatchOperationOptions struct {
	Type BatchOperationType
	// Filter 选出 Run，规则与 ListWorkflow 相同，区别是 Statuses 为空时只选运行中的 Run；分页字段不使用。
	Filter ListWorkflowOptions
	Reason string
	// ResetType 只用于 BatchOperationTypeReset。
	ResetType ResetType
}

// BatchOperation 是批量操作的状态与进度。
type BatchOperation struct {
	ID        string
	Type      BatchOperationType
	State     BatchOperationState
	Reason    string
	ResetType ResetType
	// TotalCount 在处理第一页时取当时匹配的 Run 数，结束时改为实际处理的 Run 数。
	TotalCount     int
	SucceededCount int
	FailedCount    int
	SkippedCount   int
	// Errors 是部分失败 Run 的原因样本。
	Errors     []BatchRunError
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	UpdatedAt  time.Time
}

// BatchRunError 是一个 Run 处理失败的原因。
type BatchRunError struct {
	RunID   string
	Message string
}

// ListBatchOperationsOptions 分页列出批量操作。
type ListBatchOperationsOptions struct {
	Offset int
	Limit  int
}

// ListBatchOperationsResult 是一页批量操作；Total 是总数。
type ListBatchOperationsResult struct {
	Operations []BatchOperation
	Total      int
}

func (c *workflowClient) StartBatchOperation(ctx context.Context, options StartBatchOperationOptions) (*BatchOperation, error) {
	if options.Type == "" {
		return nil, errors.New("client: StartBatchOperationOptions.Type is required")
	}
	statuses := make([]history.Status, len(options.Filter.Statuses))
	for i, status := range options.Filter.Statuses {
		statuses[i] = history.Status(status)
	}
	op, err := c.workflows().StartBatchOperation(ctx, runtimeapi.BatchStartRequest{
		Namespace: c.namespace, Type: string(options.Type), Reason: options.Reason, ResetType: string(options.ResetType),
		Filter: runtimeapi.RunListFilter{
			Namespace: c.namespace, WorkflowID: options.Filter.WorkflowID, Statuses: statuses,
			SearchAttributes: options.Filter.SearchAttributes,
		},
	})
	return batchOperation(op, err)
}

func (c *workflowClient) DescribeBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error) {
	return batchOperation(c.workflows().GetBatchOperation(ctx, runtimeapi.WorkflowRuntimeBatchRequest{Namespace: c.namespace, OperationID: operationID}))
}

func (c *workflowClient) StopBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error) {
	return batchOperation(c.workflows().CancelBatchOperation(ctx, runtimeapi.WorkflowRuntimeBatchRequest{Namespace: c.namespace, OperationID: operationID}))
}

func (c *workflowClient) ListBatchOperations(ctx context.Context, options ListBatchOperationsOptions) (*ListBatchOperationsResult, error) {
	result, err := c.workflows().ListBatchOperations(ctx, runtimeapi.WorkflowRuntimeListBatchesRequest{
		Namespace: c.namespace, Offset: options.Offset, Limit: options.Limit,
	})
	if err != nil {
		return nil, mapError(callOther, err)
	}
	out := &ListBatchOperationsResult{Total: result.Total, Operations: make([]BatchOperation, len(result.Items))}
	for i := range result.Items {
		op, _ := batchOperation(result.Items[i], nil)
		out.Operations[i] = *op
	}
	return out, nil
}

func batchOperation(op runtimeapi.BatchOperation, err error) (*BatchOperation, error) {
	if err != nil {
		return nil, mapError(callOther, err)
	}
	out := &BatchOperation{
		ID: op.ID, Type: BatchOperationType(op.Type), State: BatchOperationState(op.Status), Reason: op.Reason,
		ResetType: ResetType(op.ResetType), TotalCount: op.TotalCount, SucceededCount: op.SucceededCount,
		FailedCount: op.FailedCount, SkippedCount: op.SkippedCount, CreatedAt: op.CreatedAt,
		StartedAt: op.StartedAt, FinishedAt: op.FinishedAt, UpdatedAt: op.UpdatedAt,
	}
	for _, runErr := range op.Errors {
		out.Errors = append(out.Errors, BatchRunError{RunID: runErr.RunID, Message: runErr.Message})
	}
	return out, nil
}
