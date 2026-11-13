package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// WorkflowUpdateStage 是 UpdateWorkflow 返回句柄之前等到的阶段。
type WorkflowUpdateStage int

const (
	// WorkflowUpdateStageUnspecified 等同于 WorkflowUpdateStageAccepted。
	WorkflowUpdateStageUnspecified WorkflowUpdateStage = iota
	// WorkflowUpdateStageAccepted 在 Update 被 Run 接受（写入 WorkflowUpdateAccepted）后返回。
	WorkflowUpdateStageAccepted
	// WorkflowUpdateStageCompleted 在 handler 返回结果后返回。
	WorkflowUpdateStageCompleted
)

// UpdateWorkflowOptions 描述一次 Update。
type UpdateWorkflowOptions struct {
	// UpdateID 让重复发送的同一 Update 只执行一次；为空时 SDK 生成一个 UUID，重试的每次发送都用它。
	UpdateID string
	// WorkflowID 与 RunID 寻址目标 Run；RunID 为空时发给 Workflow ID 的当前 Run。
	WorkflowID string
	RunID      string
	// UpdateName 是 workflow.SetUpdateHandler 登记的名字。
	UpdateName string
	// Args 按 handler 业务参数的顺序排列，每个编码成一个 json/plain-v1 payload。
	Args []any
	// WaitForStage 是返回句柄之前等到的阶段。
	WaitForStage WorkflowUpdateStage
}

// WorkflowUpdateHandle 跟踪一次 Update 的结果。
type WorkflowUpdateHandle interface {
	// WorkflowID 返回 Update 所在的 Workflow ID。
	WorkflowID() string
	// RunID 返回接受 Update 的 Run。之后的等待固定在这个 Run 上，不随 continue-as-new 漂到新 Run。
	RunID() string
	// UpdateID 返回 Update ID。
	UpdateID() string
	// Get 等待 handler 的结果并解码进 valuePtr（非 nil 指针，或 nil 表示丢弃结果）。handler 返回错误、
	// 或 Run 在 Update 完成之前结束时返回 *apperror.Error。ctx 取消时返回 ctx 的错误，句柄仍可再次 Get。
	Get(ctx context.Context, valuePtr any) error
}

func (c *workflowClient) UpdateWorkflow(ctx context.Context, options UpdateWorkflowOptions) (WorkflowUpdateHandle, error) {
	if options.WorkflowID == "" && options.RunID == "" {
		return nil, errors.New("client: workflow ID or run ID is required")
	}
	if options.UpdateName == "" {
		return nil, errors.New("client: update name is required")
	}
	stage, err := waitStage(options.WaitForStage)
	if err != nil {
		return nil, err
	}
	args, err := encodePayloads(options.Args)
	if err != nil {
		return nil, err
	}
	if options.UpdateID == "" {
		options.UpdateID = newID()
	}
	result, err := c.workflows().UpdateWorkflow(ctx, runtimeapi.WorkflowRuntimeUpdateRequest{
		Namespace: c.namespace, WorkflowID: options.WorkflowID, RunID: options.RunID, Name: options.UpdateName,
		UpdateID: options.UpdateID, WaitStage: string(stage), Args: args,
	})
	if err != nil {
		return nil, mapError(callUpdate, err)
	}
	return c.newUpdateHandle(options.WorkflowID, options.RunID, options.UpdateName, options.UpdateID, args, result)
}

// newUpdateHandle 用首个应答建句柄：按 Workflow ID 发出的 Update 固定在接受它的 Run 上。
func (c *workflowClient) newUpdateHandle(workflowID, runID, updateName, updateID string, args protocol.Payloads, result protocol.WorkflowUpdateResult) (*updateHandle, error) {
	if err := checkReachedStage(result); err != nil {
		return nil, err
	}
	if result.UpdateID != "" {
		updateID = result.UpdateID
	}
	if updateID == "" {
		return nil, errors.New("client: server returned an empty update ID")
	}
	if runID == "" {
		runID = result.RunID
	}
	return &updateHandle{
		client: c, workflowID: workflowID, runID: runID, updateID: updateID,
		updateName: updateName, args: args, result: result,
	}, nil
}

func waitStage(stage WorkflowUpdateStage) (protocol.UpdateWaitStage, error) {
	switch stage {
	case WorkflowUpdateStageUnspecified, WorkflowUpdateStageAccepted:
		return protocol.UpdateWaitStageEnums.Accepted, nil
	case WorkflowUpdateStageCompleted:
		return protocol.UpdateWaitStageEnums.Completed, nil
	default:
		return "", fmt.Errorf("client: unsupported WorkflowUpdateStage %d", stage)
	}
}

func checkReachedStage(result protocol.WorkflowUpdateResult) error {
	switch result.Stage {
	case protocol.UpdateReachedStageEnums.Accepted, protocol.UpdateReachedStageEnums.Completed:
		return nil
	default:
		return fmt.Errorf("client: server returned unsupported update stage %q", result.Stage)
	}
}

type updateHandle struct {
	client     *workflowClient
	workflowID string
	runID      string
	updateID   string
	updateName string
	args       protocol.Payloads

	mu      sync.Mutex
	result  protocol.WorkflowUpdateResult
	polling bool
	changed chan struct{}
}

func (h *updateHandle) WorkflowID() string { return h.workflowID }
func (h *updateHandle) RunID() string      { return h.runID }
func (h *updateHandle) UpdateID() string   { return h.updateID }

func (h *updateHandle) Get(ctx context.Context, valuePtr any) error {
	if err := checkDecodeTarget(valuePtr); err != nil {
		return err
	}
	result, err := h.completed(ctx)
	if err != nil {
		return err
	}
	if result.Outcome == nil {
		return fmt.Errorf("client: update %s completed without an outcome", h.updateID)
	}
	if err := result.Outcome.Validate(); err != nil {
		return fmt.Errorf("client: invalid update outcome: %w", err)
	}
	if result.Outcome.Failure != nil {
		return apperror.FromFailure(*result.Outcome.Failure, decodePayload)
	}
	if valuePtr == nil {
		return nil
	}
	return decodePayload(*result.Outcome.Success, valuePtr)
}

// completed 返回 Completed 阶段的结果。并发的 Get 共用一个轮询：一个调用方轮询，其余等它的结果。
func (h *updateHandle) completed(ctx context.Context) (protocol.WorkflowUpdateResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return protocol.WorkflowUpdateResult{}, err
		}
		h.mu.Lock()
		if h.result.Stage == protocol.UpdateReachedStageEnums.Completed {
			result := h.result
			h.mu.Unlock()
			return result, nil
		}
		if h.polling {
			changed := h.changed
			h.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return protocol.WorkflowUpdateResult{}, ctx.Err()
			}
		}
		h.polling = true
		h.changed = make(chan struct{})
		changed := h.changed
		h.mu.Unlock()

		result, err := h.pollCompleted(ctx)
		h.mu.Lock()
		if err == nil {
			h.result = result
		}
		h.polling = false
		close(changed)
		h.mu.Unlock()
		if err != nil {
			return protocol.WorkflowUpdateResult{}, err
		}
	}
}

// pollCompleted 以 Completed 阶段重发同一个 Update（按 Update ID 幂等）。Server 的长轮询在软超时时
// 返回 Accepted，这里接着再发，直到 Completed 或 ctx 结束。
func (h *updateHandle) pollCompleted(ctx context.Context) (protocol.WorkflowUpdateResult, error) {
	for {
		result, err := h.client.workflows().UpdateWorkflow(ctx, runtimeapi.WorkflowRuntimeUpdateRequest{
			Namespace: h.client.namespace, WorkflowID: h.workflowID, RunID: h.runID, Name: h.updateName,
			UpdateID: h.updateID, WaitStage: string(protocol.UpdateWaitStageEnums.Completed), Args: h.args,
		})
		if err != nil {
			return protocol.WorkflowUpdateResult{}, mapError(callUpdate, err)
		}
		if err := checkReachedStage(result); err != nil {
			return protocol.WorkflowUpdateResult{}, err
		}
		if result.Stage == protocol.UpdateReachedStageEnums.Completed {
			return result, nil
		}
	}
}

// WithStartWorkflowOperation 是 UpdateWithStartWorkflow 使用的一次性启动规格。
type WithStartWorkflowOperation interface {
	// Get 等到 UpdateWithStartWorkflow 执行这个规格，返回接受 Update 的 Run（新启动或已在运行的）。
	Get(ctx context.Context) (WorkflowRun, error)
}

// UpdateWithStartWorkflowOptions 组合启动规格与 Update。UpdateOptions 的 WorkflowID 与 RunID 必须为空，
// 目标来自启动规格的 ID。
type UpdateWithStartWorkflowOptions struct {
	StartWorkflowOperation WithStartWorkflowOperation
	UpdateOptions          UpdateWorkflowOptions
}

type withStartOperation struct {
	client       *workflowClient
	options      StartWorkflowOptions
	workflowType string
	input        map[string]any
	prepareErr   error

	executed atomic.Bool
	done     chan struct{}
	once     sync.Once
	run      WorkflowRun
	err      error
}

func (c *workflowClient) NewWithStartWorkflowOperation(options StartWorkflowOptions, workflowType string, args ...any) WithStartWorkflowOperation {
	op := &withStartOperation{client: c, options: options, workflowType: workflowType, done: make(chan struct{})}
	switch {
	case options.ID == "":
		op.prepareErr = errors.New("client: StartWorkflowOptions.ID is required for update-with-start")
	case options.WorkflowIDConflictPolicy != WorkflowIDConflictPolicyUseExisting:
		op.prepareErr = errors.New("client: update-with-start requires WorkflowIDConflictPolicyUseExisting")
	case workflowType == "":
		op.prepareErr = errors.New("client: workflow type is required")
	default:
		op.input, op.prepareErr = encodeObject("workflow input", args)
	}
	return op
}

func (op *withStartOperation) Get(ctx context.Context) (WorkflowRun, error) {
	select {
	case <-op.done:
		return op.run, op.err
	case <-ctx.Done():
		if !op.executed.Load() {
			return nil, fmt.Errorf("%w: start workflow operation was not executed", ctx.Err())
		}
		return nil, ctx.Err()
	}
}

func (op *withStartOperation) set(run WorkflowRun, err error) {
	op.once.Do(func() {
		op.run, op.err = run, err
		close(op.done)
	})
}

func (c *workflowClient) UpdateWithStartWorkflow(ctx context.Context, options UpdateWithStartWorkflowOptions) (WorkflowUpdateHandle, error) {
	op, ok := options.StartWorkflowOperation.(*withStartOperation)
	if !ok || op == nil {
		return nil, errors.New("client: StartWorkflowOperation must come from NewWithStartWorkflowOperation")
	}
	if op.client != c {
		return nil, errors.New("client: StartWorkflowOperation belongs to a different client")
	}
	if op.executed.Swap(true) {
		return nil, errors.New("client: StartWorkflowOperation was already executed")
	}
	handle, err := c.updateWithStart(ctx, op, options.UpdateOptions)
	if err != nil {
		op.set(nil, err)
	}
	return handle, err
}

func (c *workflowClient) updateWithStart(ctx context.Context, op *withStartOperation, update UpdateWorkflowOptions) (WorkflowUpdateHandle, error) {
	if op.prepareErr != nil {
		return nil, op.prepareErr
	}
	if update.WorkflowID != "" || update.RunID != "" {
		return nil, errors.New("client: update-with-start takes its target from StartWorkflowOptions.ID")
	}
	if update.UpdateName == "" {
		return nil, errors.New("client: update name is required")
	}
	stage, err := waitStage(update.WaitForStage)
	if err != nil {
		return nil, err
	}
	args, err := encodePayloads(update.Args)
	if err != nil {
		return nil, err
	}
	if update.UpdateID == "" {
		update.UpdateID = newID()
	}
	resp, err := c.workflows().UpdateWithStart(ctx, runtimeapi.WorkflowRuntimeUpdateWithStartRequest{
		Namespace: c.namespace, WorkflowType: op.workflowType, WorkflowID: op.options.ID, StartInput: op.input,
		UpdateName: update.UpdateName, UpdateID: update.UpdateID, UpdateArgs: args, WaitStage: string(stage),
	})
	if err != nil {
		return nil, mapError(callUpdate, err)
	}
	op.set(&workflowRun{client: c, workflowID: resp.WorkflowID, runID: resp.RunID}, nil)
	return c.newUpdateHandle(resp.WorkflowID, resp.RunID, update.UpdateName, update.UpdateID, args, resp.Result)
}
