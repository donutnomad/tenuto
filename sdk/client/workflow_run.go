package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// WorkflowIDConflictPolicy 决定 Workflow ID 已有运行中的 Run 时启动请求怎么处理。
type WorkflowIDConflictPolicy int

const (
	// WorkflowIDConflictPolicyUnspecified 采用调用的缺省行为：ExecuteWorkflow 失败，SignalWithStartWorkflow
	// 把 Signal 投给运行中的 Run；UpdateWithStartWorkflow 不接受它。
	WorkflowIDConflictPolicyUnspecified WorkflowIDConflictPolicy = iota
	// WorkflowIDConflictPolicyFail 返回 ErrWorkflowExecutionAlreadyStarted。只用于 ExecuteWorkflow。
	WorkflowIDConflictPolicyFail
	// WorkflowIDConflictPolicyUseExisting 使用运行中的 Run。只用于 UpdateWithStartWorkflow。
	WorkflowIDConflictPolicyUseExisting
)

// StartWorkflowOptions 是启动 Workflow 的选项。执行超时、Run 超时与重试策略来自 `@Workflow` 注解生成的
// 配置，不在这里设置。
type StartWorkflowOptions struct {
	// ID 是 Workflow ID，标识这次执行；为空时 SDK 生成一个 UUID。同一 ID 同时至多一个运行中的 Run，
	// 上一个 Run 结束后可以用同一 ID 再启动。
	ID string
	// WorkflowIDConflictPolicy 见各常量的说明。
	WorkflowIDConflictPolicy WorkflowIDConflictPolicy
}

// WorkflowRun 是一次 Workflow 执行的句柄。
type WorkflowRun interface {
	// GetID 返回 Workflow ID。
	GetID() string
	// GetRunID 返回句柄创建时的 Run ID；GetWorkflow 传入空 runID 时为空。
	GetRunID() string
	// Get 等待执行结束并把结果解码进 valuePtr（非 nil 指针，或 nil 表示丢弃结果）。
	//
	// Run 以 continue-as-new 结束、或失败后按 `@Workflow` 的重试策略开始下一次尝试时，Get 跟到新 Run，
	// 直到链上最后一个 Run 结束。成功时返回 nil；否则返回 *WorkflowExecutionError，Unwrap 得到原因。
	//
	// Get 经 Server 的 wait-closed 长轮询等待 Run 结束（一次最长 20s，到期后再发）：Run 由处理请求的
	// Server 副本结束时立即返回，由其他副本结束时最多晚 100ms。ctx 取消或到期时立即返回 ctx 的错误。
	Get(ctx context.Context, valuePtr any) error
}

func (c *workflowClient) ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflowType string, args ...any) (WorkflowRun, error) {
	if workflowType == "" {
		return nil, errors.New("client: workflow type is required")
	}
	switch options.WorkflowIDConflictPolicy {
	case WorkflowIDConflictPolicyUnspecified, WorkflowIDConflictPolicyFail:
	default:
		return nil, fmt.Errorf("client: ExecuteWorkflow supports WorkflowIDConflictPolicyFail only")
	}
	input, err := encodeObject("workflow input", args)
	if err != nil {
		return nil, err
	}
	// Workflow ID 与 request ID 在发送之前确定，重试的每次发送都相同：Server 按 request ID 认出重发的启动，
	// 返回第一次启动的 Run。
	workflowID := options.ID
	if workflowID == "" {
		workflowID = newID()
	}
	resp, err := c.workflows().StartWorkflow(ctx, runtimeapi.WorkflowRuntimeStartRequest{
		Namespace: c.namespace, WorkflowType: workflowType, WorkflowID: workflowID, Input: input, RequestID: newID(),
	})
	if err != nil {
		return nil, mapError(callStart, err)
	}
	return &workflowRun{client: c, workflowID: resp.WorkflowID, runID: resp.RunID}, nil
}

func (c *workflowClient) GetWorkflow(_ context.Context, workflowID, runID string) WorkflowRun {
	return &workflowRun{client: c, workflowID: workflowID, runID: runID}
}

type workflowRun struct {
	client     *workflowClient
	workflowID string
	runID      string
}

func (r *workflowRun) GetID() string    { return r.workflowID }
func (r *workflowRun) GetRunID() string { return r.runID }

// runWaitTimeout 是 Get 的一次 wait-closed 长轮询在 Server 上的等待时长，与 Update 等待结果的软超时相同。
const runWaitTimeout = 20 * time.Second

func (r *workflowRun) Get(ctx context.Context, valuePtr any) error {
	if err := checkDecodeTarget(valuePtr); err != nil {
		return err
	}
	runID, err := r.client.resolveRunID(ctx, r.workflowID, r.runID)
	if err != nil {
		return err
	}
	for {
		closed, err := r.waitClosed(ctx, runID)
		if err != nil {
			return err
		}
		next, err := r.outcome(closed.Run, closed.CloseEvent, valuePtr)
		if next == "" {
			return err
		}
		runID = next
	}
}

// waitClosed 以 wait-closed 长轮询等待 Run 结束；Server 的一次等待到期时再发一次，直到 Run 结束或 ctx 结束。
func (r *workflowRun) waitClosed(ctx context.Context, runID string) (runtimeapi.WorkflowRuntimeWaitClosedResponse, error) {
	for {
		resp, err := r.client.workflows().WaitRunClosed(ctx, runtimeapi.WorkflowRuntimeWaitClosedRequest{
			Namespace: r.client.namespace, WorkflowID: r.workflowID, RunID: runID, TimeoutMillis: runWaitTimeout.Milliseconds(),
		})
		switch {
		case err == nil:
			return resp, nil
		case ctx.Err() != nil:
			return runtimeapi.WorkflowRuntimeWaitClosedResponse{}, ctx.Err()
		case errors.Is(err, protocol.ErrTaskPollTimeout):
			continue
		default:
			return runtimeapi.WorkflowRuntimeWaitClosedResponse{}, mapError(callOther, err)
		}
	}
}

// outcome 按 Run 的结束事件给出结果；返回非空的 next 表示继续跟随到这个 Run。
func (r *workflowRun) outcome(run *runtimeapi.Run, closing history.Event, valuePtr any) (next string, err error) {
	executionErr := func(cause error) error {
		return &WorkflowExecutionError{WorkflowID: run.WorkflowID, RunID: run.ID, WorkflowType: run.WorkflowType, cause: cause}
	}
	switch {
	case closing.WorkflowCompleted != nil:
		return "", decodeObject(closing.WorkflowCompleted.Output, valuePtr)
	case closing.WorkflowContinuedAsNew != nil:
		return closing.WorkflowContinuedAsNew.NextRunID, nil
	case closing.WorkflowFailed != nil:
		failed := closing.WorkflowFailed
		if failed.NewExecutionRunID != "" {
			return failed.NewExecutionRunID, nil
		}
		failure := protocol.Failure{Message: failed.Error}
		if failed.Failure != nil {
			failure = *failed.Failure
		}
		if failed.TimeoutType != "" {
			failure.TimeoutType = protocol.TimeoutType(failed.TimeoutType)
		}
		return "", executionErr(apperror.FromFailure(failure, decodePayload))
	case closing.WorkflowCancelled != nil:
		canceled := protocol.Failure{Message: "canceled", Type: apperror.CanceledType, NonRetryable: true, Details: closing.WorkflowCancelled.Details}
		return "", executionErr(apperror.FromFailure(canceled, decodePayload))
	case closing.WorkflowTerminated != nil:
		return "", executionErr(&TerminatedError{Reason: closing.WorkflowTerminated.Reason})
	}
	return "", fmt.Errorf("client: run %s is %s but its history has no closing event", run.ID, run.Status)
}

func (c *workflowClient) getRun(ctx context.Context, workflowID, runID string) (runtimeapi.RunWithBlocked, error) {
	resp, err := c.workflows().GetRun(ctx, runtimeapi.WorkflowRuntimeRunRequest{Namespace: c.namespace, WorkflowID: workflowID, RunID: runID})
	return resp, mapError(callOther, err)
}
