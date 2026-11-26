package runtimehttp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/gin-gonic/gin"
)

// WorkflowTaskRuntimeHTTPAPI 是 Workflow Worker 领取与完成 WorkflowTask 的端点。
// @TAG(Runtime-WorkflowTask)
// @PREFIX(/runtime/workflow-task)
type WorkflowTaskRuntimeHTTPAPI interface {
	// Poll 长轮询领取 WorkflowTask
	// @POST(/poll)
	Poll(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimePollRequest) (protocol.TaskLease, error)

	// Ack 确认 WorkflowTask 租约
	// @POST(/ack)
	Ack(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error)

	// Nack 归还 WorkflowTask 租约
	// @POST(/nack)
	Nack(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error)

	// Config 读取 task token 所属 Run 的 workflow 配置
	// @POST(/config)
	Config(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) (*program.WorkflowConfig, error)

	// Events 读取 task token 所属 Run 的完整 history
	// @POST(/events)
	Events(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) ([]history.Event, error)

	// Status 读取 task token 所属 Run 的状态
	// @POST(/status)
	Status(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) (runtimeapi.WorkflowTaskRuntimeStatusResponse, error)

	// Complete 提交全量 replay 的 WorkflowTask 命令
	// @POST(/complete)
	Complete(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeCompleteRequest) (runtimeapi.RuntimeOKResponse, error)

	// TimedOut 上报 WorkflowTask 超时
	// @POST(/timed-out)
	TimedOut(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeTimedOutRequest) (runtimeapi.RuntimeOKResponse, error)

	// Failed 上报 WorkflowTask 失败
	// @POST(/failed)
	Failed(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeFailedRequest) (runtimeapi.RuntimeOKResponse, error)

	// Activation 读取增量 activation
	// @POST(/activation)
	Activation(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeActivationRequest) (history.WorkflowActivation, error)

	// CompleteActivation 提交增量 activation 的结果
	// @POST(/complete-activation)
	CompleteActivation(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeCompleteActivationRequest) (history.WorkflowTaskCompletionAck, error)
}

// NewWorkflowTaskRuntimeHTTPHandler verifies workflow completions in the lease trust domain of codec;
// server faults are logged to logger.
func NewWorkflowTaskRuntimeHTTPHandler(runtime engine.WorkflowTaskRuntimeAPI, codec *dispatch.TaskTokenCodec, logger *slog.Logger) http.Handler {
	router := newRuntimeRouter(logger, nil)
	bindWorkflowTaskRuntimeAPI(router, runtime, codec)
	return router
}

func bindWorkflowTaskRuntimeAPI(router gin.IRoutes, runtime engine.WorkflowTaskRuntimeAPI, codec *dispatch.TaskTokenCodec) {
	api := &workflowTaskRuntimeHTTPAPI{runtime: runtime, codec: codec}
	NewWorkflowTaskRuntimeHTTPAPIWrap(api, api).BindAll(router)
}

type workflowTaskRuntimeHTTPAPI struct {
	runtime engine.WorkflowTaskRuntimeAPI
	codec   *dispatch.TaskTokenCodec
}

func (a *workflowTaskRuntimeHTTPAPI) PreHandlers() []gin.HandlerFunc { return nil }

func (a *workflowTaskRuntimeHTTPAPI) Poll(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimePollRequest) (protocol.TaskLease, error) {
	pollCtx, cancel := runtimePollContext(ctx.Request.Context(), req.TimeoutMillis)
	defer cancel()
	lease, err := a.runtime.PollWorkflowTask(protocol.WithWorkerInfo(pollCtx, protocol.WorkerInfoFromRequest(ctx.Request)))
	if err != nil {
		return protocol.TaskLease{}, runtimePollError(ctx, err)
	}
	signed, err := signRuntimeTaskLease(a.codec, lease)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	if ctx.Request.Context().Err() != nil {
		// Worker 在任务交出之前断开（例如正在停止）：任务已 started，应答送不到。记为超时并确认这份投递，
		// 下一个 Workflow Task 立即调度，不等 start-to-close 超时。
		if err := a.returnWorkflowTask(context.WithoutCancel(ctx.Request.Context()), lease, signed.Task.TaskToken); err != nil {
			return protocol.TaskLease{}, err
		}
		return protocol.TaskLease{}, errSkipResponse
	}
	return signed, nil
}

// workflowTaskReturnedMessage 是 Worker 断开、Server 交还已 started 的 Workflow Task 时 WorkflowTaskTimedOut 的错误文本。
const workflowTaskReturnedMessage = "worker disconnected before receiving the workflow task"

func (a *workflowTaskRuntimeHTTPAPI) returnWorkflowTask(ctx context.Context, lease protocol.TaskLease, token protocol.TaskToken) error {
	taskCtx := protocol.WithWorkflowTaskToken(ctx, token)
	if err := a.runtime.HandleWorkflowTaskTimedOut(taskCtx, lease.Task.RunID, lease.Task.ScheduledEventID, workflowTaskReturnedMessage); err != nil {
		return fmt.Errorf("return workflow task of run %s: %w", lease.Task.RunID, err)
	}
	return a.runtime.AckWorkflowTask(ctx, lease)
}

func (a *workflowTaskRuntimeHTTPAPI) Ack(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error) {
	lease, err := verifyRuntimeTaskLease(a.codec, req.Lease, protocol.TaskTypeWorkflow)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.AckWorkflowTask(ctx.Request.Context(), lease))
}

func (a *workflowTaskRuntimeHTTPAPI) Nack(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error) {
	lease, err := verifyRuntimeTaskLease(a.codec, req.Lease, protocol.TaskTypeWorkflow)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.NackWorkflowTask(ctx.Request.Context(), lease))
}

// Config、Events 与 Status 读取 task token 所属的 Run。
func (a *workflowTaskRuntimeHTTPAPI) Config(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) (*program.WorkflowConfig, error) {
	runID, _, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return nil, err
	}
	return a.runtime.GetWorkflowConfig(ctx.Request.Context(), runID, req.ProgramVersion)
}

func (a *workflowTaskRuntimeHTTPAPI) Events(_ *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) ([]history.Event, error) {
	runID, _, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return nil, err
	}
	return a.runtime.GetEvents(runID)
}

func (a *workflowTaskRuntimeHTTPAPI) Status(_ *gin.Context, req runtimeapi.WorkflowTaskRuntimeRunRequest) (runtimeapi.WorkflowTaskRuntimeStatusResponse, error) {
	runID, _, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return runtimeapi.WorkflowTaskRuntimeStatusResponse{}, err
	}
	status, err := a.runtime.GetRunStatus(runID)
	return runtimeapi.WorkflowTaskRuntimeStatusResponse{Status: status}, err
}

func (a *workflowTaskRuntimeHTTPAPI) Complete(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeCompleteRequest) (runtimeapi.RuntimeOKResponse, error) {
	runID, scheduledEventID, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.HandleWorkflowTaskCompleted(a.taskContext(ctx, req.TaskToken), runID, scheduledEventID, req.Commands, req.WorkerIdentity))
}

func (a *workflowTaskRuntimeHTTPAPI) TimedOut(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeTimedOutRequest) (runtimeapi.RuntimeOKResponse, error) {
	runID, scheduledEventID, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.HandleWorkflowTaskTimedOut(a.taskContext(ctx, req.TaskToken), runID, scheduledEventID, req.Error))
}

// Failed 只接受 Worker 可以上报的失败原因（history.IsWorkerWorkflowTaskFailureCause）：unhandled_events、run_resumed、
// reset_workflow 等原因由 Server 写入，按「重做」处理（不计连续失败、不退避），Worker 上报它们会绕过失败退避。
func (a *workflowTaskRuntimeHTTPAPI) Failed(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeFailedRequest) (runtimeapi.RuntimeOKResponse, error) {
	if !history.IsWorkerWorkflowTaskFailureCause(req.Cause) {
		return runtimeapi.RuntimeOKResponse{}, protocol.InvalidRequestf("workflow task failure cause %q cannot be reported by a worker; accepted causes: %s, %s, %s, %s",
			req.Cause, history.WorkflowTaskFailedCauseNonDeterministic, history.WorkflowTaskFailedCauseSessionInvalid, history.WorkflowTaskFailedCauseWorkflowPanic,
			history.WorkflowTaskFailedCausePayloadSizeLimitExceeded)
	}
	runID, scheduledEventID, err := workflowTaskRequestRef(a.codec, req.TaskToken)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.HandleWorkflowTaskFailed(a.taskContext(ctx, req.TaskToken), runID, scheduledEventID, req.Cause, req.Reason))
}

func (a *workflowTaskRuntimeHTTPAPI) Activation(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeActivationRequest) (history.WorkflowActivation, error) {
	task, err := workflowTaskFromToken(a.codec, req.TaskToken)
	if err != nil {
		return history.WorkflowActivation{}, err
	}
	return a.runtime.GetWorkflowActivation(ctx.Request.Context(), history.WorkflowActivationRequest{
		Task:                task,
		AfterEventID:        req.AfterEventID,
		ExecutionGeneration: req.ExecutionGeneration,
		ActivationID:        req.ActivationID,
	})
}

func (a *workflowTaskRuntimeHTTPAPI) CompleteActivation(ctx *gin.Context, req runtimeapi.WorkflowTaskRuntimeCompleteActivationRequest) (history.WorkflowTaskCompletionAck, error) {
	task, err := workflowTaskFromToken(a.codec, req.TaskToken)
	if err != nil {
		return history.WorkflowTaskCompletionAck{}, err
	}
	return a.runtime.CompleteWorkflowActivation(a.taskContext(ctx, req.TaskToken), history.WorkflowTaskCompletionRequest{Task: task, Activation: req.Activation, Commands: req.Commands})
}

// taskContext 把本次请求的 task token 带给 runtime 的完成校验。
func (a *workflowTaskRuntimeHTTPAPI) taskContext(ctx *gin.Context, token protocol.TaskToken) context.Context {
	return protocol.WithWorkflowTaskToken(ctx.Request.Context(), token)
}

func workflowTaskRequestRef(codec *dispatch.TaskTokenCodec, token protocol.TaskToken) (string, int64, error) {
	task, err := workflowTaskFromToken(codec, token)
	if err != nil {
		return "", 0, err
	}
	return task.RunID, task.ScheduledEventID, nil
}

// workflowTaskFromToken 解出 token 里的 task，并要求它是 Workflow Task。
func workflowTaskFromToken(codec *dispatch.TaskTokenCodec, token protocol.TaskToken) (protocol.Task, error) {
	if token == "" {
		return protocol.Task{}, protocol.InvalidRequestf("workflow task token is empty")
	}
	task, err := codec.Decode(token)
	if err != nil {
		return protocol.Task{}, err
	}
	if task.Type != protocol.TaskTypeWorkflow {
		return protocol.Task{}, protocol.InvalidRequestf("task token type %s is invalid for workflow task", task.Type)
	}
	return task, nil
}
