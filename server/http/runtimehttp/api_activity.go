package runtimehttp

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
	"github.com/gin-gonic/gin"
)

// ActivityRuntimeHTTPAPI 是 Activity Worker 领取、续约与上报任务的端点。
// @TAG(Runtime-Activity)
// @PREFIX(/runtime/activity)
type ActivityRuntimeHTTPAPI interface {
	// Poll 长轮询领取 Activity 任务
	// @POST(/poll)
	Poll(ctx *gin.Context, req runtimeapi.ActivityRuntimePollRequest) (protocol.TaskLease, error)

	// Ack 确认 Activity 任务租约
	// @POST(/ack)
	Ack(ctx *gin.Context, req runtimeapi.ActivityRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error)

	// Nack 归还 Activity 任务租约
	// @POST(/nack)
	Nack(ctx *gin.Context, req runtimeapi.ActivityRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error)

	// Begin 开始执行 Activity 任务
	// @POST(/begin)
	Begin(ctx *gin.Context, req runtimeapi.ActivityRuntimeBeginRequest) (protocol.ActivityTaskBeginResult, error)

	// Heartbeat 上报 Activity 心跳
	// @POST(/heartbeat)
	Heartbeat(ctx *gin.Context, req runtimeapi.ActivityRuntimeHeartbeatRequest) (protocol.ActivityHeartbeatResult, error)

	// Cancellation 查询 Activity 任务的取消状态
	// @POST(/cancellation)
	Cancellation(ctx *gin.Context, req runtimeapi.ActivityRuntimeTaskRequest) (protocol.ActivityCancellationResult, error)

	// Complete 上报 Activity 异步完成
	// @POST(/complete)
	Complete(ctx *gin.Context, req runtimeapi.ActivityRuntimeCompleteRequest) (runtimeapi.RuntimeOKResponse, error)

	// Fail 上报 Activity 失败
	// @POST(/fail)
	Fail(ctx *gin.Context, req runtimeapi.ActivityRuntimeFailRequest) (runtimeapi.RuntimeOKResponse, error)

	// Release 交还因 Worker 停止而没有结果的 Activity attempt
	// @POST(/release)
	Release(ctx *gin.Context, req runtimeapi.ActivityRuntimeTaskRequest) (runtimeapi.RuntimeOKResponse, error)
}

// ActivityRuntime 是 Activity 端点需要的 runtime：任务执行过程与任务队列。
type ActivityRuntime interface {
	activitytasks.API
	engine.ActivityWorkerQueueAPI
}

// NewActivityRuntimeHTTPHandler signs and verifies activity leases with codec; server faults are logged to logger.
func NewActivityRuntimeHTTPHandler(runtime ActivityRuntime, codec *dispatch.TaskTokenCodec, logger *slog.Logger) http.Handler {
	router := newRuntimeRouter(logger, nil)
	bindActivityRuntimeAPI(router, runtime, codec)
	return router
}

func bindActivityRuntimeAPI(router gin.IRoutes, runtime ActivityRuntime, codec *dispatch.TaskTokenCodec) {
	api := &activityRuntimeHTTPAPI{runtime: runtime, codec: codec}
	NewActivityRuntimeHTTPAPIWrap(api, api).BindAll(router)
}

type activityRuntimeHTTPAPI struct {
	runtime ActivityRuntime
	codec   *dispatch.TaskTokenCodec
}

func (a *activityRuntimeHTTPAPI) PreHandlers() []gin.HandlerFunc { return nil }

func (a *activityRuntimeHTTPAPI) Poll(ctx *gin.Context, req runtimeapi.ActivityRuntimePollRequest) (protocol.TaskLease, error) {
	worker := protocol.WorkerInfoFromRequest(ctx.Request)
	pollCtx, cancel := runtimePollContext(ctx.Request.Context(), req.TimeoutMillis)
	defer cancel()
	lease, err := a.runtime.PollActivityTask(protocol.WithWorkerInfo(protocol.WithNamespace(pollCtx, worker.Namespace), worker), req.TaskQueue)
	if err != nil {
		return protocol.TaskLease{}, runtimePollError(ctx, err)
	}
	if ctx.Request.Context().Err() != nil {
		// Worker 在任务交出之前断开（例如正在停止）：attempt 还没有开始，退回租约，队列立即重投。
		if err := a.runtime.NackActivityTask(context.WithoutCancel(ctx.Request.Context()), lease); err != nil {
			return protocol.TaskLease{}, err
		}
		return protocol.TaskLease{}, errSkipResponse
	}
	return signRuntimeTaskLease(a.codec, lease)
}

func (a *activityRuntimeHTTPAPI) Ack(ctx *gin.Context, req runtimeapi.ActivityRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error) {
	lease, err := verifyRuntimeTaskLease(a.codec, req.Lease, protocol.TaskTypeActivity)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.AckActivityTask(ctx.Request.Context(), lease))
}

func (a *activityRuntimeHTTPAPI) Nack(ctx *gin.Context, req runtimeapi.ActivityRuntimeLeaseRequest) (runtimeapi.RuntimeOKResponse, error) {
	lease, err := verifyRuntimeTaskLease(a.codec, req.Lease, protocol.TaskTypeActivity)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.NackActivityTask(ctx.Request.Context(), lease))
}

func (a *activityRuntimeHTTPAPI) Begin(ctx *gin.Context, req runtimeapi.ActivityRuntimeBeginRequest) (protocol.ActivityTaskBeginResult, error) {
	task, err := activityRuntimeRequestTask(a.codec, req.ActivityRuntimeTaskRequest)
	if err != nil {
		return protocol.ActivityTaskBeginResult{}, err
	}
	task.RequestID = req.RequestID
	return a.runtime.BeginActivityTask(ctx.Request.Context(), task)
}

func (a *activityRuntimeHTTPAPI) Heartbeat(ctx *gin.Context, req runtimeapi.ActivityRuntimeHeartbeatRequest) (protocol.ActivityHeartbeatResult, error) {
	task, err := activityRuntimeRequestTask(a.codec, req.ActivityRuntimeTaskRequest)
	if err != nil {
		return protocol.ActivityHeartbeatResult{}, err
	}
	return a.runtime.RecordActivityHeartbeat(ctx.Request.Context(), task, req.Details)
}

func (a *activityRuntimeHTTPAPI) Cancellation(ctx *gin.Context, req runtimeapi.ActivityRuntimeTaskRequest) (protocol.ActivityCancellationResult, error) {
	task, err := activityRuntimeRequestTask(a.codec, req)
	if err != nil {
		return protocol.ActivityCancellationResult{}, err
	}
	return a.runtime.CheckActivityTaskCancellation(ctx.Request.Context(), task)
}

func (a *activityRuntimeHTTPAPI) Complete(ctx *gin.Context, req runtimeapi.ActivityRuntimeCompleteRequest) (runtimeapi.RuntimeOKResponse, error) {
	task, err := activityRuntimeRequestTask(a.codec, req.ActivityRuntimeTaskRequest)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	if req.Failure != nil {
		// 入口校验复用 protocol.Failure.Validate（数量/字节上限与 canonical 化），超限直接 400。
		if err := req.Failure.Validate(); err != nil {
			return runtimeapi.RuntimeOKResponse{}, protocol.InvalidRequestf("invalid activity failure: %w", err)
		}
	}
	return runtimeOK(a.runtime.CompleteActivityTask(ctx.Request.Context(), task, req.Output, req.Failure))
}

func (a *activityRuntimeHTTPAPI) Fail(ctx *gin.Context, req runtimeapi.ActivityRuntimeFailRequest) (runtimeapi.RuntimeOKResponse, error) {
	task, err := activityRuntimeRequestTask(a.codec, req.ActivityRuntimeTaskRequest)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	if req.Failure == nil {
		return runtimeapi.RuntimeOKResponse{}, protocol.InvalidRequestf("activity failure is required")
	}
	return runtimeOK(a.runtime.FailActivityTask(ctx.Request.Context(), task, *req.Failure))
}

func (a *activityRuntimeHTTPAPI) Release(ctx *gin.Context, req runtimeapi.ActivityRuntimeTaskRequest) (runtimeapi.RuntimeOKResponse, error) {
	task, err := activityRuntimeRequestTask(a.codec, req)
	if err != nil {
		return runtimeapi.RuntimeOKResponse{}, err
	}
	return runtimeOK(a.runtime.ReleaseActivityTask(ctx.Request.Context(), task))
}

// activityRuntimeRequestTask 用 token 里签过名的 task 作为本次请求的任务，Worker 身份取自请求。
func activityRuntimeRequestTask(codec *dispatch.TaskTokenCodec, req runtimeapi.ActivityRuntimeTaskRequest) (protocol.Task, error) {
	token := req.TaskToken
	if token == "" {
		return protocol.Task{}, protocol.InvalidRequestf("activity runtime task token is empty")
	}
	decoded, err := codec.Decode(token)
	if err != nil {
		return protocol.Task{}, err
	}
	if decoded.Type != protocol.TaskTypeActivity {
		return protocol.Task{}, protocol.InvalidRequestf("task token type %s is invalid for activity task", decoded.Type)
	}
	decoded.TaskToken = token
	decoded.WorkerIdentity = req.WorkerIdentity
	decoded.WorkerBuildID = req.WorkerBuildID
	return decoded, nil
}
