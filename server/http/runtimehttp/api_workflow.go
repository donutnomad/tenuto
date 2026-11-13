package runtimehttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
	"github.com/gin-gonic/gin"
)

// WorkflowRuntimeHTTPAPI 是面向外部调用方（SDK client、console、运维）的 workflow 端点。
// @TAG(Runtime-Workflow)
// @PREFIX(/runtime/workflow)
type WorkflowRuntimeHTTPAPI interface {
	// Register 登记 workflow 配置
	// @POST(/register)
	Register(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterRequest) (runtimeapi.WorkflowConfigRef, error)

	// SetWorkerTarget 指定 workflow 的目标 Worker
	// @POST(/set-worker-target)
	SetWorkerTarget(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSetWorkerTargetRequest) (runtimeapi.WorkflowConfigRef, error)

	// Start 按 workflow ID 启动 Run
	// @POST(/start)
	Start(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartRequest) (runtimeapi.WorkflowRuntimeStartResponse, error)

	// StartVersion 按配置版本启动 Run
	// @POST(/start-version)
	StartVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartVersionRequest) (runtimeapi.WorkflowRuntimeStartResponse, error)

	// Cancel 请求取消 Run
	// @POST(/cancel)
	Cancel(ctx *gin.Context, req runtimeapi.WorkflowRuntimeCancelRequest) (runtimeapi.RuntimeOKResponse, error)

	// Terminate 强制终止 Run
	// @POST(/terminate)
	Terminate(ctx *gin.Context, req runtimeapi.WorkflowRuntimeTerminateRequest) (runtimeapi.RuntimeOKResponse, error)

	// GetRun 读取 Run 及其阻塞信息
	// @POST(/get-run)
	GetRun(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) (*runtimeapi.RunWithBlocked, error)

	// WaitClosed 长轮询等待 Run 结束，返回结束后的 Run 与结束事件；到期时 Run 仍在运行则应答 204
	// @POST(/wait-closed)
	WaitClosed(ctx *gin.Context, req runtimeapi.WorkflowRuntimeWaitClosedRequest) (*runtimeapi.WorkflowRuntimeWaitClosedResponse, error)

	// DispatchPending 派发待处理任务
	// @POST(/dispatch-pending)
	DispatchPending(ctx *gin.Context) (runtimeapi.WorkflowDispatchResult, error)

	// RegisterActivityWorkerVersion 登记 Activity Worker 版本
	// @POST(/register-activity-worker-version)
	RegisterActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error)

	// HeartbeatActivityWorkerVersion 续约 Activity Worker 版本
	// @POST(/heartbeat-activity-worker-version)
	HeartbeatActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeHeartbeatActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error)

	// DeregisterActivityWorkerVersion 注销 Activity Worker 版本，它的路由立刻退出选路
	// @POST(/deregister-activity-worker-version)
	DeregisterActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeDeregisterActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error)

	// ListRuns 查询 Run 列表
	// @POST(/list)
	ListRuns(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListRunsRequest) (runtimeapi.RunListResult, error)

	// RunStats 统计 Run 状态分布
	// @POST(/run-stats)
	RunStats(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunStatsRequest) (runtimeapi.RunStats, error)

	// UpsertSearchAttributes 写入 Run 的搜索属性
	// @POST(/search-attributes)
	UpsertSearchAttributes(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSearchAttributesRequest) (runtimeapi.RuntimeOKResponse, error)

	// History 读取 Run 的完整 history
	// @POST(/history)
	History(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) ([]history.Event, error)

	// HistoryPage 分页读取 Run 的 history
	// @POST(/history-page)
	HistoryPage(ctx *gin.Context, req runtimeapi.WorkflowRuntimeHistoryPageRequest) (runtimeapi.HistoryPage, error)

	// Signal 向 Run 发送 signal
	// @POST(/signal)
	Signal(ctx *gin.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) (runtimeapi.WorkflowRuntimeSignalResponse, error)

	// Query 查询 Run 的状态
	// @POST(/query)
	Query(ctx *gin.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) (runtimeapi.WorkflowQueryResult, error)

	// Update 向 Run 发送 update，参数为 Payloads
	// @POST(/update)
	Update(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateRequest) (protocol.WorkflowUpdateResult, error)

	// UpdateWithStart 按 workflow type 启动（或复用）Run 并发送 update
	// @POST(/update-with-start)
	UpdateWithStart(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateWithStartRequest) (runtimeapi.WorkflowRuntimeUpdateWithStartResponse, error)

	// Capabilities 返回服务端支持的 workflow 能力
	// @POST(/capabilities)
	Capabilities(ctx *gin.Context) (runtimeapi.RuntimeCapabilities, error)

	// SignalWithStart 启动（或复用）Run 并发送 signal
	// @POST(/signal-with-start)
	SignalWithStart(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSignalWithStartRequest) (runtimeapi.WorkflowRuntimeSignalWithStartResponse, error)

	// GetConfig 读取 Run 的 workflow 配置
	// @POST(/get-config)
	GetConfig(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) (*program.WorkflowConfig, error)

	// RegisterArtifact 登记随二进制发布的 workflow 制品
	// @POST(/artifacts)
	RegisterArtifact(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterRequest) (runtimeapi.WorkflowConfigRef, error)

	// SetRunPolicy 设置 Run 的版本策略
	// @POST(/set-policy)
	SetRunPolicy(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSetRunPolicyRequest) (*runtimeapi.Run, error)

	// ListConfigs 查询 workflow 配置列表
	// @POST(/list-configs)
	ListConfigs(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListConfigsRequest) (runtimeapi.WorkflowConfigListResult, error)

	// ListWorkers 查询 Worker 列表
	// @POST(/list-workers)
	ListWorkers(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListWorkersRequest) (runtimeapi.WorkerListResult, error)

	// CreateSchedule 创建 Schedule
	// @POST(/create-schedule)
	CreateSchedule(ctx *gin.Context, req runtimeapi.ScheduleCreateRequest) (*runtimeapi.Schedule, error)

	// GetSchedule 读取 Schedule
	// @POST(/get-schedule)
	GetSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (*runtimeapi.Schedule, error)

	// UpdateSchedule 更新 Schedule
	// @POST(/update-schedule)
	UpdateSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateScheduleRequest) (*runtimeapi.Schedule, error)

	// DeleteSchedule 删除 Schedule
	// @POST(/delete-schedule)
	DeleteSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error)

	// PauseSchedule 暂停 Schedule
	// @POST(/pause-schedule)
	PauseSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error)

	// ResumeSchedule 恢复 Schedule
	// @POST(/resume-schedule)
	ResumeSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error)

	// ListSchedules 查询 Schedule 列表
	// @POST(/list-schedules)
	ListSchedules(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListSchedulesRequest) (runtimeapi.ScheduleListResult, error)

	// TriggerSchedule 立即触发 Schedule
	// @POST(/trigger-schedule)
	TriggerSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.WorkflowRuntimeTriggerScheduleResponse, error)

	// StartBatch 受理批量操作，应答 202 与 pending 记录，后台驱动异步处理
	// @POST(/start-batch)
	StartBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartBatchRequest) (*runtimeapi.BatchOperation, error)

	// GetBatch 读取批量操作的状态与进度
	// @POST(/get-batch)
	GetBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (*runtimeapi.BatchOperation, error)

	// ListBatches 查询批量操作列表
	// @POST(/list-batches)
	ListBatches(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListBatchesRequest) (runtimeapi.BatchListResult, error)

	// CancelBatch 取消批量操作，已处理的 Run 不回滚
	// @POST(/cancel-batch)
	CancelBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (*runtimeapi.BatchOperation, error)

	// Reset 把 Run 重置到指定位置并产生新 Run
	// @POST(/reset)
	Reset(ctx *gin.Context, req runtimeapi.WorkflowRuntimeResetRequest) (runtimeapi.WorkflowRuntimeResetResponse, error)

	// ResetActivity 重置 Activity 的重试状态
	// @POST(/reset-activity)
	ResetActivity(ctx *gin.Context, req runtimeapi.WorkflowRuntimeResetActivityRequest) (runtimeapi.RuntimeOKResponse, error)

	// ListTaskQueues 查询任务队列列表
	// @POST(/list-task-queues)
	ListTaskQueues(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListTaskQueuesRequest) (runtimeapi.TaskQueueListResult, error)

	// ListNamespaces 查询命名空间列表
	// @POST(/list-namespaces)
	ListNamespaces(ctx *gin.Context) (runtimeapi.NamespaceListResult, error)

	// ActivityStates 读取 Run 的 Activity 状态
	// @POST(/activity-states)
	ActivityStates(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) ([]*runtimeapi.ActivityState, error)
}

// workflowEngine 是 workflow 端点里由执行引擎实现的操作，*engine.Server 满足它。
type workflowEngine interface {
	StartWorkflow(ctx context.Context, namespace, workflowType, workflowID, requestID string, input map[string]any) (*execution.Run, error)
	StartWorkflowVersion(ctx context.Context, ref definition.WorkflowConfigRef, workflowID string, input map[string]any) (*execution.Run, error)
	CancelWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error
	TerminateWorkflow(ctx context.Context, namespace, workflowID, runID, reason string) error
	ResetWorkflow(ctx context.Context, req execution.ResetWorkflowRequest) (string, error)
	ResetActivity(namespace, runID, activityID string, opts execution.ResetActivityOptions) error
	SetRunPolicy(ctx context.Context, namespace, runID, policy string) (*execution.Run, error)
	DispatchPendingTasks(ctx context.Context) (dispatch.WorkflowDispatchResult, error)
	RegisterActivityWorkerVersion(ctx context.Context, worker protocol.ActivityWorkerVersion) error
	HeartbeatActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error
	DeregisterActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error
	UpsertSearchAttributes(ctx context.Context, namespace, workflowID, runID string, attrs map[string]string) error
	SignalWorkflow(ctx context.Context, namespace, workflowID, runID, name, requestID string, input map[string]any) (int64, error)
	QueryWorkflow(ctx context.Context, namespace, workflowID, runID, name string, input map[string]any) (execution.WorkflowQueryResult, error)
	UpdateWorkflow(ctx context.Context, namespace, workflowID, runID, name, updateID string, stage protocol.UpdateWaitStage, args protocol.Payloads) (protocol.WorkflowUpdateResult, error)
	UpdateWithStart(ctx context.Context, namespace, workflowType, workflowID string, startInput map[string]any, updateName, updateID string, args protocol.Payloads, stage protocol.UpdateWaitStage) (string, protocol.WorkflowUpdateResult, error)
	SignalWithStart(ctx context.Context, namespace, workflowType, workflowID string, startInput map[string]any, signalName, requestID string, signalInput map[string]any) (string, int64, error)
	ListWorkers(filter executionview.WorkerListFilter) (executionview.WorkerListResult, error)
	WaitRunClosed(ctx context.Context, namespace, workflowID, runID string) (*execution.Run, history.Event, error)
}

// workflowDefinitions 是登记 Workflow 内容与发布目标 Worker 配置的操作，*definitions.UseCase 满足它。
type workflowDefinitions interface {
	RegisterWorkflowInNamespace(namespace string, workflow *program.WorkflowConfig) (definition.WorkflowConfigRef, error)
	RegisterWorkflowArtifactInNamespace(namespace string, workflow *program.WorkflowConfig) (definition.WorkflowConfigRef, error)
	EnrollWorkflowInNamespace(namespace string, workflow *program.WorkflowConfig) (definition.WorkflowConfigRef, error)
	SetWorkflowWorkerTarget(namespace, workflowID, identity string) (definition.WorkflowConfigRef, error)
}

// runReads 是 Run、History、配置、Worker 与 namespace 的查询，*executionview.View 满足它。
type runReads interface {
	GetRunWithState(namespace, workflowID, runID string) (*execution.Run, *execution.WorkflowExecutionState, error)
	GetRunWithBlocked(namespace, workflowID, runID string) (*executionview.RunWithBlocked, error)
	GetEvents(namespace, workflowID, runID string) ([]history.Event, error)
	GetEventPage(namespace, workflowID string, request execution.HistoryPageRequest) (execution.HistoryPage, error)
	GetWorkflowConfig(namespace, runID, programVersion string) (*program.WorkflowConfig, error)
	GetActivityStates(namespace, runID string) ([]*execution.ActivityState, error)
	ListRuns(filter execution.RunListFilter) (execution.RunListResult, error)
	GetRunStats(namespace string) (execution.RunStats, error)
	ListWorkflowConfigs(namespace string) (executionview.WorkflowConfigListResult, error)
	ListTaskQueues(namespace string) (dispatch.TaskQueueListResult, error)
	ListNamespaces() (executionview.NamespaceListResult, error)
}

// scheduleOps 是 Schedule 的管理操作，*schedules.UseCase 满足它。
type scheduleOps interface {
	Create(req schedules.CreateRequest) (*schedule.Schedule, error)
	Get(namespace, scheduleID string) (*schedule.Schedule, error)
	Update(namespace, scheduleID string, req schedules.UpdateRequest) (*schedule.Schedule, error)
	Delete(namespace, scheduleID string) error
	Pause(namespace, scheduleID string) error
	Resume(namespace, scheduleID string) error
	List(filter schedules.ListFilter) (schedules.ListResult, error)
	Trigger(ctx context.Context, namespace, scheduleID string) (string, error)
}

// batchOps 是批量操作的受理、查询与取消，*batches.UseCase 满足它。
type batchOps interface {
	Start(req batches.StartRequest) (*batch.Operation, error)
	Get(namespace, operationID string) (*batch.Operation, error)
	List(filter batches.ListFilter) (batches.ListResult, error)
	Cancel(namespace, operationID string) (*batch.Operation, error)
}

// NewWorkflowRuntimeHTTPHandler 只挂 workflow 端点，各操作交给 server 及它的用例与读模型。
func NewWorkflowRuntimeHTTPHandler(server *engine.Server) http.Handler {
	router := newRuntimeRouter(server.Logger(), nil)
	bindWorkflowRuntimeAPI(router, server)
	return router
}

func bindWorkflowRuntimeAPI(router gin.IRoutes, server *engine.Server) {
	api := &workflowRuntimeHTTPAPI{
		engine:      server,
		definitions: server.Definitions(),
		runs:        server.View(),
		schedules:   server.ScheduleUseCase(),
		batches:     server.Batches(),
	}
	NewWorkflowRuntimeHTTPAPIWrap(api, api).BindAll(router)
}

type workflowRuntimeHTTPAPI struct {
	engine      workflowEngine
	definitions workflowDefinitions
	runs        runReads
	schedules   scheduleOps
	batches     batchOps
}

func (a *workflowRuntimeHTTPAPI) PreHandlers() []gin.HandlerFunc { return nil }

func (a *workflowRuntimeHTTPAPI) Register(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterRequest) (runtimeapi.WorkflowConfigRef, error) {
	if err := ctx.Request.Context().Err(); err != nil {
		return runtimeapi.WorkflowConfigRef{}, err
	}
	workflow, err := decodeWorkflowConfig(req.Workflow)
	if err != nil {
		return runtimeapi.WorkflowConfigRef{}, err
	}
	return configRefToWire(a.definitions.RegisterWorkflowInNamespace(req.Namespace, workflow))
}

func (a *workflowRuntimeHTTPAPI) RegisterArtifact(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterRequest) (runtimeapi.WorkflowConfigRef, error) {
	if err := ctx.Request.Context().Err(); err != nil {
		return runtimeapi.WorkflowConfigRef{}, err
	}
	workflow, err := decodeWorkflowConfig(req.Workflow)
	if err != nil {
		return runtimeapi.WorkflowConfigRef{}, err
	}
	if req.ActivateIfNew {
		return configRefToWire(a.definitions.EnrollWorkflowInNamespace(req.Namespace, workflow))
	}
	return configRefToWire(a.definitions.RegisterWorkflowArtifactInNamespace(req.Namespace, workflow))
}

func (a *workflowRuntimeHTTPAPI) SetWorkerTarget(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSetWorkerTargetRequest) (runtimeapi.WorkflowConfigRef, error) {
	if err := ctx.Request.Context().Err(); err != nil {
		return runtimeapi.WorkflowConfigRef{}, err
	}
	return configRefToWire(a.definitions.SetWorkflowWorkerTarget(req.Namespace, req.WorkflowID, req.TargetWorkerIdentity))
}

func (a *workflowRuntimeHTTPAPI) SetRunPolicy(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSetRunPolicyRequest) (*runtimeapi.Run, error) {
	return runWithErrToWire(a.engine.SetRunPolicy(ctx.Request.Context(), req.Namespace, req.RunID, req.Policy))
}

func (a *workflowRuntimeHTTPAPI) Start(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartRequest) (runtimeapi.WorkflowRuntimeStartResponse, error) {
	return startResponse(a.engine.StartWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowType, req.WorkflowID, req.RequestID, req.Input))
}

func (a *workflowRuntimeHTTPAPI) StartVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartVersionRequest) (runtimeapi.WorkflowRuntimeStartResponse, error) {
	return startResponse(a.engine.StartWorkflowVersion(ctx.Request.Context(), definition.WorkflowConfigRef(req.Ref), req.WorkflowID, req.Input))
}

// startResponse 把新建的 Run 转成启动应答，Workflow ID 是引擎最终采用的值（请求没给时由引擎生成）。
func startResponse(run *execution.Run, err error) (runtimeapi.WorkflowRuntimeStartResponse, error) {
	if err != nil {
		return runtimeapi.WorkflowRuntimeStartResponse{}, err
	}
	return runtimeapi.WorkflowRuntimeStartResponse{RunID: run.ID, WorkflowID: run.WorkflowID}, nil
}

func (a *workflowRuntimeHTTPAPI) Cancel(ctx *gin.Context, req runtimeapi.WorkflowRuntimeCancelRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.CancelWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.Reason))
}

func (a *workflowRuntimeHTTPAPI) Terminate(ctx *gin.Context, req runtimeapi.WorkflowRuntimeTerminateRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.TerminateWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.Reason))
}

func (a *workflowRuntimeHTTPAPI) GetRun(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) (*runtimeapi.RunWithBlocked, error) {
	return runWithBlockedToWire(a.runs.GetRunWithBlocked(req.Namespace, req.WorkflowID, req.RunID))
}

// WaitClosed 的期限到期、Server 停止时 Run 仍在运行都应答 204：客户端再发一次，可以落到其他副本上。
func (a *workflowRuntimeHTTPAPI) WaitClosed(ctx *gin.Context, req runtimeapi.WorkflowRuntimeWaitClosedRequest) (*runtimeapi.WorkflowRuntimeWaitClosedResponse, error) {
	pollCtx, cancel := runtimePollContext(ctx.Request.Context(), req.TimeoutMillis)
	defer cancel()
	run, closing, err := a.engine.WaitRunClosed(pollCtx, req.Namespace, req.WorkflowID, req.RunID)
	if err != nil {
		if pollCtx.Err() != nil && errors.Is(err, pollCtx.Err()) {
			ctx.Status(http.StatusNoContent)
			return nil, errSkipResponse
		}
		return nil, err
	}
	return &runtimeapi.WorkflowRuntimeWaitClosedResponse{Run: runToWire(run), CloseEvent: closing}, nil
}

func (a *workflowRuntimeHTTPAPI) GetConfig(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) (*program.WorkflowConfig, error) {
	return a.runs.GetWorkflowConfig(req.Namespace, req.RunID, req.ProgramVersion)
}

func (a *workflowRuntimeHTTPAPI) DispatchPending(ctx *gin.Context) (runtimeapi.WorkflowDispatchResult, error) {
	result, err := a.engine.DispatchPendingTasks(ctx.Request.Context())
	return runtimeapi.WorkflowDispatchResult(result), err
}

func (a *workflowRuntimeHTTPAPI) RegisterActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRegisterActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.RegisterActivityWorkerVersion(ctx.Request.Context(), req.Worker))
}

func (a *workflowRuntimeHTTPAPI) HeartbeatActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeHeartbeatActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.HeartbeatActivityWorkerVersionInNamespace(ctx.Request.Context(), req.Namespace, req.Identity, req.BuildID))
}

func (a *workflowRuntimeHTTPAPI) DeregisterActivityWorkerVersion(ctx *gin.Context, req runtimeapi.WorkflowRuntimeDeregisterActivityWorkerRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.DeregisterActivityWorkerVersionInNamespace(ctx.Request.Context(), req.Namespace, req.Identity, req.BuildID))
}

func (a *workflowRuntimeHTTPAPI) ListRuns(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListRunsRequest) (runtimeapi.RunListResult, error) {
	return runListResultToWire(a.runs.ListRuns(execution.RunListFilter(req.Filter)))
}

func (a *workflowRuntimeHTTPAPI) RunStats(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunStatsRequest) (runtimeapi.RunStats, error) {
	stats, err := a.runs.GetRunStats(req.Namespace)
	return runtimeapi.RunStats(stats), err
}

func (a *workflowRuntimeHTTPAPI) ListConfigs(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListConfigsRequest) (runtimeapi.WorkflowConfigListResult, error) {
	result, err := a.runs.ListWorkflowConfigs(req.Namespace)
	return runtimeapi.WorkflowConfigListResult{
		Items: mapSlice(result.Items, func(item executionview.WorkflowConfigSummary) runtimeapi.WorkflowConfigSummary {
			return runtimeapi.WorkflowConfigSummary(item)
		}),
		Total: result.Total,
	}, err
}

func (a *workflowRuntimeHTTPAPI) ListWorkers(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListWorkersRequest) (runtimeapi.WorkerListResult, error) {
	result, err := a.engine.ListWorkers(executionview.WorkerListFilter(req.Filter))
	return runtimeapi.WorkerListResult{
		Items: mapSlice(result.Items, func(item executionview.WorkerListItem) runtimeapi.WorkerListItem {
			return runtimeapi.WorkerListItem(item)
		}),
		Total: result.Total,
	}, err
}

func (a *workflowRuntimeHTTPAPI) UpsertSearchAttributes(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSearchAttributesRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.UpsertSearchAttributes(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.SearchAttributes))
}

func (a *workflowRuntimeHTTPAPI) History(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) ([]history.Event, error) {
	return a.runs.GetEvents(req.Namespace, req.WorkflowID, req.RunID)
}

func (a *workflowRuntimeHTTPAPI) HistoryPage(ctx *gin.Context, req runtimeapi.WorkflowRuntimeHistoryPageRequest) (runtimeapi.HistoryPage, error) {
	page, err := a.runs.GetEventPage(req.Namespace, req.WorkflowID, historyPageRequestFromWire(req.HistoryPageRequest))
	return runtimeapi.HistoryPage(page), err
}

func (a *workflowRuntimeHTTPAPI) Signal(ctx *gin.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) (runtimeapi.WorkflowRuntimeSignalResponse, error) {
	eventID, err := a.engine.SignalWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.Name, req.RequestID, req.Input)
	return runtimeapi.WorkflowRuntimeSignalResponse{EventID: eventID}, err
}

func (a *workflowRuntimeHTTPAPI) Query(ctx *gin.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) (runtimeapi.WorkflowQueryResult, error) {
	return queryResultToWire(a.engine.QueryWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.Name, req.Input))
}

func (a *workflowRuntimeHTTPAPI) Update(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateRequest) (protocol.WorkflowUpdateResult, error) {
	result, err := a.engine.UpdateWorkflow(ctx.Request.Context(), req.Namespace, req.WorkflowID, req.RunID, req.Name, req.UpdateID, protocol.UpdateWaitStage(req.WaitStage), req.Args)
	return result, workflowUpdateError(ctx, err)
}

func (a *workflowRuntimeHTTPAPI) UpdateWithStart(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateWithStartRequest) (runtimeapi.WorkflowRuntimeUpdateWithStartResponse, error) {
	runID, result, err := a.engine.UpdateWithStart(ctx.Request.Context(), req.Namespace, req.WorkflowType, req.WorkflowID,
		req.StartInput, req.UpdateName, req.UpdateID, req.UpdateArgs, protocol.UpdateWaitStage(req.WaitStage))
	if err != nil {
		return runtimeapi.WorkflowRuntimeUpdateWithStartResponse{}, workflowUpdateError(ctx, err)
	}
	workflowID, err := a.startedWorkflowID(ctx, req.Namespace, req.WorkflowID, runID)
	return runtimeapi.WorkflowRuntimeUpdateWithStartResponse{RunID: runID, WorkflowID: workflowID, Result: result}, err
}

// startedWorkflowID 返回 with-start 调用实际作用的 Workflow ID：请求没给时由引擎生成，从新建的 Run 读回。
func (a *workflowRuntimeHTTPAPI) startedWorkflowID(ctx *gin.Context, namespace, workflowID, runID string) (string, error) {
	if workflowID != "" {
		return workflowID, nil
	}
	run, _, err := a.runs.GetRunWithState(namespace, "", runID)
	if err != nil {
		return "", err
	}
	return run.WorkflowID, nil
}

// workflowUpdateError 只用于 update 端点：validator 拒绝的 wire 就是 protocol.Failure 本身
// （validator 的错误按 apperror.Encode 编出，typed 的 Type/NonRetryable/Details/Cause 原样保留）。
// 拒绝仍是 400 同步失败：零 history、不 accept、不注册 waiter、不产生 rejected 事件。
// 其它错误原样返回，交给统一的错误映射。
func workflowUpdateError(ctx *gin.Context, err error) error {
	cause, rejected := engine.UpdateRejectionCause(err)
	if !rejected {
		return err
	}
	failure, encodeErr := apperror.Encode(cause, apperror.JSONEncoder)
	if encodeErr == nil {
		if validateErr := failure.Validate(); validateErr != nil {
			// 只有 details/cause 过不了 wire：保留标量（Type/NonRetryable）与文案，
			// 拒绝身份与 typed 语义仍可达。
			failure.Details = nil
			failure.Cause = nil
			encodeErr = failure.Validate()
		}
	}
	if encodeErr != nil {
		// 连标量都无法编码（罕见）：退化成 message-only Failure，仍然是 400 + 可判定 JSON。
		failure = protocol.Failure{Message: cause.Error()}
	}
	ctx.Header("Content-Type", "application/json")
	ctx.Status(http.StatusBadRequest)
	_ = json.NewEncoder(ctx.Writer).Encode(failure)
	return errSkipResponse
}

func (a *workflowRuntimeHTTPAPI) Capabilities(*gin.Context) (runtimeapi.RuntimeCapabilities, error) {
	return runtimeapi.RuntimeCapabilities{
		WorkflowCapabilities: program.CurrentWLangCapabilities(),
	}, nil
}

func (a *workflowRuntimeHTTPAPI) SignalWithStart(ctx *gin.Context, req runtimeapi.WorkflowRuntimeSignalWithStartRequest) (runtimeapi.WorkflowRuntimeSignalWithStartResponse, error) {
	runID, eventID, err := a.engine.SignalWithStart(ctx.Request.Context(), req.Namespace, req.WorkflowType, req.WorkflowID,
		req.StartInput, req.SignalName, req.RequestID, req.SignalInput)
	if err != nil {
		return runtimeapi.WorkflowRuntimeSignalWithStartResponse{}, err
	}
	workflowID, err := a.startedWorkflowID(ctx, req.Namespace, req.WorkflowID, runID)
	return runtimeapi.WorkflowRuntimeSignalWithStartResponse{RunID: runID, WorkflowID: workflowID, EventID: eventID}, err
}

func (a *workflowRuntimeHTTPAPI) CreateSchedule(ctx *gin.Context, req runtimeapi.ScheduleCreateRequest) (*runtimeapi.Schedule, error) {
	return scheduleWithErrToWire(a.schedules.Create(schedules.CreateRequest(req)))
}

func (a *workflowRuntimeHTTPAPI) GetSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (*runtimeapi.Schedule, error) {
	return scheduleWithErrToWire(a.schedules.Get(req.Namespace, req.ScheduleID))
}

func (a *workflowRuntimeHTTPAPI) UpdateSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeUpdateScheduleRequest) (*runtimeapi.Schedule, error) {
	return scheduleWithErrToWire(a.schedules.Update(req.Namespace, req.ScheduleID, schedules.UpdateRequest(req.Update)))
}

func (a *workflowRuntimeHTTPAPI) DeleteSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.schedules.Delete(req.Namespace, req.ScheduleID))
}

func (a *workflowRuntimeHTTPAPI) PauseSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.schedules.Pause(req.Namespace, req.ScheduleID))
}

func (a *workflowRuntimeHTTPAPI) ResumeSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.schedules.Resume(req.Namespace, req.ScheduleID))
}

func (a *workflowRuntimeHTTPAPI) ListSchedules(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListSchedulesRequest) (runtimeapi.ScheduleListResult, error) {
	return scheduleListResultToWire(a.schedules.List(schedules.ListFilter(req.Filter)))
}

func (a *workflowRuntimeHTTPAPI) TriggerSchedule(ctx *gin.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.WorkflowRuntimeTriggerScheduleResponse, error) {
	runID, err := a.schedules.Trigger(ctx.Request.Context(), req.Namespace, req.ScheduleID)
	return runtimeapi.WorkflowRuntimeTriggerScheduleResponse{RunID: runID}, err
}

// StartBatch 只保存 pending 记录，应答 202 Accepted：处理在后台进行，经 get-batch 查询进度。
func (a *workflowRuntimeHTTPAPI) StartBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeStartBatchRequest) (*runtimeapi.BatchOperation, error) {
	op, err := a.batches.Start(batchStartRequestFromWire(req.Req))
	if err == nil {
		ctx.Status(http.StatusAccepted)
	}
	return batchOperationWithErrToWire(op, err)
}

func (a *workflowRuntimeHTTPAPI) GetBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (*runtimeapi.BatchOperation, error) {
	return batchOperationWithErrToWire(a.batches.Get(req.Namespace, req.OperationID))
}

func (a *workflowRuntimeHTTPAPI) ListBatches(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListBatchesRequest) (runtimeapi.BatchListResult, error) {
	return batchListResultToWire(a.batches.List(batches.ListFilter(req)))
}

func (a *workflowRuntimeHTTPAPI) CancelBatch(ctx *gin.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (*runtimeapi.BatchOperation, error) {
	return batchOperationWithErrToWire(a.batches.Cancel(req.Namespace, req.OperationID))
}

func (a *workflowRuntimeHTTPAPI) Reset(ctx *gin.Context, req runtimeapi.WorkflowRuntimeResetRequest) (runtimeapi.WorkflowRuntimeResetResponse, error) {
	reset := execution.ResetWorkflowRequest{
		Namespace:           req.Namespace,
		RunID:               req.RunID,
		Reason:              req.Reason,
		ResetType:           execution.ResetType(req.ResetType),
		ResetEventID:        req.ResetEventID,
		ReapplyExcludeTypes: req.ReapplyExcludeTypes,
	}
	if err := reset.Validate(); err != nil {
		return runtimeapi.WorkflowRuntimeResetResponse{}, err
	}
	runID, err := a.engine.ResetWorkflow(ctx.Request.Context(), reset)
	return runtimeapi.WorkflowRuntimeResetResponse{RunID: runID}, err
}

func (a *workflowRuntimeHTTPAPI) ResetActivity(ctx *gin.Context, req runtimeapi.WorkflowRuntimeResetActivityRequest) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeOK(a.engine.ResetActivity(req.Namespace, req.RunID, req.ActivityID, execution.ResetActivityOptions{
		ResetHeartbeat: req.ResetHeartbeat,
		Reason:         req.Reason,
	}))
}

func (a *workflowRuntimeHTTPAPI) ListTaskQueues(ctx *gin.Context, req runtimeapi.WorkflowRuntimeListTaskQueuesRequest) (runtimeapi.TaskQueueListResult, error) {
	result, err := a.runs.ListTaskQueues(req.Namespace)
	return runtimeapi.TaskQueueListResult{
		Items: mapSlice(result.Items, func(item dispatch.TaskQueueInfo) runtimeapi.TaskQueueInfo {
			return runtimeapi.TaskQueueInfo(item)
		}),
		Total: result.Total,
	}, err
}

func (a *workflowRuntimeHTTPAPI) ListNamespaces(ctx *gin.Context) (runtimeapi.NamespaceListResult, error) {
	result, err := a.runs.ListNamespaces()
	return runtimeapi.NamespaceListResult{
		Items: mapSlice(result.Items, func(item executionview.NamespaceInfo) runtimeapi.NamespaceInfo {
			return runtimeapi.NamespaceInfo(item)
		}),
		Total: result.Total,
	}, err
}

func (a *workflowRuntimeHTTPAPI) ActivityStates(ctx *gin.Context, req runtimeapi.WorkflowRuntimeRunRequest) ([]*runtimeapi.ActivityState, error) {
	states, err := a.runs.GetActivityStates(req.Namespace, req.RunID)
	return mapSlice(states, activityStateToWire), err
}
