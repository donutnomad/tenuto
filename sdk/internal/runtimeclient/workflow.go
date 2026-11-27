package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"go.opentelemetry.io/otel/attribute"
)

// WorkflowRuntimeHTTPClient 调用面向外部调用方的 workflow 端点：启动、交互、读取、Schedule、批量操作，
// 以及 Worker 登记 Activity 路由与 Workflow 定义。
//
// 调用的重试分两类（见 retryMode）。按幂等调用重试：读取与长轮询、Cancel（已请求过取消时返回成功）、Terminate
// 与 Schedule 的 Delete（重发可能报告 Run 已结束、Schedule 不存在）、带 request ID 的 Start 与 Signal、带 Update ID
// 的 Update、带 request ID 的 Reset、搜索属性与 Run 策略的写入、Schedule 的 Update / Pause / Resume、批量操作的取消、
// Worker 的登记与心跳。只在请求确定没有被处理时重试（runtimeapi.IsIdempotent 为 false 的端点）：不带 request ID 的
// Reset、Schedule 的 Create 与 Trigger、批量操作的启动，它们重发会再执行一次。
type WorkflowRuntimeHTTPClient struct {
	baseURL   string
	client    *http.Client
	telemetry *clientTelemetry
	retry     RetryPolicy
}

func NewWorkflowRuntimeHTTPClient(baseURL string, client *http.Client, options RuntimeHTTPClientOptions) *WorkflowRuntimeHTTPClient {
	return &WorkflowRuntimeHTTPClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		client:    runtimeHTTPClient(client, options.Telemetry),
		telemetry: newClientTelemetry(options.Telemetry),
		retry:     options.Retry.normalized(),
	}
}

// StartWorkflow 按 Workflow Type 启动 Run。workflowID 留空时服务端生成；应答带最终采用的 Workflow ID。
func (c *WorkflowRuntimeHTTPClient) StartWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeStartRequest) (runtimeapi.WorkflowRuntimeStartResponse, error) {
	var resp runtimeapi.WorkflowRuntimeStartResponse
	err := c.post(ctx, runtimeapi.WorkflowRuntimeStartPath, req, &resp)
	return resp, err
}

func (c *WorkflowRuntimeHTTPClient) CancelWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeCancelRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeCancelPath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) TerminateWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeTerminateRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeTerminatePath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) GetRun(ctx context.Context, req runtimeapi.WorkflowRuntimeRunRequest) (runtimeapi.RunWithBlocked, error) {
	var resp runtimeapi.RunWithBlocked
	err := c.post(ctx, runtimeapi.WorkflowRuntimeGetRunPath, req, &resp)
	if err == nil && resp.Run == nil {
		err = fmt.Errorf("workflow runtime api: get-run returned no run")
	}
	return resp, err
}

// WaitRunClosed 长轮询等待 Run 结束。Server 的等待到期时 Run 仍在运行则返回 protocol.ErrTaskPollTimeout，
// 调用方再发一次。ctx 取消时中止请求。
func (c *WorkflowRuntimeHTTPClient) WaitRunClosed(ctx context.Context, req runtimeapi.WorkflowRuntimeWaitClosedRequest) (runtimeapi.WorkflowRuntimeWaitClosedResponse, error) {
	var resp runtimeapi.WorkflowRuntimeWaitClosedResponse
	err := c.post(ctx, runtimeapi.WorkflowRuntimeWaitClosedPath, req, &resp)
	if err == nil && resp.Run == nil {
		err = fmt.Errorf("workflow runtime api: wait-closed returned no run")
	}
	return resp, err
}

func (c *WorkflowRuntimeHTTPClient) GetEventPage(ctx context.Context, req runtimeapi.WorkflowRuntimeHistoryPageRequest) (runtimeapi.HistoryPage, error) {
	var page runtimeapi.HistoryPage
	err := c.post(ctx, runtimeapi.WorkflowRuntimeHistoryPagePath, req, &page)
	return page, err
}

func (c *WorkflowRuntimeHTTPClient) ListRuns(ctx context.Context, filter runtimeapi.RunListFilter) (runtimeapi.RunListResult, error) {
	var result runtimeapi.RunListResult
	err := c.post(ctx, runtimeapi.WorkflowRuntimeListRunsPath, runtimeapi.WorkflowRuntimeListRunsRequest{Filter: filter}, &result)
	return result, err
}

func (c *WorkflowRuntimeHTTPClient) UpsertSearchAttributes(ctx context.Context, req runtimeapi.WorkflowRuntimeSearchAttributesRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeSearchAttributesPath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) SignalWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeSignalPath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) SignalWithStart(ctx context.Context, req runtimeapi.WorkflowRuntimeSignalWithStartRequest) (runtimeapi.WorkflowRuntimeSignalWithStartResponse, error) {
	var resp runtimeapi.WorkflowRuntimeSignalWithStartResponse
	err := c.post(ctx, runtimeapi.WorkflowRuntimeSignalWithStartPath, req, &resp)
	return resp, err
}

func (c *WorkflowRuntimeHTTPClient) QueryWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeInteractionRequest) (runtimeapi.WorkflowQueryResult, error) {
	var result runtimeapi.WorkflowQueryResult
	err := c.post(ctx, runtimeapi.WorkflowRuntimeQueryPath, req, &result)
	return result, err
}

func (c *WorkflowRuntimeHTTPClient) UpdateWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeUpdateRequest) (result protocol.WorkflowUpdateResult, err error) {
	ctx, span := c.telemetry.start(ctx, "UpdateWorkflow", updateSpanAttributes(req.Namespace, req.WorkflowID, req.RunID, req.Name, req.UpdateID)...)
	defer func() { finishSpan(span, err) }()
	if req.Args == nil {
		req.Args = protocol.Payloads{}
	}
	err = c.post(ctx, runtimeapi.WorkflowRuntimeUpdatePath, req, &result)
	return result, err
}

func (c *WorkflowRuntimeHTTPClient) UpdateWithStart(ctx context.Context, req runtimeapi.WorkflowRuntimeUpdateWithStartRequest) (resp runtimeapi.WorkflowRuntimeUpdateWithStartResponse, err error) {
	ctx, span := c.telemetry.start(ctx, "UpdateWithStartWorkflow", updateSpanAttributes(req.Namespace, req.WorkflowID, "", req.UpdateName, req.UpdateID)...)
	defer func() { finishSpan(span, err) }()
	if req.UpdateArgs == nil {
		req.UpdateArgs = protocol.Payloads{}
	}
	err = c.post(ctx, runtimeapi.WorkflowRuntimeUpdateWithStartPath, req, &resp)
	return resp, err
}

func updateSpanAttributes(namespace, workflowID, runID, updateName, updateID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(attrNamespace, protocol.NormalizeNamespace(namespace)),
		attribute.String(attrWorkflowID, workflowID),
		attribute.String(attrWorkflowRunID, runID),
		attribute.String("tenuto.update.name", updateName),
		attribute.String("tenuto.update.id", updateID),
	}
}

// ResetWorkflow 不带 request_id 时每次请求分叉一个新 Run，只在请求确定没有被处理时重试；带 request_id 时 Server
// 让重复的请求返回第一次 reset 出的 Run，按幂等调用重试。
func (c *WorkflowRuntimeHTTPClient) ResetWorkflow(ctx context.Context, req runtimeapi.WorkflowRuntimeResetRequest) (string, error) {
	var resp runtimeapi.WorkflowRuntimeResetResponse
	mode := retryUnsent
	if req.RequestID != "" {
		mode = retryIdempotent
	}
	err := runtimePostRetrying(ctx, c.retry, mode, c.client, c.baseURL, runtimeapi.WorkflowRuntimeResetPath, "workflow runtime api", nil, req, &resp)
	return resp.RunID, err
}

func (c *WorkflowRuntimeHTTPClient) SetRunPolicy(ctx context.Context, req runtimeapi.WorkflowRuntimeSetRunPolicyRequest) (runtimeapi.Run, error) {
	var run runtimeapi.Run
	err := c.post(ctx, runtimeapi.WorkflowRuntimeSetPolicyPath, req, &run)
	return run, err
}

func (c *WorkflowRuntimeHTTPClient) CreateSchedule(ctx context.Context, req runtimeapi.ScheduleCreateRequest) (runtimeapi.Schedule, error) {
	var schedule runtimeapi.Schedule
	err := c.post(ctx, runtimeapi.WorkflowRuntimeCreateSchedulePath, req, &schedule)
	return schedule, err
}

func (c *WorkflowRuntimeHTTPClient) GetSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.Schedule, error) {
	var schedule runtimeapi.Schedule
	err := c.post(ctx, runtimeapi.WorkflowRuntimeGetSchedulePath, req, &schedule)
	return schedule, err
}

func (c *WorkflowRuntimeHTTPClient) UpdateSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeUpdateScheduleRequest) (runtimeapi.Schedule, error) {
	var schedule runtimeapi.Schedule
	err := c.post(ctx, runtimeapi.WorkflowRuntimeUpdateSchedulePath, req, &schedule)
	return schedule, err
}

func (c *WorkflowRuntimeHTTPClient) DeleteSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeDeleteSchedulePath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) PauseSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimePauseSchedulePath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) ResumeSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeResumeSchedulePath, req, nil)
}

func (c *WorkflowRuntimeHTTPClient) ListSchedules(ctx context.Context, filter runtimeapi.ScheduleListFilter) (runtimeapi.ScheduleListResult, error) {
	var result runtimeapi.ScheduleListResult
	err := c.post(ctx, runtimeapi.WorkflowRuntimeListSchedulesPath, runtimeapi.WorkflowRuntimeListSchedulesRequest{Filter: filter}, &result)
	return result, err
}

func (c *WorkflowRuntimeHTTPClient) TriggerSchedule(ctx context.Context, req runtimeapi.WorkflowRuntimeScheduleRequest) (runtimeapi.WorkflowRuntimeTriggerScheduleResponse, error) {
	var resp runtimeapi.WorkflowRuntimeTriggerScheduleResponse
	err := c.post(ctx, runtimeapi.WorkflowRuntimeTriggerSchedulePath, req, &resp)
	return resp, err
}

// StartBatchOperation 受理批量操作并返回 pending 记录（HTTP 202）；处理在服务端后台进行。
func (c *WorkflowRuntimeHTTPClient) StartBatchOperation(ctx context.Context, req runtimeapi.BatchStartRequest) (runtimeapi.BatchOperation, error) {
	var op runtimeapi.BatchOperation
	err := c.post(ctx, runtimeapi.WorkflowRuntimeStartBatchPath, runtimeapi.WorkflowRuntimeStartBatchRequest{Req: req}, &op)
	return op, err
}

func (c *WorkflowRuntimeHTTPClient) GetBatchOperation(ctx context.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (runtimeapi.BatchOperation, error) {
	var op runtimeapi.BatchOperation
	err := c.post(ctx, runtimeapi.WorkflowRuntimeGetBatchPath, req, &op)
	return op, err
}

func (c *WorkflowRuntimeHTTPClient) ListBatchOperations(ctx context.Context, req runtimeapi.WorkflowRuntimeListBatchesRequest) (runtimeapi.BatchListResult, error) {
	var result runtimeapi.BatchListResult
	err := c.post(ctx, runtimeapi.WorkflowRuntimeListBatchesPath, req, &result)
	return result, err
}

// CancelBatchOperation 取消 pending 或 running 的批量操作并返回最新记录；已处理的 Run 不回滚。
func (c *WorkflowRuntimeHTTPClient) CancelBatchOperation(ctx context.Context, req runtimeapi.WorkflowRuntimeBatchRequest) (runtimeapi.BatchOperation, error) {
	var op runtimeapi.BatchOperation
	err := c.post(ctx, runtimeapi.WorkflowRuntimeCancelBatchPath, req, &op)
	return op, err
}

func (c *WorkflowRuntimeHTTPClient) RegisterActivityWorkerVersion(ctx context.Context, worker protocol.ActivityWorkerVersion) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeRegisterActivityWorkerPath, runtimeapi.WorkflowRuntimeRegisterActivityWorkerRequest{Worker: worker}, nil)
}

// HeartbeatActivityWorkerVersionInNamespace 续期 Worker 的登记。Server 上没有这份登记（404）时返回包装了
// protocol.ErrActivityWorkerVersionNotRegistered 的错误，Worker 据此重新登记。
func (c *WorkflowRuntimeHTTPClient) HeartbeatActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	err := c.post(ctx, runtimeapi.WorkflowRuntimeHeartbeatActivityWorkerPath, runtimeapi.WorkflowRuntimeHeartbeatActivityWorkerRequest{Namespace: namespace, Identity: identity, BuildID: buildID}, nil)
	if status, ok := errors.AsType[*StatusError](err); ok && status.Code == http.StatusNotFound {
		return fmt.Errorf("%w: %w", protocol.ErrActivityWorkerVersionNotRegistered, err)
	}
	return err
}

// DeregisterActivityWorkerVersionInNamespace 注销 Worker 的 Activity 路由，Worker 正常停止时调用。
func (c *WorkflowRuntimeHTTPClient) DeregisterActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	return c.post(ctx, runtimeapi.WorkflowRuntimeDeregisterActivityWorkerPath, runtimeapi.WorkflowRuntimeDeregisterActivityWorkerRequest{Namespace: namespace, Identity: identity, BuildID: buildID}, nil)
}

// RegisterWorkflowJSON 让 Activity-role Worker 经路由 registry 登记随二进制发布的
// workflow.json，语义与 WorkflowTaskRuntimeHTTPClient.RegisterWorkflowJSON 相同。
func (c *WorkflowRuntimeHTTPClient) RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (string, error) {
	return registerWorkflowJSON(ctx, namespace, workflowJSON, c.post)
}

func registerWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte, post func(ctx context.Context, path string, req any, resp any) error) (string, error) {
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(workflowJSON, &workflow); err != nil {
		return "", fmt.Errorf("decode workflow artifact: %w", err)
	}
	data, err := json.Marshal(workflow)
	if err != nil {
		return "", fmt.Errorf("encode workflow artifact: %w", err)
	}
	request := runtimeapi.WorkflowRuntimeRegisterRequest{Namespace: namespace, Workflow: data, ActivateIfNew: true}
	var ref runtimeapi.WorkflowConfigRef
	if err := post(ctx, runtimeapi.WorkflowRuntimeArtifactsPath, request, &ref); err != nil {
		return "", err
	}
	return ref.Version, nil
}

// post 发送调用，瞬时错误按 retry 重试。重发会再执行一次的端点（runtimeapi.IsIdempotent 为 false）只在请求确定
// 没有被处理时重试。
func (c *WorkflowRuntimeHTTPClient) post(ctx context.Context, path string, req any, resp any) error {
	mode := retryIdempotent
	if !runtimeapi.IsIdempotent(path) {
		mode = retryUnsent
	}
	return runtimePostRetrying(ctx, c.retry, mode, c.client, c.baseURL, path, "workflow runtime api", nil, req, resp)
}
