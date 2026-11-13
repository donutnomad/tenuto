package runtimeclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

type WorkflowTaskRuntimeHTTPClient struct {
	baseURL     string
	client      *http.Client
	pollTimeout time.Duration
	retry       RetryPolicy
}

func NewWorkflowTaskRuntimeHTTPClient(baseURL string, client *http.Client, options RuntimeHTTPClientOptions) *WorkflowTaskRuntimeHTTPClient {
	options = normalizeRuntimeHTTPClientOptions(options)
	return &WorkflowTaskRuntimeHTTPClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		client:      runtimeHTTPClient(client, options.Telemetry),
		pollTimeout: options.PollTimeout,
		retry:       options.Retry,
	}
}

func (c *WorkflowTaskRuntimeHTTPClient) PollWorkflowTask(ctx context.Context) (protocol.TaskLease, error) {
	var lease protocol.TaskLease
	err := runtimePoll(ctx, c.retry, c.client, c.baseURL, runtimeapi.WorkflowTaskRuntimePollPath, "workflow task runtime api", workerInfoHeaders(ctx),
		runtimeapi.WorkflowTaskRuntimePollRequest{TimeoutMillis: c.pollTimeout.Milliseconds()}, &lease)
	return lease, err
}

func (c *WorkflowTaskRuntimeHTTPClient) AckWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return c.post(ctx, runtimeapi.WorkflowTaskRuntimeAckPath, runtimeapi.WorkflowTaskRuntimeLeaseRequest{Lease: lease}, nil)
}

func (c *WorkflowTaskRuntimeHTTPClient) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	return c.post(ctx, runtimeapi.WorkflowTaskRuntimeNackPath, runtimeapi.WorkflowTaskRuntimeLeaseRequest{Lease: lease}, nil)
}

// GetWorkflowConfig、GetEventsContext 与 GetRunStatusContext 读取 ctx 里 Workflow Task 所属的 Run：服务端按
// ctx 携带的 task token 定位 Run，runID 是这个 Run 的 ID。ctx 没有 task token 时返回错误。
func (c *WorkflowTaskRuntimeHTTPClient) GetWorkflowConfig(ctx context.Context, runID, programVersion string) (*program.WorkflowConfig, error) {
	request, err := workflowTaskRunRequest(ctx, programVersion)
	if err != nil {
		return nil, err
	}
	var wf program.WorkflowConfig
	if err := c.post(ctx, runtimeapi.WorkflowTaskRuntimeConfigPath, request, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (c *WorkflowTaskRuntimeHTTPClient) GetEvents(runID string) ([]history.Event, error) {
	return c.GetEventsContext(context.Background(), runID)
}

func (c *WorkflowTaskRuntimeHTTPClient) GetEventsContext(ctx context.Context, runID string) ([]history.Event, error) {
	request, err := workflowTaskRunRequest(ctx, "")
	if err != nil {
		return nil, err
	}
	var events []history.Event
	err = c.post(ctx, runtimeapi.WorkflowTaskRuntimeEventsPath, request, &events)
	return events, err
}

func (c *WorkflowTaskRuntimeHTTPClient) GetRunStatus(runID string) (history.Status, error) {
	return c.GetRunStatusContext(context.Background(), runID)
}

func (c *WorkflowTaskRuntimeHTTPClient) GetRunStatusContext(ctx context.Context, runID string) (history.Status, error) {
	request, err := workflowTaskRunRequest(ctx, "")
	if err != nil {
		return "", err
	}
	var resp runtimeapi.WorkflowTaskRuntimeStatusResponse
	err = c.post(ctx, runtimeapi.WorkflowTaskRuntimeStatusPath, request, &resp)
	return resp.Status, err
}

func workflowTaskRunRequest(ctx context.Context, programVersion string) (runtimeapi.WorkflowTaskRuntimeRunRequest, error) {
	token := protocol.WorkflowTaskTokenFrom(ctx)
	if token == "" {
		return runtimeapi.WorkflowTaskRuntimeRunRequest{}, fmt.Errorf("workflow task runtime token is empty")
	}
	return runtimeapi.WorkflowTaskRuntimeRunRequest{TaskToken: token, ProgramVersion: programVersion}, nil
}

func (c *WorkflowTaskRuntimeHTTPClient) HandleWorkflowTaskCompleted(ctx context.Context, runID string, scheduledEventID int64, commands []history.Command, workerIdentity string) error {
	token := protocol.WorkflowTaskTokenFrom(ctx)
	if token == "" {
		return fmt.Errorf("workflow task runtime token is empty")
	}
	return c.post(ctx, runtimeapi.WorkflowTaskRuntimeCompletePath, runtimeapi.WorkflowTaskRuntimeCompleteRequest{
		TaskToken:      token,
		Commands:       commands,
		WorkerIdentity: workerIdentity,
	}, nil)
}

func (c *WorkflowTaskRuntimeHTTPClient) HandleWorkflowTaskTimedOut(ctx context.Context, runID string, scheduledEventID int64, errMsg string) error {
	token := protocol.WorkflowTaskTokenFrom(ctx)
	if token == "" {
		return fmt.Errorf("workflow task runtime token is empty")
	}
	return c.post(ctx, runtimeapi.WorkflowTaskRuntimeTimedOutPath, runtimeapi.WorkflowTaskRuntimeTimedOutRequest{
		TaskToken: token,
		Error:     errMsg,
	}, nil)
}

func (c *WorkflowTaskRuntimeHTTPClient) HandleWorkflowTaskFailed(ctx context.Context, runID string, scheduledEventID int64, cause, reason string) error {
	token := protocol.WorkflowTaskTokenFrom(ctx)
	if token == "" {
		return fmt.Errorf("workflow task runtime token is empty")
	}
	return c.post(ctx, runtimeapi.WorkflowTaskRuntimeFailedPath, runtimeapi.WorkflowTaskRuntimeFailedRequest{
		TaskToken: token,
		Cause:     cause,
		Reason:    reason,
	}, nil)
}

func (c *WorkflowTaskRuntimeHTTPClient) GetWorkflowActivation(ctx context.Context, request history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	token, err := RuntimeTaskToken(request.Task)
	if err != nil {
		return history.WorkflowActivation{}, err
	}
	var activation history.WorkflowActivation
	err = c.post(ctx, runtimeapi.WorkflowTaskRuntimeActivationPath, runtimeapi.WorkflowTaskRuntimeActivationRequest{
		TaskToken:           token,
		AfterEventID:        request.AfterEventID,
		ExecutionGeneration: request.ExecutionGeneration,
		ActivationID:        request.ActivationID,
	}, &activation)
	return activation, err
}

func (c *WorkflowTaskRuntimeHTTPClient) CompleteWorkflowActivation(ctx context.Context, request history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	token, err := RuntimeTaskToken(request.Task)
	if err != nil {
		return history.WorkflowTaskCompletionAck{}, err
	}
	var ack history.WorkflowTaskCompletionAck
	err = c.post(ctx, runtimeapi.WorkflowTaskRuntimeCompleteActivationPath, runtimeapi.WorkflowTaskRuntimeCompleteActivationRequest{
		TaskToken:  token,
		Activation: request.Activation,
		Commands:   request.Commands,
	}, &ack)
	return ack, err
}

// RegisterWorkflowJSON 按「新内容首次出现时激活」登记随二进制发布的 workflow.json。
//
// 这是 Worker 启动期自动登记在远程 transport 上的实现：与进程内的
// 服务端 RegisterWorkflowJSON 走同一语义。
func (c *WorkflowTaskRuntimeHTTPClient) RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (string, error) {
	return registerWorkflowJSON(ctx, namespace, workflowJSON, c.post)
}

// post 发送 Workflow Task 调用：都由 task token 定界（已关闭的 Workflow Task 再次完成或失败是空操作），按幂等调用重试。
func (c *WorkflowTaskRuntimeHTTPClient) post(ctx context.Context, path string, req any, resp any) error {
	return runtimePostRetrying(ctx, c.retry, retryIdempotent, c.client, c.baseURL, path, "workflow task runtime api", workerInfoHeaders(ctx), req, resp)
}
