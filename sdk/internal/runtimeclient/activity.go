package runtimeclient

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

type ActivityRuntimeHTTPClient struct {
	baseURL     string
	client      *http.Client
	pollTimeout time.Duration
	retry       RetryPolicy
}

func NewActivityRuntimeHTTPClient(baseURL string, client *http.Client, options RuntimeHTTPClientOptions) *ActivityRuntimeHTTPClient {
	options = normalizeRuntimeHTTPClientOptions(options)
	return &ActivityRuntimeHTTPClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		client:      runtimeHTTPClient(client, options.Telemetry),
		pollTimeout: options.PollTimeout,
		retry:       options.Retry,
	}
}

func (c *ActivityRuntimeHTTPClient) BeginActivityTask(ctx context.Context, task protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	var result protocol.ActivityTaskBeginResult
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return result, err
	}
	err = c.post(ctx, runtimeapi.ActivityRuntimeBeginPath, runtimeapi.ActivityRuntimeBeginRequest{ActivityRuntimeTaskRequest: activityTaskRequest(token, task), RequestID: task.RequestID}, &result)
	return result, err
}

func (c *ActivityRuntimeHTTPClient) PollActivityTask(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	var lease protocol.TaskLease
	err := runtimePoll(ctx, c.retry, c.client, c.baseURL, runtimeapi.ActivityRuntimePollPath, "activity runtime api", workerInfoHeaders(ctx),
		runtimeapi.ActivityRuntimePollRequest{TaskQueue: taskQueue, TimeoutMillis: c.pollTimeout.Milliseconds()}, &lease)
	return lease, err
}

func (c *ActivityRuntimeHTTPClient) AckActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return c.post(ctx, runtimeapi.ActivityRuntimeAckPath, runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease}, nil)
}

func (c *ActivityRuntimeHTTPClient) NackActivityTask(ctx context.Context, lease protocol.TaskLease) error {
	return c.post(ctx, runtimeapi.ActivityRuntimeNackPath, runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease}, nil)
}

func (c *ActivityRuntimeHTTPClient) RecordActivityHeartbeat(ctx context.Context, task protocol.Task, details map[string]any) (protocol.ActivityHeartbeatResult, error) {
	var result protocol.ActivityHeartbeatResult
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return result, err
	}
	err = c.post(ctx, runtimeapi.ActivityRuntimeHeartbeatPath, runtimeapi.ActivityRuntimeHeartbeatRequest{ActivityRuntimeTaskRequest: activityTaskRequest(token, task), Details: details}, &result)
	return result, err
}

func (c *ActivityRuntimeHTTPClient) CheckActivityTaskCancellation(ctx context.Context, task protocol.Task) (protocol.ActivityCancellationResult, error) {
	var result protocol.ActivityCancellationResult
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return result, err
	}
	err = c.post(ctx, runtimeapi.ActivityRuntimeCancelPath, activityTaskRequest(token, task), &result)
	return result, err
}

// CompleteActivityTask 上报一次异步完成：failure 为 nil 是成功，非 nil 是失败（空 Message 仍失败）。
func (c *ActivityRuntimeHTTPClient) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return err
	}
	return c.post(ctx, runtimeapi.ActivityRuntimeCompletePath, runtimeapi.ActivityRuntimeCompleteRequest{ActivityRuntimeTaskRequest: activityTaskRequest(token, task), Output: output, Failure: failure}, nil)
}

func (c *ActivityRuntimeHTTPClient) FailActivityTask(ctx context.Context, task protocol.Task, failure protocol.Failure) error {
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return err
	}
	return c.post(ctx, runtimeapi.ActivityRuntimeFailPath, runtimeapi.ActivityRuntimeFailRequest{ActivityRuntimeTaskRequest: activityTaskRequest(token, task), Failure: &failure}, nil)
}

// ReleaseActivityTask 交还因 Worker 停止而没有结果的 attempt，Server 以同一个 attempt 重新派发。
func (c *ActivityRuntimeHTTPClient) ReleaseActivityTask(ctx context.Context, task protocol.Task) error {
	token, err := RuntimeTaskToken(task)
	if err != nil {
		return err
	}
	return c.post(ctx, runtimeapi.ActivityRuntimeReleasePath, activityTaskRequest(token, task), nil)
}

// post 发送 Activity 调用：都由 task token 定界（同一 attempt 的重复开始、心跳、完成由 Server 去重），按幂等调用重试。
func (c *ActivityRuntimeHTTPClient) post(ctx context.Context, path string, req any, resp any) error {
	return runtimePostRetrying(ctx, c.retry, retryIdempotent, c.client, c.baseURL, path, "activity runtime api", workerInfoHeaders(ctx), req, resp)
}

// activityTaskRequest 是 Activity 调用的寻址部分：token 与 Worker 身份，不附带任务载荷。
func activityTaskRequest(token protocol.TaskToken, task protocol.Task) runtimeapi.ActivityRuntimeTaskRequest {
	return runtimeapi.ActivityRuntimeTaskRequest{TaskToken: token, WorkerIdentity: task.WorkerIdentity, WorkerBuildID: task.WorkerBuildID}
}
