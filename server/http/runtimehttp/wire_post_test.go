package runtimehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

// wireStatusError 是非 2xx 应答：状态码与响应体原文。
type wireStatusError struct {
	code int
	body string
}

func (e *wireStatusError) Error() string {
	return fmt.Sprintf("%d %s: %s", e.code, http.StatusText(e.code), e.body)
}

// wirePost 按 runtime HTTP 的线上契约发送一个请求：POST JSON，ctx 里的 Worker 信息写进请求头。
// 2xx 时把应答解码进 resp（数字保留精确值），204 返回 protocol.ErrTaskPollTimeout，其余返回 *wireStatusError。
// handler 测试经它直接驱动端点，不依赖 SDK 的传输层。
func wirePost(ctx context.Context, baseURL, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if info, ok := protocol.WorkerInfoFrom(ctx); ok {
		protocol.ApplyWorkerInfoHeaders(httpReq, info)
	}
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode == http.StatusNoContent {
		return protocol.ErrTaskPollTimeout
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		data, _ := io.ReadAll(httpResp.Body)
		return &wireStatusError{code: httpResp.StatusCode, body: strings.TrimSpace(string(data))}
	}
	if resp == nil {
		return nil
	}
	return jsonvalue.Decode(httpResp.Body, resp)
}

func pollActivityTask(ctx context.Context, baseURL, taskQueue string, timeout time.Duration) (protocol.TaskLease, error) {
	var lease protocol.TaskLease
	err := wirePost(ctx, baseURL, runtimeapi.ActivityRuntimePollPath, runtimeapi.ActivityRuntimePollRequest{TaskQueue: taskQueue, TimeoutMillis: timeout.Milliseconds()}, &lease)
	return lease, err
}

func ackActivityTask(ctx context.Context, baseURL string, lease protocol.TaskLease) error {
	return wirePost(ctx, baseURL, runtimeapi.ActivityRuntimeAckPath, runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease}, nil)
}

func nackActivityTask(ctx context.Context, baseURL string, lease protocol.TaskLease) error {
	return wirePost(ctx, baseURL, runtimeapi.ActivityRuntimeNackPath, runtimeapi.ActivityRuntimeLeaseRequest{Lease: lease}, nil)
}

func pollWorkflowTask(ctx context.Context, baseURL string, timeout time.Duration) (protocol.TaskLease, error) {
	var lease protocol.TaskLease
	err := wirePost(ctx, baseURL, runtimeapi.WorkflowTaskRuntimePollPath, runtimeapi.WorkflowTaskRuntimePollRequest{TimeoutMillis: timeout.Milliseconds()}, &lease)
	return lease, err
}

func nackWorkflowTask(ctx context.Context, baseURL string, lease protocol.TaskLease) error {
	return wirePost(ctx, baseURL, runtimeapi.WorkflowTaskRuntimeNackPath, runtimeapi.WorkflowTaskRuntimeLeaseRequest{Lease: lease}, nil)
}
