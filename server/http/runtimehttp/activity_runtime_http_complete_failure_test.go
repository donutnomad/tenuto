package runtimehttp

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// ============================================
// 缺陷 6：activity complete 端点携带失败载荷
// ============================================
// 单轨：complete 请求 body 统一 {output, failure}，failure != nil 即失败。
// 覆盖：结构化失败到达运行时、成功调用（failure 省略）、服务端入口复用 protocol.Failure.Validate。
// ============================================

type completionCall struct {
	output  map[string]any
	failure *protocol.Failure
}

type completionCaptureRuntime struct {
	activitytasks.API
	calls    []completionCall
	failures []protocol.Failure
}

func (r *completionCaptureRuntime) CompleteActivityTask(_ context.Context, _ protocol.Task, output map[string]any, failure *protocol.Failure) error {
	r.calls = append(r.calls, completionCall{output: output, failure: failure})
	return nil
}

func (r *completionCaptureRuntime) FailActivityTask(_ context.Context, _ protocol.Task, failure protocol.Failure) error {
	r.failures = append(r.failures, failure)
	return nil
}

// completionHTTP 把 complete 请求发到只挂 Activity 端点的 handler。
type completionHTTP struct {
	url string
}

func newCompletionHTTPClient(t *testing.T, runtime activitytasks.API) (completionHTTP, func()) {
	t.Helper()
	server := httptest.NewServer(NewActivityRuntimeHTTPHandler(activityRuntimeForTest{API: runtime}, testTaskTokens(), slog.New(slog.DiscardHandler)))
	return completionHTTP{url: server.URL}, server.Close
}

func (c completionHTTP) CompleteActivityTask(ctx context.Context, task protocol.Task, output map[string]any, failure *protocol.Failure) error {
	return wirePost(ctx, c.url, runtimeapi.ActivityRuntimeCompletePath, runtimeapi.ActivityRuntimeCompleteRequest{
		TaskToken: task.TaskToken, Task: task, Output: output, Failure: failure,
	}, nil)
}

// Error Case: 结构化失败（空 message）经 complete 端点原样到达运行时，且不落成成功。
func TestCompleteRequestCarriesStructuredFailure(t *testing.T) {
	runtime := &completionCaptureRuntime{}
	client, closeServer := newCompletionHTTPClient(t, runtime)
	defer closeServer()

	failure := &protocol.Failure{Type: "PaymentDeclined", NonRetryable: true}
	if err := client.CompleteActivityTask(context.Background(), newFailureHTTPTask(t), nil, failure); err != nil {
		t.Fatalf("CompleteActivityTask: %v", err)
	}
	if len(runtime.calls) != 1 {
		t.Fatalf("complete calls = %d, want 1", len(runtime.calls))
	}
	call := runtime.calls[0]
	if call.failure == nil || call.failure.Type != "PaymentDeclined" || !call.failure.NonRetryable || call.failure.Message != "" {
		t.Fatalf("structured failure mangled on wire: %+v", call.failure)
	}
	if len(runtime.failures) != 0 {
		t.Fatalf("structured completion must not route to fail: %+v", runtime.failures)
	}
}

// Happy Path：failure 为 nil 的成功调用行为不变。
func TestCompleteRequestNilFailureSucceeds(t *testing.T) {
	runtime := &completionCaptureRuntime{}
	client, closeServer := newCompletionHTTPClient(t, runtime)
	defer closeServer()

	if err := client.CompleteActivityTask(context.Background(), newFailureHTTPTask(t), map[string]any{"paid": true}, nil); err != nil {
		t.Fatalf("CompleteActivityTask: %v", err)
	}
	if len(runtime.calls) != 1 {
		t.Fatalf("complete calls = %d, want 1", len(runtime.calls))
	}
	if runtime.calls[0].failure != nil {
		t.Fatalf("nil failure must stay nil on wire, got %+v", runtime.calls[0].failure)
	}
	if paid, _ := runtime.calls[0].output["paid"].(bool); !paid {
		t.Fatalf("output lost on wire: %+v", runtime.calls[0].output)
	}
}

// Error Case: 超限 details 在 HTTP 入口明确失败，运行时不被调用。
func TestCompleteRequestRejectsInvalidFailure(t *testing.T) {
	runtime := &completionCaptureRuntime{}
	client, closeServer := newCompletionHTTPClient(t, runtime)
	defer closeServer()

	payload, err := protocol.NewJSONPayload("x")
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	details := make(protocol.Payloads, 1025) // 超过 maxUpdatePayloadCount=1024
	for i := range details {
		details[i] = payload
	}
	err = client.CompleteActivityTask(context.Background(), newFailureHTTPTask(t), nil, &protocol.Failure{Type: "T", Details: details})
	if err == nil || !strings.Contains(err.Error(), "invalid activity failure") {
		t.Fatalf("超限 details 应被入口拒绝, got %v", err)
	}
	if len(runtime.calls)+len(runtime.failures) != 0 {
		t.Fatalf("入口校验失败不应调用运行时: %+v", runtime)
	}
}
