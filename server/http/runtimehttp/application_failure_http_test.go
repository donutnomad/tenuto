package runtimehttp

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// failCaptureRuntime 只实现失败上报，其余方法通过嵌入的 nil 接口暴露。
type failCaptureRuntime struct {
	activitytasks.API
	failures []protocol.Failure
}

func (r *failCaptureRuntime) FailActivityTask(_ context.Context, _ protocol.Task, failure protocol.Failure) error {
	r.failures = append(r.failures, failure)
	return nil
}

func newFailureHTTPTask(t *testing.T) protocol.Task {
	t.Helper()
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-fail", ScheduledEventID: 7, TaskQueue: "payments"}
	token, err := testTaskTokens().Encode(task)
	if err != nil {
		t.Fatalf("EncodeTaskToken: %v", err)
	}
	task.TaskToken = token
	return task
}

// 失败上报统一走 failure 载荷：服务端拿到完整结构化 Failure。
func TestFailRequestCarriesStructuredFailure(t *testing.T) {
	runtime := &failCaptureRuntime{}
	server := httptest.NewServer(NewActivityRuntimeHTTPHandler(activityRuntimeForTest{API: runtime}, testTaskTokens(), slog.New(slog.DiscardHandler)))
	defer server.Close()
	task := newFailureHTTPTask(t)

	failure := protocol.Failure{
		Message:      "card declined",
		Type:         "PaymentDeclined",
		NonRetryable: true,
		Details:      protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`"card-1"`)}},
		Cause:        &protocol.Failure{Message: "inner", Type: "InnerType"},
	}
	if err := wirePost(context.Background(), server.URL, runtimeapi.ActivityRuntimeFailPath, runtimeapi.ActivityRuntimeFailRequest{
		ActivityRuntimeTaskRequest: runtimeapi.ActivityRuntimeTaskRequest{TaskToken: task.TaskToken}, Failure: &failure,
	}, nil); err != nil {
		t.Fatalf("FailActivityTask: %v", err)
	}
	if len(runtime.failures) != 1 {
		t.Fatalf("fail calls = %d, want 1", len(runtime.failures))
	}
	got := runtime.failures[0]
	if !reflect.DeepEqual(got, failure) {
		t.Fatalf("server failure = %+v, want %+v", got, failure)
	}
	if got.Type != "PaymentDeclined" || !got.NonRetryable {
		t.Fatalf("failure scalars = %+v", got)
	}
	if len(got.Details) != 1 || got.Details[0].Encoding != protocol.PayloadEncodingJSONPlainV1 || string(got.Details[0].Data) != `"card-1"` {
		t.Fatalf("failure details = %+v", got.Details)
	}
	if got.Cause == nil || got.Cause.Type != "InnerType" {
		t.Fatalf("failure cause = %+v", got.Cause)
	}
}
