package runtimeclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

func TestRuntimePostKeepsExactNumbersInResponses(t *testing.T) {
	// 应答里的用户载荷（Query 结果、Update 结果、Activity 输入）保留数字的精确值。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":{"amount":9007199254740993,"rate":0.25}}`))
	}))
	defer server.Close()
	var resp struct {
		Result map[string]any `json:"result"`
	}
	if err := runtimePost(context.Background(), server.Client(), server.URL, "/", "test api", nil, map[string]any{}, &resp); err != nil {
		t.Fatal(err)
	}
	if got := resp.Result["amount"]; got != int64(9007199254740993) {
		t.Fatalf("amount = %#v", got)
	}
	if got := resp.Result["rate"]; got != 0.25 {
		t.Fatalf("rate = %#v", got)
	}
}

// 2xx 空 body 不是成功：不解析应答体的调用（Signal、Cancel、Activity 完成）也要报错，幂等调用照常重试。
func TestRuntimePostTreatsEmptySuccessBodyAsTransportError(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) <= 2 {
			return // 200 与空 body
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	if err := runtimePost(context.Background(), server.Client(), server.URL, "/", "test api", nil, map[string]any{}, nil); err == nil {
		t.Fatal("empty 200 body was treated as success")
	} else if _, ok := errors.AsType[*transportError](err); !ok {
		t.Fatalf("err = %T %v, want *transportError", err, err)
	}
	client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
	if err := client.CancelWorkflow(context.Background(), runtimeapi.WorkflowRuntimeCancelRequest{RunID: "run-1"}); err != nil {
		t.Fatalf("cancel after an empty 200 = %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

// 心跳应答 404（Server 上没有这份登记）映射成 protocol.ErrActivityWorkerVersionNotRegistered，Worker 据此重新登记；
// 其他失败不映射。
func TestHeartbeatActivityWorkerVersionMapsNotFoundToNotRegistered(t *testing.T) {
	status := http.StatusNotFound
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "activity worker version is not registered: default/pod-1/v1", status)
	}))
	defer server.Close()
	client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{})
	if err := client.HeartbeatActivityWorkerVersionInNamespace(context.Background(), "default", "pod-1", "v1"); !errors.Is(err, protocol.ErrActivityWorkerVersionNotRegistered) {
		t.Fatalf("404 heartbeat = %v, want ErrActivityWorkerVersionNotRegistered", err)
	}
	status = http.StatusBadRequest
	if err := client.HeartbeatActivityWorkerVersionInNamespace(context.Background(), "default", "", "v1"); err == nil || errors.Is(err, protocol.ErrActivityWorkerVersionNotRegistered) {
		t.Fatalf("400 heartbeat = %v, want a plain error", err)
	}
}
