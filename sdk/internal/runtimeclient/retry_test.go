package runtimeclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

var fastRetry = RetryPolicy{InitialInterval: 5 * time.Millisecond, MaximumInterval: 20 * time.Millisecond}

// statusServer 对前 failures 个请求应答 status，之后应答 ok。
func statusServer(t *testing.T, status int, failures int64, ok func(http.ResponseWriter)) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= failures {
			http.Error(w, "unavailable", status)
			return
		}
		ok(w)
	}))
	t.Cleanup(server.Close)
	return server, &attempts
}

func writeRun(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(runtimeapi.RunWithBlocked{Run: &runtimeapi.Run{ID: "run-1"}})
}

func TestRetryTransientStatusThenSucceed(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusGatewayTimeout} {
		server, attempts := statusServer(t, status, 2, writeRun)
		client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
		resp, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"})
		if err != nil || resp.Run.ID != "run-1" {
			t.Fatalf("status %d: GetRun = %+v, %v", status, resp, err)
		}
		if got := attempts.Load(); got != 3 {
			t.Fatalf("status %d: attempts = %d, want 3", status, got)
		}
	}
}

func TestRetryDoesNotRepeatClientErrors(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusInternalServerError} {
		server, attempts := statusServer(t, status, 100, writeRun)
		client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
		_, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.Code != status {
			t.Fatalf("status %d: err = %v", status, err)
		}
		if got := attempts.Load(); got != 1 {
			t.Fatalf("status %d: attempts = %d, want 1", status, got)
		}
	}
}

// 502/504 时请求可能已被处理：重发会再执行一次的调用不重试；503/429 表示没有被处理，照常重试。
func TestRetryUnsentCallOnlyOnUnprocessedStatus(t *testing.T) {
	server, attempts := statusServer(t, http.StatusBadGateway, 100, func(w http.ResponseWriter) {})
	client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
	if _, err := client.ResetWorkflow(context.Background(), runtimeapi.WorkflowRuntimeResetRequest{RunID: "run-1"}); err == nil {
		t.Fatal("reset succeeded against 502")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("reset attempts after 502 = %d, want 1", got)
	}
	server, attempts = statusServer(t, http.StatusServiceUnavailable, 2, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(runtimeapi.WorkflowRuntimeResetResponse{RunID: "run-2"})
	})
	client = NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
	if runID, err := client.ResetWorkflow(context.Background(), runtimeapi.WorkflowRuntimeResetRequest{RunID: "run-1"}); err != nil || runID != "run-2" {
		t.Fatalf("reset after 503 = %q, %v", runID, err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("reset attempts after 503 = %d, want 3", got)
	}
}

// 带 request_id 的 Reset 重发返回第一次 reset 出的 Run：502/504 与连接中断照常重试。
func TestRetryResetWithRequestIDAsIdempotent(t *testing.T) {
	for _, status := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		server, attempts := statusServer(t, status, 2, func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(runtimeapi.WorkflowRuntimeResetResponse{RunID: "run-2"})
		})
		client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
		runID, err := client.ResetWorkflow(context.Background(), runtimeapi.WorkflowRuntimeResetRequest{RunID: "run-1", RequestID: "reset-1"})
		if err != nil || runID != "run-2" {
			t.Fatalf("status %d: reset with request id = %q, %v", status, runID, err)
		}
		if got := attempts.Load(); got != 3 {
			t.Fatalf("status %d: reset attempts = %d, want 3", status, got)
		}
	}
}

// 连接没有建立（拒绝连接）时请求确定没有到达 Server：重发会再执行一次的调用也重试。
func TestRetryUnsentCallOnDialFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(runtimeapi.WorkflowRuntimeResetResponse{RunID: "run-2"})
	}))
	defer server.Close()
	var dials atomic.Int64
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dials.Add(1) <= 2 {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	client := NewWorkflowRuntimeHTTPClient(server.URL, &http.Client{Transport: transport}, RuntimeHTTPClientOptions{Retry: fastRetry})
	if runID, err := client.ResetWorkflow(context.Background(), runtimeapi.WorkflowRuntimeResetRequest{RunID: "run-1"}); err != nil || runID != "run-2" {
		t.Fatalf("reset = %q, %v", runID, err)
	}
	if got := dials.Load(); got != 3 {
		t.Fatalf("dials = %d, want 3", got)
	}
}

// 调用方 RoundTripper 自己的错误（鉴权、配置）不是瞬时错误，不重试。
func TestRetryIgnoresRoundTripperErrors(t *testing.T) {
	var calls atomic.Int64
	rt := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("token expired")
	})
	client := NewWorkflowRuntimeHTTPClient("http://runtime", &http.Client{Transport: rt}, RuntimeHTTPClientOptions{Retry: fastRetry})
	if _, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"}); err == nil {
		t.Fatal("GetRun succeeded")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

// Server 证书不受信任是配置错误：握手失败不重试（Server 拒绝客户端证书的 alert 见 sdk/client 的 mTLS 测试）。
func TestRetryDoesNotRepeatTLSVerificationFailure(t *testing.T) {
	var handshakes atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeRun(w) }))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			handshakes.Add(1)
		}
	}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	// 缺省 Transport 不信任 httptest 的自签证书。
	client := NewWorkflowRuntimeHTTPClient(server.URL, &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}, RuntimeHTTPClientOptions{Retry: fastRetry})
	_, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"})
	var verifyErr *tls.CertificateVerificationError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("err = %v, want a certificate verification error", err)
	}
	if got := handshakes.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// ctx 有 deadline 时重试到 deadline 为止，返回最后一次的错误；没有 deadline 时重试 MaximumElapsed。
func TestRetryStopsAtDeadlineOrMaximumElapsed(t *testing.T) {
	server, attempts := statusServer(t, http.StatusServiceUnavailable, 1<<30, writeRun)

	client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.GetRun(ctx, runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"})
	elapsed := time.Since(started)
	var statusErr *StatusError
	if !errors.As(err, &statusErr) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the last 503 or the deadline", err)
	}
	if elapsed > 400*time.Millisecond || attempts.Load() < 3 {
		t.Fatalf("returned after %v and %d attempts; want several attempts within the 200ms deadline", elapsed, attempts.Load())
	}

	attempts.Store(0)
	policy := fastRetry
	policy.MaximumElapsed = 150 * time.Millisecond
	client = NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: policy})
	started = time.Now()
	if _, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"}); !errors.As(err, &statusErr) {
		t.Fatalf("err = %v, want the last 503", err)
	}
	if elapsed := time.Since(started); elapsed > 400*time.Millisecond || attempts.Load() < 3 {
		t.Fatalf("returned after %v and %d attempts; want several attempts within 150ms", elapsed, attempts.Load())
	}
}

func TestRetryDisabled(t *testing.T) {
	server, attempts := statusServer(t, http.StatusServiceUnavailable, 1, writeRun)
	client := NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: RetryPolicy{Disabled: true}})
	if _, err := client.GetRun(context.Background(), runtimeapi.WorkflowRuntimeRunRequest{RunID: "run-1"}); err == nil {
		t.Fatal("GetRun succeeded against 503 with retries disabled")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

// Worker 的长轮询遇到连接断开（副本重启）时重发，拿到下一个副本给出的任务。
func TestPollRetriesDroppedConnection(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.TaskLease{Token: "lease-1", Task: protocol.Task{RunID: "run-1"}})
	}))
	defer server.Close()
	client := NewWorkflowTaskRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{Retry: fastRetry})
	lease, err := client.PollWorkflowTask(context.Background())
	if err != nil || lease.Token != "lease-1" {
		t.Fatalf("poll = %+v, %v", lease, err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}
