package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

// Worker 停止时取消长轮询。应答已经开始到达时 Server 已交出任务（Workflow Task 已 started），取消不能丢弃它：
// lease 照常返回，由 Worker 在宽限期内处理。应答到达之前取消则中止请求，Server 发现断开后交还任务。
func TestPollKeepsTaskWhoseResponseArrivedBeforeCancel(t *testing.T) {
	cancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-cancelled
		_ = json.NewEncoder(w).Encode(protocol.TaskLease{Token: "lease-1", Task: protocol.Task{RunID: "run-1"}})
	}))
	defer server.Close()

	for name, poll := range map[string]func(context.Context) (protocol.TaskLease, error){
		"workflow": NewWorkflowTaskRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{}).PollWorkflowTask,
		"activity": func(ctx context.Context) (protocol.TaskLease, error) {
			return NewActivityRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{}).PollActivityTask(ctx, "default")
		},
	} {
		t.Run(name, func(t *testing.T) {
			cancelled = make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: func() {
				cancel()
				close(cancelled)
			}})
			lease, err := poll(ctx)
			if err != nil {
				t.Fatalf("应答到达之后取消，poll 不应失败: %v", err)
			}
			if lease.Token != "lease-1" || lease.Task.RunID != "run-1" {
				t.Fatalf("lease = %+v", lease)
			}
		})
	}
}

func TestPollCancelledBeforeResponseAbortsRequest(t *testing.T) {
	requestGone := make(chan struct{})
	polling := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 读完请求体之后 Server 才能察觉连接断开（真实端点先解码请求）。
		_, _ = io.Copy(io.Discard, r.Body)
		close(polling)
		<-r.Context().Done()
		close(requestGone)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-polling
		cancel()
	}()
	_, err := NewWorkflowTaskRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{}).PollWorkflowTask(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	<-requestGone
}
