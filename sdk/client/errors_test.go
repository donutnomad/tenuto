package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// mapError 的映射表：每个状态码与调用种类对应一个错误；Server 发出的应答体原样进 Message。
func TestErrorMappingTable(t *testing.T) {
	const rejection = `{"message":"card has expired","type":"CardExpired","nonRetryable":true,"details":[{"encoding":"json/plain-v1","data":"InJlYXNvbiI="}]}`
	cases := []struct {
		name    string
		kind    callKind
		status  int
		body    string
		is      []error
		isNot   []error
		appType string
	}{
		{name: "400 invalid argument", kind: callOther, status: http.StatusBadRequest, body: "invalid request: workflow type is empty", is: []error{ErrInvalidArgument}, isNot: []error{ErrUpdateRejected}},
		{name: "404 not found", kind: callOther, status: http.StatusNotFound, body: "run not found", is: []error{ErrNotFound}},
		{name: "409 conflict", kind: callOther, status: http.StatusConflict, body: "schedule already exists", is: []error{ErrConflict}, isNot: []error{ErrWorkflowExecutionAlreadyStarted}},
		{name: "409 on start", kind: callStart, status: http.StatusConflict, body: "current run conflict", is: []error{ErrWorkflowExecutionAlreadyStarted, ErrConflict}},
		{name: "504 deadline", kind: callOther, status: http.StatusGatewayTimeout, body: "context deadline exceeded", is: []error{ErrDeadlineExceeded}},
		{name: "500 internal", kind: callOther, status: http.StatusInternalServerError, body: "internal server error", is: []error{ErrInternal}, isNot: []error{ErrUnavailable}},
		{name: "503 unavailable", kind: callOther, status: http.StatusServiceUnavailable, body: "service temporarily unavailable", is: []error{ErrUnavailable}, isNot: []error{ErrInternal}},
		{name: "502 unavailable", kind: callOther, status: http.StatusBadGateway, body: "service temporarily unavailable; the request may have taken effect", is: []error{ErrUnavailable}},
		{name: "400 update rejection", kind: callUpdate, status: http.StatusBadRequest, body: rejection, is: []error{ErrUpdateRejected}, isNot: []error{ErrInvalidArgument}, appType: "CardExpired"},
		{name: "400 plain text on update", kind: callUpdate, status: http.StatusBadRequest, body: "decode request: unexpected EOF", is: []error{ErrInvalidArgument}, isNot: []error{ErrUpdateRejected}},
		{name: "other status", kind: callOther, status: http.StatusMethodNotAllowed, body: "method is not allowed", isNot: []error{ErrInvalidArgument, ErrNotFound, ErrConflict, ErrInternal}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, tc.body, tc.status)
			}))
			defer server.Close()
			// 只看一次应答的映射；502/503/504 的重试由 retry_test 覆盖。
			c, err := Dial(Options{HostPort: server.URL, Retry: RetryOptions{Disabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			conn := c.(*workflowClient).conn
			err = conn.Workflow.CancelWorkflow(context.Background(), runtimeapi.WorkflowRuntimeCancelRequest{RunID: "run"})
			err = mapError(tc.kind, err)
			for _, target := range tc.is {
				if !errors.Is(err, target) {
					t.Errorf("errors.Is(%v, %v) = false", err, target)
				}
			}
			for _, target := range tc.isNot {
				if errors.Is(err, target) {
					t.Errorf("errors.Is(%v, %v) = true", err, target)
				}
			}
			if tc.appType != "" {
				var appErr *apperror.Error
				var reason string
				if !errors.As(err, &appErr) || appErr.Type() != tc.appType || !appErr.NonRetryable() || appErr.Details(&reason) != nil || reason != "reason" {
					t.Fatalf("rejection = %#v", err)
				}
				return
			}
			var serviceErr *ServiceError
			if !errors.As(err, &serviceErr) || serviceErr.StatusCode != tc.status || serviceErr.Message != tc.body {
				t.Fatalf("service error = %#v", err)
			}
		})
	}
	if err := mapError(callOther, context.Canceled); err != context.Canceled {
		t.Fatalf("a transport error must pass through, got %v", err)
	}
}

func TestDialValidatesHostPort(t *testing.T) {
	for hostPort, wantErr := range map[string]bool{
		"":                      true,
		"ftp://example.com":     true,
		"http://":               true,
		"localhost:7400":        false,
		"https://tenuto.local/": false,
	} {
		c, err := Dial(Options{HostPort: hostPort})
		if (err != nil) != wantErr {
			t.Errorf("Dial(%q) err = %v", hostPort, err)
		}
		if c != nil {
			c.Close()
		}
	}
}
