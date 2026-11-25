package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/registration"
)

// Activity-role Worker 经 WorkflowRuntimeHTTPClient 登记定义，语义是「新内容首次出现时激活」。
func TestWorkflowRuntimeClientRegistersWorkflowJSON(t *testing.T) {
	var request runtimeapi.WorkflowRuntimeRegisterRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != runtimeapi.WorkflowRuntimeArtifactsPath {
			t.Errorf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(runtimeapi.WorkflowConfigRef{Version: "version-1"})
	}))
	defer server.Close()

	var registrar registration.Registrar = NewWorkflowRuntimeHTTPClient(server.URL, nil, RuntimeHTTPClientOptions{})
	version, err := registrar.RegisterWorkflowJSON(context.Background(), "orders", []byte(`{"type":"order-flow"}`))
	if err != nil {
		t.Fatal(err)
	}
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(request.Workflow, &workflow); err != nil {
		t.Fatal(err)
	}
	if version != "version-1" || request.Namespace != "orders" || workflow.Type != "order-flow" || !request.ActivateIfNew {
		t.Fatalf("version = %q, request = %+v", version, request)
	}
}

func TestRuntimeTaskTokenIsRequired(t *testing.T) {
	if _, err := RuntimeTaskToken(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-a", ScheduledEventID: 1}); err == nil {
		t.Fatal("accepted an activity without a task token")
	}
}

func newCaptureTaskClient() (*WorkflowTaskRuntimeHTTPClient, *captureRoundTripper) {
	transport := &captureRoundTripper{}
	return NewWorkflowTaskRuntimeHTTPClient("http://workflow-runtime", &http.Client{Transport: transport}, RuntimeHTTPClientOptions{}), transport
}

func TestWorkflowTaskRuntimeHTTPClientGetRunStatusUsesContext(t *testing.T) {
	transport := &blockingRoundTripper{}
	client := NewWorkflowTaskRuntimeHTTPClient("http://workflow-runtime", &http.Client{Transport: transport}, RuntimeHTTPClientOptions{})
	ctx, cancel := context.WithTimeout(protocol.WithWorkflowTaskToken(context.Background(), "task-token"), 10*time.Millisecond)
	defer cancel()

	_, err := client.GetRunStatusContext(ctx, "run-context")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if transport.gotContext == nil {
		t.Fatalf("round tripper did not receive request")
	}
	if err := transport.gotContext.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request context was not cancelled by caller context: %v", err)
	}
}

func TestWorkflowTaskRuntimeHTTPClientCompletesWithTaskTokenOnly(t *testing.T) {
	client, transport := newCaptureTaskClient()
	ctx := protocol.WithWorkflowTaskToken(context.Background(), "task-token")

	if err := client.HandleWorkflowTaskCompleted(ctx, "run-token-only", 7, []history.Command{{Type: history.CmdCompleteWorkflow}}, ""); err != nil {
		t.Fatal(err)
	}
	body := string(transport.body)
	if !strings.Contains(body, `"task_token"`) {
		t.Fatalf("expected task token in request body: %s", body)
	}
	if strings.Contains(body, `"run_id"`) || strings.Contains(body, `"scheduled_event_id"`) {
		t.Fatalf("workflow task runtime request must use token-only identity: %s", body)
	}
}

func TestWorkflowTaskRuntimeHTTPClientPreservesUpdateOutcome(t *testing.T) {
	client, transport := newCaptureTaskClient()
	payload, err := protocol.NewJSONPayload(uint64(18446744073709551615))
	if err != nil {
		t.Fatal(err)
	}
	outcome := protocol.UpdateOutcome{Success: &payload}
	ctx := protocol.WithWorkflowTaskToken(context.Background(), "task-token")
	if err := client.HandleWorkflowTaskCompleted(ctx, "run-update", 7, []history.Command{{
		Type: history.CmdCompleteWorkflowUpdate, Name: "create", UpdateID: "u-1", UpdateOutcome: &outcome,
	}}, "worker-a"); err != nil {
		t.Fatal(err)
	}
	var request struct {
		Commands []history.Command `json:"commands"`
	}
	if err := json.Unmarshal(transport.body, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Commands) != 1 || request.Commands[0].UpdateOutcome == nil || request.Commands[0].UpdateOutcome.Success == nil {
		t.Fatalf("update outcome missing from command wire: %s", transport.body)
	}
	if got := string(request.Commands[0].UpdateOutcome.Success.Data); got != "18446744073709551615" {
		t.Fatalf("payload data = %s", got)
	}
}

func TestWorkflowTaskRuntimeHTTPClientRequiresServerIssuedToken(t *testing.T) {
	client, _ := newCaptureTaskClient()
	if err := client.HandleWorkflowTaskCompleted(context.Background(), "run-a", 7, nil, "worker-a"); err == nil {
		t.Fatal("workflow completion accepted a request without a server-issued token")
	}
}

// Block 路径（HandleWorkflowTaskFailed）与 complete 对称：token-only 身份，cause 与 reason 在请求体里。
func TestWorkflowTaskRuntimeHTTPClientFailsWithTaskTokenAndCause(t *testing.T) {
	client, transport := newCaptureTaskClient()
	ctx := protocol.WithWorkflowTaskToken(context.Background(), "task-token")

	if err := client.HandleWorkflowTaskFailed(ctx, "run-fail-token", 9, history.WorkflowTaskFailedCauseNonDeterministic, "boom"); err != nil {
		t.Fatal(err)
	}
	body := string(transport.body)
	if !strings.Contains(body, `"task_token"`) {
		t.Fatalf("expected task token in request body: %s", body)
	}
	if !strings.Contains(body, history.WorkflowTaskFailedCauseNonDeterministic) {
		t.Fatalf("expected cause in request body: %s", body)
	}
	if strings.Contains(body, `"run_id"`) || strings.Contains(body, `"scheduled_event_id"`) {
		t.Fatalf("workflow task runtime request must use token-only identity: %s", body)
	}
}

type blockingRoundTripper struct {
	gotContext context.Context
}

func (t *blockingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.gotContext = req.Context()
	<-req.Context().Done()
	return nil, req.Context().Err()
}

type captureRoundTripper struct {
	body []byte
}

func (t *captureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var payload map[string]any
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		return nil, err
	}
	t.body, _ = json.Marshal(payload)
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
}
