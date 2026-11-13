package runtimehttp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 缺陷 5：Update validator 拒绝经 HTTP 保持可判定
// ============================================
// 组合边界：真实 engine.Server + httptest + 原始 HTTP 请求。
// 覆盖：拒绝仍同步失败且零 history；validator 错误按 protocol.Failure 跨 wire（typed 字段原样保留）；
// 其它端点的非 2xx 应答体是错误文本。SDK 侧的错误还原在 sdk/client 的测试里。
// ============================================

const rejectionUpdateName = "set_priority"

// newRejectionRuntime 建一个 Running run + 用给定错误拒绝指定 update name 的 validator。
func newRejectionRuntime(t *testing.T, runID string, validatorErr error) (*engine.Server, execution.ExecutionManager) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	server.RegisterUpdateValidator(rejectionUpdateName, func(string, protocol.Payloads) error {
		return validatorErr
	})
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, Namespace: protocol.DefaultNamespace}))
	}); err != nil {
		t.Fatal(err)
	}
	return server, mgr
}

// rejectionHTTP 把请求发到一个 handler。
type rejectionHTTP struct {
	url string
}

func newRejectionHTTPClient(t *testing.T, handler http.Handler) (rejectionHTTP, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	return rejectionHTTP{url: server.URL}, server.Close
}

// update 发送 accept-only 的 Update，返回拒绝时 400 应答体解出的 Failure。
func (c rejectionHTTP) update(t *testing.T, runID, updateID string) protocol.Failure {
	t.Helper()
	err := wirePost(context.Background(), c.url, runtimeapi.WorkflowRuntimeUpdatePath, runtimeapi.WorkflowRuntimeUpdateRequest{
		WorkflowID: "wf-" + runID, RunID: runID, Name: rejectionUpdateName, UpdateID: updateID,
		WaitStage: string(protocol.UpdateWaitStageEnums.Accepted), Args: protocol.Payloads{},
	}, nil)
	var status *wireStatusError
	if !errors.As(err, &status) || status.code != http.StatusBadRequest {
		t.Fatalf("rejection must be a 400, got %v", err)
	}
	var failure protocol.Failure
	if err := json.Unmarshal([]byte(status.body), &failure); err != nil {
		t.Fatalf("rejection body is not a Failure: %q", status.body)
	}
	return failure
}

func (c rejectionHTTP) start() error {
	return wirePost(context.Background(), c.url, runtimeapi.WorkflowRuntimeStartPath,
		runtimeapi.WorkflowRuntimeStartRequest{WorkflowType: "wf-1", WorkflowID: "wf-1"}, nil)
}

// Error Case（typed validator）：拒绝跨 HTTP 后仍是同步拒绝，应答体是逐字段保留的 Failure，零 history。
func TestUpdateRejectionSurvivesHTTP(t *testing.T) {
	decodeTarget := struct {
		Reason string `json:"reason"`
	}{}
	validatorErr := apperror.NewWithOptions("card has expired", "CardExpired", apperror.Options{
		NonRetryable: true,
		Cause:        apperror.New("inner failure", "InnerType"),
		Details:      []any{map[string]any{"reason": "insufficient_funds"}},
	})
	const runID = "run-update-reject-http"
	runtime, mgr := newRejectionRuntime(t, runID, validatorErr)
	client, closeServer := newRejectionHTTPClient(t, NewWorkflowRuntimeHTTPHandler(runtime))
	defer closeServer()

	failure := client.update(t, runID, "u-reject")
	if failure.Message != "card has expired" || failure.Type != "CardExpired" || !failure.NonRetryable {
		t.Fatalf("typed rejection fields lost: %+v", failure)
	}
	if len(failure.Details) != 1 {
		t.Fatalf("details lost on wire: %+v", failure.Details)
	}
	if failure.Cause == nil || failure.Cause.Type != "InnerType" {
		t.Fatalf("cause lost on wire: %+v", failure.Cause)
	}
	if decodeErr := failure.Details[0].Decode(&decodeTarget); decodeErr != nil || decodeTarget.Reason != "insufficient_funds" {
		t.Fatalf("detail payload mangled: err=%v value=%+v", decodeErr, decodeTarget)
	}

	// 零 history：拒绝发生在写 accepted 之前。
	events, getErr := mgr.GetEvents(runID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	for i := range events {
		if events[i].WorkflowUpdateAccepted != nil || events[i].WorkflowUpdateCompleted != nil {
			t.Fatalf("rejection must write zero history, got %+v", events[i])
		}
	}
}

// Error Case（普通 error validator）：Failure 只有原因文本，Type 为空且无 details/cause。
func TestUpdateRejectionFromPlainErrorKeepsMessage(t *testing.T) {
	const runID = "run-update-reject-plain"
	runtime, _ := newRejectionRuntime(t, runID, errors.New("priority is required"))
	client, closeServer := newRejectionHTTPClient(t, NewWorkflowRuntimeHTTPHandler(runtime))
	defer closeServer()

	failure := client.update(t, runID, "u-plain")
	if failure.Message != "priority is required" || failure.Type != "" {
		t.Fatalf("plain validator failure must stay message-only, got %+v", failure)
	}
	if failure.Cause != nil || len(failure.Details) != 0 {
		t.Fatalf("plain validator failure must carry no cause/details, got %+v", failure)
	}
}

// Edge Case: 其它端点的 4xx 应答体是错误文本。
func TestOtherEndpointErrorTextUnchanged(t *testing.T) {
	client, closeServer := newRejectionHTTPClient(t, failingEngineHandler(protocol.InvalidRequestf("boom")))
	defer closeServer()

	var status *wireStatusError
	if err := client.start(); !errors.As(err, &status) || status.code != http.StatusBadRequest || status.body != "boom" {
		t.Fatalf("400 response = %v", err)
	}
}

// Error Case: 服务端故障是 500，响应体是固定文本，存储层的错误细节只进日志。
func TestServerFaultIs500WithGenericBody(t *testing.T) {
	client, closeServer := newRejectionHTTPClient(t, failingEngineHandler(errors.New("Error 1045: Access denied for user 'tenuto'@'10.0.0.5'")))
	defer closeServer()

	var status *wireStatusError
	if err := client.start(); !errors.As(err, &status) || status.code != http.StatusInternalServerError || status.body != "internal server error" {
		t.Fatalf("server fault response = %v", err)
	}
}

// failingEngineHandler 挂 workflow 端点，引擎操作由 failingRuntime 应答。
func failingEngineHandler(err error) http.Handler {
	router := newRuntimeRouter(slog.New(slog.DiscardHandler), nil)
	api := &workflowRuntimeHTTPAPI{engine: &failingRuntime{err: err}}
	NewWorkflowRuntimeHTTPAPIWrap(api, api).BindAll(router)
	return router
}

// failingRuntime 只实现被点名的方法，其余通过嵌入的 nil 接口暴露。
type failingRuntime struct {
	workflowEngine
	err error
}

func (r *failingRuntime) UpdateWorkflow(context.Context, string, string, string, string, string, protocol.UpdateWaitStage, protocol.Payloads) (protocol.WorkflowUpdateResult, error) {
	return protocol.WorkflowUpdateResult{}, r.err
}

func (r *failingRuntime) StartWorkflow(context.Context, string, string, string, string, map[string]any) (*execution.Run, error) {
	return nil, r.err
}
