package runtimehttp

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// runtimeHTTPRoutes 是远程 Worker、console 与 sdk/client 依赖的全部路由。
// noBody 标记不读取请求体的端点。
var runtimeHTTPRoutes = []struct {
	path   string
	noBody bool
}{
	{path: runtimeapi.ActivityRuntimePollPath},
	{path: runtimeapi.ActivityRuntimeAckPath},
	{path: runtimeapi.ActivityRuntimeNackPath},
	{path: runtimeapi.ActivityRuntimeBeginPath},
	{path: runtimeapi.ActivityRuntimeHeartbeatPath},
	{path: runtimeapi.ActivityRuntimeCancelPath},
	{path: runtimeapi.ActivityRuntimeCompletePath},
	{path: runtimeapi.ActivityRuntimeFailPath},
	{path: runtimeapi.ActivityRuntimeReleasePath},

	{path: runtimeapi.WorkflowTaskRuntimePollPath},
	{path: runtimeapi.WorkflowTaskRuntimeAckPath},
	{path: runtimeapi.WorkflowTaskRuntimeNackPath},
	{path: runtimeapi.WorkflowTaskRuntimeConfigPath},
	{path: runtimeapi.WorkflowTaskRuntimeEventsPath},
	{path: runtimeapi.WorkflowTaskRuntimeStatusPath},
	{path: runtimeapi.WorkflowTaskRuntimeCompletePath},
	{path: runtimeapi.WorkflowTaskRuntimeTimedOutPath},
	{path: runtimeapi.WorkflowTaskRuntimeFailedPath},
	{path: runtimeapi.WorkflowTaskRuntimeActivationPath},
	{path: runtimeapi.WorkflowTaskRuntimeCompleteActivationPath},

	{path: runtimeapi.WorkflowRuntimeRegisterPath},
	{path: runtimeapi.WorkflowRuntimeSetWorkerTargetPath},
	{path: runtimeapi.WorkflowRuntimeStartPath},
	{path: runtimeapi.WorkflowRuntimeStartVersionPath},
	{path: runtimeapi.WorkflowRuntimeCancelPath},
	{path: runtimeapi.WorkflowRuntimeTerminatePath},
	{path: runtimeapi.WorkflowRuntimeGetRunPath},
	{path: runtimeapi.WorkflowRuntimeWaitClosedPath},
	{path: runtimeapi.WorkflowRuntimeDispatchPath, noBody: true},
	{path: runtimeapi.WorkflowRuntimeRegisterActivityWorkerPath},
	{path: runtimeapi.WorkflowRuntimeHeartbeatActivityWorkerPath},
	{path: runtimeapi.WorkflowRuntimeDeregisterActivityWorkerPath},
	{path: runtimeapi.WorkflowRuntimeListRunsPath},
	{path: runtimeapi.WorkflowRuntimeRunStatsPath},
	{path: runtimeapi.WorkflowRuntimeSearchAttributesPath},
	{path: runtimeapi.WorkflowRuntimeHistoryPath},
	{path: runtimeapi.WorkflowRuntimeHistoryPagePath},
	{path: runtimeapi.WorkflowRuntimeSignalPath},
	{path: runtimeapi.WorkflowRuntimeQueryPath},
	{path: runtimeapi.WorkflowRuntimeUpdatePath},
	{path: runtimeapi.WorkflowRuntimeUpdateWithStartPath},
	{path: runtimeapi.WorkflowRuntimeCapabilitiesPath, noBody: true},
	{path: runtimeapi.WorkflowRuntimeSignalWithStartPath},
	{path: runtimeapi.WorkflowRuntimeGetConfigPath},
	{path: runtimeapi.WorkflowRuntimeArtifactsPath},
	{path: runtimeapi.WorkflowRuntimeSetPolicyPath},
	{path: runtimeapi.WorkflowRuntimeListConfigsPath},
	{path: runtimeapi.WorkflowRuntimeListWorkersPath},
	{path: runtimeapi.WorkflowRuntimeCreateSchedulePath},
	{path: runtimeapi.WorkflowRuntimeGetSchedulePath},
	{path: runtimeapi.WorkflowRuntimeUpdateSchedulePath},
	{path: runtimeapi.WorkflowRuntimeDeleteSchedulePath},
	{path: runtimeapi.WorkflowRuntimePauseSchedulePath},
	{path: runtimeapi.WorkflowRuntimeResumeSchedulePath},
	{path: runtimeapi.WorkflowRuntimeListSchedulesPath},
	{path: runtimeapi.WorkflowRuntimeTriggerSchedulePath},
	{path: runtimeapi.WorkflowRuntimeStartBatchPath},
	{path: runtimeapi.WorkflowRuntimeGetBatchPath},
	{path: runtimeapi.WorkflowRuntimeListBatchesPath},
	{path: runtimeapi.WorkflowRuntimeCancelBatchPath},
	{path: runtimeapi.WorkflowRuntimeResetPath},
	{path: runtimeapi.WorkflowRuntimeResetActivityPath},
	{path: runtimeapi.WorkflowRuntimeListTaskQueuesPath},
	{path: runtimeapi.WorkflowRuntimeListNamespacesPath, noBody: true},
	{path: runtimeapi.WorkflowRuntimeActivityStatesPath},
}

func newRuntimeHTTPRouteTestHandler(t *testing.T) http.Handler {
	t.Helper()
	queue := newRuntimeHTTPTestQueue()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: queue, ActivityRouter: queue.activities()})
	t.Cleanup(server.Stop)
	return NewRuntimeHTTPHandler(server)
}

func serveRuntimeHTTP(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// 浏览器不经预检就能跨站发出的 POST（text/plain、表单、multipart）与缺少 Content-Type 的 POST 一律 415，
// 有请求体与没有请求体的端点都一样；带参数的 application/json 照常受理。
func TestRuntimeHTTPRequiresJSONContentType(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	paths := []string{runtimeapi.WorkflowRuntimeTerminatePath, runtimeapi.WorkflowRuntimeDispatchPath, runtimeapi.ActivityRuntimeCompletePath}
	for _, path := range paths {
		for _, contentType := range []string{"", "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "application/jsonx"} {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"run_id":"r"}`))
			if contentType != "" {
				request.Header.Set("Content-Type", contentType)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnsupportedMediaType {
				t.Errorf("POST %s with %q = %d %q, want 415", path, contentType, recorder.Code, recorder.Body.String())
			}
		}
	}
	request := httptest.NewRequest(http.MethodPost, runtimeapi.WorkflowRuntimeCapabilitiesPath, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("POST capabilities with charset = %d %q", recorder.Code, recorder.Body.String())
	}
}

// 路由表与清单逐条一致：不多不少，全部是 POST。
func TestRuntimeHTTPRouteInventory(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	var registered []string
	for _, route := range handler.(*gin.Engine).Routes() {
		registered = append(registered, route.Method+" "+route.Path)
	}
	var want []string
	for _, route := range runtimeHTTPRoutes {
		want = append(want, "POST "+route.path)
	}
	slices.Sort(registered)
	slices.Sort(want)
	if !slices.Equal(registered, want) {
		t.Fatalf("registered routes differ from the inventory\n got: %v\nwant: %v", registered, want)
	}
}

// 每条路由 POST 可达（坏 JSON 走到解码并得到既有的 400 文案），GET 得到既有的 405。
func TestRuntimeHTTPRoutesAcceptOnlyPost(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	for _, route := range runtimeHTTPRoutes {
		get := serveRuntimeHTTP(handler, http.MethodGet, route.path, "")
		if get.Code != http.StatusMethodNotAllowed || get.Body.String() != "method is not allowed\n" {
			t.Errorf("GET %s = %d %q", route.path, get.Code, get.Body.String())
		}

		post := serveRuntimeHTTP(handler, http.MethodPost, route.path, "{")
		if route.noBody {
			if post.Code != http.StatusOK || post.Header().Get("Content-Type") != "application/json" {
				t.Errorf("POST %s = %d %q (%s)", route.path, post.Code, post.Body.String(), post.Header().Get("Content-Type"))
			}
			continue
		}
		if post.Code != http.StatusBadRequest || !strings.HasPrefix(post.Body.String(), "decode request: ") {
			t.Errorf("POST %s = %d %q", route.path, post.Code, post.Body.String())
		}
	}
}

func TestRuntimeHTTPRejectsOversizedBody(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	body := `{"run_id":"` + strings.Repeat("a", protocol.MaxRuntimeRequestBody) + `"}`
	response := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeGetRunPath, body)
	if response.Code != http.StatusRequestEntityTooLarge || response.Body.String() != "request body exceeds 4194304 bytes\n" {
		t.Fatalf("oversized body = %d %q", response.Code, response.Body.String())
	}
}

func TestRuntimeErrorStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{execution.ErrRunNotFound, http.StatusNotFound},
		{definition.ErrWorkflowConfigNotFound, http.StatusNotFound},
		{fmt.Errorf("load run: %w", execution.ErrRunNotFound), http.StatusNotFound},
		{execution.ErrNoCurrentRun, http.StatusNotFound},
		{fmt.Errorf("%w: default/pod-1/v1", protocol.ErrActivityWorkerVersionNotRegistered), http.StatusNotFound},
		{fmt.Errorf("%w: default/batch-1", batch.ErrNotFound), http.StatusNotFound},
		{fmt.Errorf("%w: batch operation b-1 is completed, cannot become cancelled", batch.ErrInvalidTransition), http.StatusConflict},
		{batch.ErrConcurrentUpdate, http.StatusConflict},
		{fmt.Errorf("%w: default/sch-1", schedule.ErrNotFound), http.StatusNotFound},
		{fmt.Errorf("%w: default/sch-1", schedule.ErrAlreadyExists), http.StatusConflict},
		{schedule.ErrConcurrentUpdate, http.StatusConflict},
		{execution.ErrCurrentRunConflict, http.StatusConflict},
		{fmt.Errorf("run run-1: %w", execution.ErrRunAlreadyExists), http.StatusConflict},
		{protocol.InvalidRequestf("workflow id is empty"), http.StatusBadRequest},
		{fmt.Errorf("start: %w", protocol.InvalidRequestf("workflow id is empty")), http.StatusBadRequest},
		{execution.ErrRunOrWorkflowIDRequired, http.StatusBadRequest},
		{execution.ErrInvalidHistoryPageToken, http.StatusBadRequest},
		{protocol.ErrUpdateRejected, http.StatusBadRequest},
		{fmt.Errorf("%w: run run-1", execution.ErrWorkflowClosing), http.StatusServiceUnavailable},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
		{errors.New("boom"), http.StatusInternalServerError},
		{fmt.Errorf("dial tcp 10.0.0.5:3306: connection refused"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, tc.err, db.IsTransient); got != tc.want {
			t.Errorf("runtimeErrorStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
	// 正在关闭的 Run 没有写入请求：重发会再执行一次的 Signal 端点也是 503，SDK 重发。
	if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeSignalPath, execution.ErrWorkflowClosing, nil); got != http.StatusServiceUnavailable {
		t.Errorf("signal on a closing run: runtimeErrorStatus = %d, want 503", got)
	}
}

// 数据库与任务队列的瞬时失败：幂等端点 503（SDK 重试），重发会再执行一次的端点 502（SDK 不重发）；
// 非瞬时的数据库错误仍是 500。错误用驱动真实返回的类型构造。
func TestRuntimeErrorStatusClassifiesTransientFailures(t *testing.T) {
	transient := []error{
		&mysqldriver.MySQLError{Number: 1040, Message: "Too many connections"},
		fmt.Errorf("load run: %w", &mysqldriver.MySQLError{Number: 1040, Message: "Too many connections"}),
		&mysqldriver.MySQLError{Number: 1203, Message: "User already has more than 'max_user_connections' active connections"},
		&mysqldriver.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"},
		fmt.Errorf("commit: %w", &mysqldriver.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}),
		&mysqldriver.MySQLError{Number: 2006, Message: "MySQL server has gone away"},
		&mysqldriver.MySQLError{Number: 2013, Message: "Lost connection to MySQL server during query"},
		mysqldriver.ErrInvalidConn,
		fmt.Errorf("start: %w", mysqldriver.ErrInvalidConn),
		driver.ErrBadConn,
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		fmt.Errorf("query: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}),
		fmt.Errorf("frontend: dequeue owner %q partitions [1]: %w", "127.0.0.1:17601",
			errors.Join(grpcstatus.Error(codes.Unavailable, "partition 1: queue closed"), grpcstatus.Error(codes.Unavailable, "partition 2: queue closed"))),
	}
	for _, err := range transient {
		if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, err, db.IsTransient); got != http.StatusServiceUnavailable {
			t.Errorf("idempotent path: runtimeErrorStatus(%v) = %d, want 503", err, got)
		}
		if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeResetPath, err, db.IsTransient); got != http.StatusBadGateway {
			t.Errorf("reset: runtimeErrorStatus(%v) = %d, want 502", err, got)
		}
	}
	permanent := []error{
		&mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry 'x' for key 'PRIMARY'"},
		&mysqldriver.MySQLError{Number: 1146, Message: "Table 'tenuto.tenuto_runs' doesn't exist"},
		&mysqldriver.MySQLError{Number: 1406, Message: "Data too long for column 'input'"},
		grpcstatus.Error(codes.InvalidArgument, "stale ack token"),
		errors.New("Error 1040: Too many connections"),
	}
	for _, err := range permanent {
		if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, err, db.IsTransient); got != http.StatusInternalServerError {
			t.Errorf("runtimeErrorStatus(%v) = %d, want 500", err, got)
		}
	}
	// 没有传入分类函数（内存后端的测试装配）时数据库错误仍是 500；gRPC Unavailable 不依赖它。
	if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, transient[0], nil); got != http.StatusInternalServerError {
		t.Errorf("without classifier = %d, want 500", got)
	}
	if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, transient[len(transient)-1], nil); got != http.StatusServiceUnavailable {
		t.Errorf("gRPC Unavailable without classifier = %d, want 503", got)
	}
	// 调用方的期限仍是 504，即使它也满足 net.Error。
	if got := runtimeErrorStatus(runtimeapi.WorkflowRuntimeGetRunPath, fmt.Errorf("query: %w", context.DeadlineExceeded), db.IsTransient); got != http.StatusGatewayTimeout {
		t.Errorf("deadline = %d, want 504", got)
	}
}

func TestWriteRuntimeErrorSetsRetryAfterOnUnavailable(t *testing.T) {
	write := func(path string, err error) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, path, nil)
		c.Set(loggerKey, slog.New(slog.DiscardHandler))
		c.Set(transientKey, db.IsTransient)
		writeRuntimeError(c, err)
		return recorder
	}
	tooMany := &mysqldriver.MySQLError{Number: 1040, Message: "Too many connections"}
	response := write(runtimeapi.WorkflowRuntimeGetRunPath, fmt.Errorf("dial tcp 10.0.0.5:3306: %w", tooMany))
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != retryAfterSeconds {
		t.Fatalf("status = %d, Retry-After = %q", response.Code, response.Header().Get("Retry-After"))
	}
	// 应答体是固定文本，不带数据库地址。
	if body := response.Body.String(); body != "service temporarily unavailable\n" {
		t.Fatalf("body = %q", body)
	}
	response = write(runtimeapi.WorkflowRuntimeTriggerSchedulePath, tooMany)
	if response.Code != http.StatusBadGateway || response.Header().Get("Retry-After") != "" || strings.Contains(response.Body.String(), "1040") {
		t.Fatalf("trigger-schedule: status = %d, Retry-After = %q, body = %q", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
}

// Server 没有余量（Query 重放名额已满）：请求没有生效，任何端点都应答 503 与 Retry-After，SDK 退避重发。
func TestWriteRuntimeErrorAnswersResourceExhaustedWith503(t *testing.T) {
	err := fmt.Errorf("query replay concurrency limit 8 reached: %w", protocol.ErrResourceExhausted)
	for _, path := range []string{runtimeapi.WorkflowRuntimeQueryPath, runtimeapi.WorkflowRuntimeResetPath} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, path, nil)
		c.Set(loggerKey, slog.New(slog.DiscardHandler))
		writeRuntimeError(c, err)
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != retryAfterSeconds {
			t.Fatalf("%s: status = %d, Retry-After = %q", path, recorder.Code, recorder.Header().Get("Retry-After"))
		}
	}
}

// 请求在生效之前被取消（等 keyed lock 时 context 结束）应答 503 与 Retry-After，不应答空 body 的 200；
// 非幂等端点同样是 503，因为取消只发生在提交之前。
func TestWriteRuntimeErrorAnswersCancelledRequestsWith503(t *testing.T) {
	for _, path := range []string{runtimeapi.WorkflowRuntimeSignalPath, runtimeapi.WorkflowRuntimeResetPath} {
		for _, requestCancelled := range []bool{false, true} {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			requestCtx, cancel := context.WithCancel(context.Background())
			if requestCancelled {
				cancel()
			}
			c.Request = httptest.NewRequest(http.MethodPost, path, nil).WithContext(requestCtx)
			c.Set(loggerKey, slog.New(slog.DiscardHandler))
			writeRuntimeError(c, fmt.Errorf("lock workflow: %w", context.Canceled))
			cancel()
			if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != retryAfterSeconds || recorder.Body.Len() == 0 {
				t.Errorf("%s (request cancelled %v) = %d, Retry-After %q, body %q", path, requestCancelled,
					recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
			}
		}
	}
}

// 长轮询在请求方仍在等待时因停机结束（context.Canceled）应答 204。
func TestRuntimePollErrorMapsShutdownToNoContent(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, runtimeapi.WorkflowTaskRuntimePollPath, nil)
	if err := runtimePollError(c, context.Canceled); !errors.Is(err, errSkipResponse) {
		t.Fatalf("err = %v, want errSkipResponse", err)
	}
	c.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
}

// 目标不存在是 404，响应体是 err.Error() 的纯文本。
func TestRuntimeHTTPMapsNotFoundTo404(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	response := serveRuntimeHTTP(handler, http.MethodPost, runtimeapi.WorkflowRuntimeGetRunPath, `{"run_id":"missing"}`)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "text/plain; charset=utf-8" {
		t.Fatalf("content type = %q", contentType)
	}
	if !strings.Contains(response.Body.String(), execution.ErrRunNotFound.Error()) {
		t.Fatalf("body = %q", response.Body.String())
	}

}

// 每类失败经真实端点得到各自的状态码，4xx 的响应体保留错误文本。
func TestRuntimeHTTPStatusByFailureKind(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	cases := []struct {
		name, path, body string
		status           int
		text             string
	}{
		{"schedule not found", runtimeapi.WorkflowRuntimeGetSchedulePath, `{"schedule_id":"missing"}`, http.StatusNotFound, "schedule not found"},
		{"batch operation not found", runtimeapi.WorkflowRuntimeGetBatchPath, `{"operation_id":"missing"}`, http.StatusNotFound, "batch operation not found"},
		{"cancel missing batch operation", runtimeapi.WorkflowRuntimeCancelBatchPath, `{"operation_id":"missing"}`, http.StatusNotFound, "batch operation not found"},
		{"unknown batch operation type", runtimeapi.WorkflowRuntimeStartBatchPath, `{"req":{"type":"pause"}}`, http.StatusBadRequest, "unknown batch operation type"},
		{"batch reset without reset type", runtimeapi.WorkflowRuntimeStartBatchPath, `{"req":{"type":"reset"}}`, http.StatusBadRequest, "reset_type"},
		{"no current run for the workflow id", runtimeapi.WorkflowRuntimeSignalPath, `{"workflow_id":"missing","name":"approve"}`, http.StatusNotFound, "no current run"},
		{"invalid cron expression", runtimeapi.WorkflowRuntimeCreateSchedulePath, `{"workflow_type":"wf","cron_expression":"not a cron"}`, http.StatusBadRequest, "invalid cron_expression"},
		{"missing workflow type", runtimeapi.WorkflowRuntimeStartPath, `{"workflow_id":"wf-1"}`, http.StatusBadRequest, "workflow type is empty"},
		{"missing run and workflow id", runtimeapi.WorkflowRuntimeSignalPath, `{"name":"approve"}`, http.StatusBadRequest, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := serveRuntimeHTTP(handler, http.MethodPost, tc.path, tc.body)
			if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.text) {
				t.Fatalf("POST %s = %d %q, want %d containing %q", tc.path, response.Code, response.Body.String(), tc.status, tc.text)
			}
		})
	}
}

// 长轮询在服务端超时后应答 204 且没有 body。
func TestRuntimeHTTPPollTimeoutWritesNoContent(t *testing.T) {
	handler := newRuntimeHTTPRouteTestHandler(t)
	for _, path := range []string{runtimeapi.WorkflowTaskRuntimePollPath, runtimeapi.ActivityRuntimePollPath} {
		response := serveRuntimeHTTP(handler, http.MethodPost, path, `{"task_queue":"payments","timeout_millis":10}`)
		if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
			t.Errorf("POST %s = %d %q", path, response.Code, response.Body.String())
		}
	}
}

// 队列报告空轮询（taskq 的长轮询先于 HTTP 请求到期）时同样应答 204。
func TestRuntimePollErrorMapsQueuePollTimeoutToNoContent(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, runtimeapi.ActivityRuntimePollPath, nil)
	if err := runtimePollError(c, protocol.ErrTaskPollTimeout); !errors.Is(err, errSkipResponse) {
		t.Fatalf("err = %v, want errSkipResponse", err)
	}
	c.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
}
