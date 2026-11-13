package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
)

// 下列哨兵错误按 Server 应答的状态码分类，经 errors.Is 判断；具体应答是 *ServiceError。
var (
	// ErrInvalidArgument 表示请求本身有问题（HTTP 400）：缺字段、取值非法、Run 已结束不接受这个操作等。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrUnauthenticated 表示 Server 拒绝了请求的凭证（HTTP 401）：Options.AuthToken 缺失或与 Server 的
	// WORKFLOW_HTTP_AUTH_TOKEN 不符。重试不会改变结果，SDK 不重试它。
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrNotFound 表示寻址的 Run、Workflow、Schedule 或批量操作不存在（HTTP 404）。
	ErrNotFound = errors.New("not found")
	// ErrConflict 表示与已有记录或并发写入冲突（HTTP 409）。
	ErrConflict = errors.New("conflict")
	// ErrWorkflowExecutionAlreadyStarted 表示同一 Workflow ID 已有运行中的 Run，启动被拒绝。
	// 它同时满足 errors.Is(err, ErrConflict)。
	ErrWorkflowExecutionAlreadyStarted = errors.New("workflow execution already started")
	// ErrDeadlineExceeded 表示 Server 端的等待到期（HTTP 504）。
	ErrDeadlineExceeded = errors.New("server deadline exceeded")
	// ErrUnavailable 表示 Server 或它依赖的数据库、任务队列暂时不可用（HTTP 503 或 502），SDK 按 Options.Retry
	// 重试用尽后返回。502 出现在重发会再执行一次的调用（Reset、Schedule 的 Create 与 Trigger、批量操作的启动）上，
	// 请求可能已经生效，重发前先读状态确认。
	ErrUnavailable = errors.New("service unavailable")
	// ErrInternal 表示 Server 内部错误（HTTP 500），细节只记在 Server 日志里。
	ErrInternal = errors.New("internal server error")
	// ErrUpdateRejected 表示 Update 被 validator 拒绝：同步失败，不写历史、不 accept。
	// 与 Server 内部使用的是同一个值，错误同时可 errors.As 到携带拒绝原因的 *apperror.Error。
	ErrUpdateRejected = protocol.ErrUpdateRejected
)

// ServiceError 是 Server 返回的非 2xx 应答。errors.Is 按状态码匹配上面的哨兵错误。
type ServiceError struct {
	// StatusCode 是 HTTP 状态码。
	StatusCode int
	// Message 是 Server 给出的错误文本。
	Message string
	kind    error
}

// Error 返回状态码与 Server 的错误文本。
func (e *ServiceError) Error() string {
	return fmt.Sprintf("tenuto server: %d %s: %s", e.StatusCode, http.StatusText(e.StatusCode), e.Message)
}

// Is 让 errors.Is 按状态码分类匹配哨兵错误。
func (e *ServiceError) Is(target error) bool {
	if e.kind == nil {
		return false
	}
	if target == e.kind {
		return true
	}
	return e.kind == ErrWorkflowExecutionAlreadyStarted && target == ErrConflict
}

// WorkflowExecutionError 是 WorkflowRun.Get 在 Run 没有成功完成时返回的错误。Unwrap 得到原因：
//
//   - Workflow 返回错误：*apperror.Error，Type、NonRetryable、Details 与 Workflow 返回的应用错误相同；
//   - 执行超时或单次 Run 超时：*apperror.Error，TimeoutType 为 TimeoutTypeWorkflowExecution 或 TimeoutTypeWorkflowRun；
//   - 取消：取消错误，apperror.IsCanceled 与 workflow.IsCanceledError 为 true，Details 是 Workflow 给出的 details；
//   - 终止：*TerminatedError。
type WorkflowExecutionError struct {
	// WorkflowID 是 Run 的 Workflow ID。
	WorkflowID string
	// RunID 是以失败结束的 Run，即跟随 continue-as-new 与重试之后的最后一个 Run。
	RunID string
	// WorkflowType 是 Run 的 Workflow Type。
	WorkflowType string
	cause        error
}

// Error 返回执行标识与原因。
func (e *WorkflowExecutionError) Error() string {
	return fmt.Sprintf("workflow execution error (type: %s, workflowID: %s, runID: %s): %v", e.WorkflowType, e.WorkflowID, e.RunID, e.cause)
}

// Unwrap 返回原因，供 errors.As / errors.Is 使用。
func (e *WorkflowExecutionError) Unwrap() error { return e.cause }

// TerminatedError 是被 TerminateWorkflow（或批量终止、Reset）终止的 Run 的结束原因。
type TerminatedError struct {
	// Reason 是终止时给出的原因。
	Reason string
}

// Error 返回终止原因。
func (e *TerminatedError) Error() string {
	if e.Reason == "" {
		return "terminated"
	}
	return "terminated: " + e.Reason
}

// QueryFailedError 表示 Query 没有得到结果：查询类型没有注册 handler、Run 还没有完成第一个 Workflow Task、
// handler 阻塞或返回了拒绝。
type QueryFailedError struct {
	// QueryType 是查询类型。
	QueryType string
	// Message 是 Server 给出的原因。
	Message string
}

// Error 返回查询类型与原因。
func (e *QueryFailedError) Error() string {
	return fmt.Sprintf("query %s failed: %s", e.QueryType, e.Message)
}

// TimeoutType 的两个 Workflow 取值；Activity 的超时种类见 sdk/workflow。
const (
	// TimeoutTypeWorkflowExecution 是整条执行链（含 continue-as-new 与重试）的执行超时。
	TimeoutTypeWorkflowExecution = protocol.TimeoutTypeWorkflowExecution
	// TimeoutTypeWorkflowRun 是单个 Run 的超时。
	TimeoutTypeWorkflowRun = protocol.TimeoutTypeWorkflowRun
)

// callKind 区分调用：同一个状态码在不同调用上的含义不同。
type callKind int

const (
	callOther callKind = iota
	// callStart 是会新建 Run 的调用，409 表示 Workflow ID 已有运行中的 Run。
	callStart
	// callUpdate 是 update 与 update-with-start，400 + Failure JSON 是 validator 拒绝。
	callUpdate
)

// mapError 是 Server 应答到 SDK 错误的唯一映射。非 HTTP 状态错误（网络、ctx）原样返回。
func mapError(kind callKind, err error) error {
	var status *runtimeclient.StatusError
	if err == nil || !errors.As(err, &status) {
		return err
	}
	if kind == callUpdate && status.Code == http.StatusBadRequest {
		if failure, ok := decodeRejection(status.Body); ok {
			return fmt.Errorf("%w: %w", ErrUpdateRejected, apperror.FromFailure(failure, decodePayload))
		}
	}
	serviceErr := &ServiceError{StatusCode: status.Code, Message: status.Body}
	switch status.Code {
	case http.StatusBadRequest:
		serviceErr.kind = ErrInvalidArgument
	case http.StatusUnauthorized:
		serviceErr.kind = ErrUnauthenticated
	case http.StatusNotFound:
		serviceErr.kind = ErrNotFound
	case http.StatusConflict:
		serviceErr.kind = ErrConflict
		if kind == callStart {
			serviceErr.kind = ErrWorkflowExecutionAlreadyStarted
		}
	case http.StatusGatewayTimeout:
		serviceErr.kind = ErrDeadlineExceeded
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		serviceErr.kind = ErrUnavailable
	case http.StatusInternalServerError:
		serviceErr.kind = ErrInternal
	}
	return serviceErr
}

// decodeRejection 解析 validator 拒绝的应答体：一个 protocol.Failure JSON。其它 400 的应答体是纯文本。
func decodeRejection(body string) (protocol.Failure, bool) {
	var failure protocol.Failure
	if err := json.Unmarshal([]byte(body), &failure); err != nil {
		return protocol.Failure{}, false
	}
	return failure, true
}
