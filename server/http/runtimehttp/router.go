package runtimehttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:gogen: plugin:swaggen -output api_generated.go

func init() {
	// 本包的路由只通过 gin.New() 组装；Release 模式关闭 gin 的调试输出。
	gin.SetMode(gin.ReleaseMode)
}

// HandlerOption 配置 NewRuntimeHTTPHandler。
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	transient func(error) bool
}

// WithTransientErrors 设置识别依赖（数据库、任务队列）瞬时失败的函数，由组合根传入基础设施的分类（MySQL 的
// db.IsTransient 与 TaskQ 重试用完的 taskq.IsTransient）。命中的错误按 runtimeErrorStatus 应答 503 或 502；不设置时
// 这些错误是 500。
func WithTransientErrors(transient func(error) bool) HandlerOption {
	return func(options *handlerOptions) { options.transient = transient }
}

// NewRuntimeHTTPHandler keeps the external, workflow-worker, and activity-worker
// transports on one route surface so every host exposes the same runtime contract.
// Every route signs and verifies task tokens with the server's codec.
func NewRuntimeHTTPHandler(server *engine.Server, options ...HandlerOption) http.Handler {
	var resolved handlerOptions
	for _, option := range options {
		option(&resolved)
	}
	codec := server.TaskTokens()
	router := newRuntimeRouter(server.Logger(), resolved.transient)
	bindActivityRuntimeAPI(router, engine.NewActivityRuntimeService(server), codec)
	bindWorkflowRuntimeAPI(router, server)
	bindWorkflowTaskRuntimeAPI(router, server, codec)
	return router
}

// newRuntimeRouter 组装不带请求日志的 gin 引擎，并让未命中路由与非 POST 请求
// 沿用 net/http 的纯文本应答。服务端故障记入 logger，transient 识别依赖的瞬时失败（可为 nil）。
// POST 必须声明 JSON 请求体（requireJSONPost）。
func newRuntimeRouter(logger *slog.Logger, transient func(error) bool) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery(), func(c *gin.Context) {
		c.Set(loggerKey, logger)
		c.Set(transientKey, transient)
	}, requireJSONPost)
	router.HandleMethodNotAllowed = true
	router.RedirectTrailingSlash = false
	router.NoMethod(func(c *gin.Context) {
		http.Error(c.Writer, "method is not allowed", http.StatusMethodNotAllowed)
	})
	router.NoRoute(func(c *gin.Context) {
		http.NotFound(c.Writer, c.Request)
	})
	return router
}

// loggerKey 是请求上下文里日志记录器的键，writeRuntimeError 用它记录服务端故障。
const loggerKey = "tenuto.logger"

// transientKey 是请求上下文里瞬时失败分类函数的键，见 WithTransientErrors。
const transientKey = "tenuto.transient-errors"

// errSkipResponse 表示端点已经自行写完应答（长轮询的 204、update 拒绝的 Failure JSON），
// onGinResponse 不再写入。
var errSkipResponse = errors.New("skip response")

// abortText 写入纯文本应答并返回 errSkipResponse，供需要特殊状态码的端点使用。
func abortText(c *gin.Context, status int, text string) error {
	http.Error(c.Writer, text, status)
	c.Abort()
	return errSkipResponse
}

// maxRuntimeRequestBody 限制请求体大小，防止大 body 造成 OOM。
const maxRuntimeRequestBody = 4 * 1024 * 1024 // 4MB

// onGinBind 由生成的路由包装调用：解码 JSON 请求体，失败时应答 400。
//
// 用户载荷（start / signal / query / update / schedule 的 input、Activity 的输出与心跳详情、Workflow 命令的
// 输入输出）里的数字先解码成 json.Number，再归一化成引擎的数值表示：整数是 int64（超过 int64 的是 uint64），
// 带小数的是 float64。超过 2^53 的整数因此原样保留。
func onGinBind(c *gin.Context, val any, _ string) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRuntimeRequestBody)
	if err := jsonvalue.Decode(c.Request.Body, val); err != nil {
		http.Error(c.Writer, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
		return false
	}
	return true
}

// requireJSONPost 对 Content-Type 不是 application/json 的 POST 应答 415，包括没有请求体的端点。
func requireJSONPost(c *gin.Context) {
	if c.Request.Method == http.MethodPost && !IsJSONRequest(c.Request) {
		http.Error(c.Writer, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		c.Abort()
	}
}

// IsJSONRequest 报告请求是否声明 Content-Type: application/json（可带 charset 等参数）。
//
// Runtime API 与 /admin 只接受这种请求体：浏览器不经 CORS 预检就能跨站发出的 POST 只有
// text/plain、application/x-www-form-urlencoded 与 multipart/form-data 三种，Server 不应答预检，
// 所以第三方网页无法借运维人员的浏览器（它在受信网络里）调用这些端点。
func IsJSONRequest(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mediaType == "application/json"
}

// onGinResponse 由生成的路由包装调用：成功写 JSON，失败写纯文本错误。
func onGinResponse[T any](c *gin.Context, data T, err error) {
	switch {
	case errors.Is(err, errSkipResponse):
	case err != nil:
		writeRuntimeError(c, err)
	default:
		c.Header("Content-Type", "application/json")
		_ = json.NewEncoder(c.Writer).Encode(data)
	}
}

// writeRuntimeError 写出失败应答。4xx 与 504 的响应体是错误文本；服务端故障（5xx）只返回固定文本，
// 真实错误记入日志。
//
// context.Canceled 一律应答 503 与 Retry-After：引擎只在提交之前响应取消（等 keyed lock、读 Run），提交之后的
// 动作不随请求取消，所以这个错误表示请求没有生效。请求方已经断开时这次写入无害。
func writeRuntimeError(c *gin.Context, err error) {
	logger := c.MustGet(loggerKey).(*slog.Logger)
	transientValue, _ := c.Get(transientKey)
	transient, _ := transientValue.(func(error) bool)
	if errors.Is(err, context.Canceled) {
		logger.Info("runtime http request cancelled before taking effect", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
		c.Header("Retry-After", retryAfterSeconds)
		http.Error(c.Writer, "request cancelled before taking effect; retry", http.StatusServiceUnavailable)
		return
	}
	switch code := runtimeErrorStatus(c.Request.URL.Path, err, transient); code {
	case http.StatusServiceUnavailable:
		logger.Warn("runtime http request failed: dependency temporarily unavailable", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
		c.Header("Retry-After", retryAfterSeconds)
		http.Error(c.Writer, "service temporarily unavailable", code)
	case http.StatusBadGateway:
		logger.Warn("runtime http request failed: dependency temporarily unavailable", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
		http.Error(c.Writer, "service temporarily unavailable; the request may have taken effect", code)
	case http.StatusInternalServerError:
		logger.Error("runtime http request failed", "method", c.Request.Method, "path", c.Request.URL.Path, "err", err)
		http.Error(c.Writer, "internal server error", code)
	default:
		http.Error(c.Writer, err.Error(), code)
	}
}

// retryAfterSeconds 是 503 应答的 Retry-After：数据库连接数打满、锁等待与连接重建通常在秒级恢复。
const retryAfterSeconds = "1"

// runtimeErrorStatus 是错误到 HTTP 状态码的唯一映射：
// 目标不存在为 404，与已有记录或并发写入冲突为 409，请求本身的问题为 400，等待超时为 504，Server 没有余量处理
// （protocol.ErrResourceExhausted）为 503；
// 数据库或任务队列暂时不可用（transient 命中、gRPC Unavailable）在幂等端点上为 503，在重发会再执行一次的端点
// （runtimeapi.IsIdempotent 为 false）上为 502：这类失败不说明请求有没有部分生效，503 会让客户端重发；
// 其余错误属于服务端故障，为 500。transient 可为 nil。
func runtimeErrorStatus(path string, err error, transient func(error) bool) int {
	switch {
	case isAny(err, execution.ErrRunNotFound, execution.ErrNoCurrentRun, batch.ErrNotFound,
		definition.ErrWorkflowConfigNotFound, schedule.ErrNotFound, protocol.ErrActivityWorkerVersionNotRegistered):
		return http.StatusNotFound
	case isAny(err, execution.ErrCurrentRunConflict, execution.ErrRunAlreadyExists, schedule.ErrAlreadyExists, schedule.ErrConcurrentUpdate,
		schedule.ErrOverlapSkipped, batch.ErrAlreadyExists, batch.ErrConcurrentUpdate,
		batch.ErrInvalidTransition):
		return http.StatusConflict
	case isAny(err, protocol.ErrInvalidRequest, protocol.ErrUpdateRejected, execution.ErrRunOrWorkflowIDRequired,
		execution.ErrInvalidHistoryPageToken, execution.ErrExpiredHistoryPageToken, dispatch.ErrActivityRouteUnavailable):
		return http.StatusBadRequest
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, protocol.ErrResourceExhausted):
		// 请求没有生效，任何端点上都可以重发。
		return http.StatusServiceUnavailable
	case (transient != nil && transient(err)) || grpcUnavailable(err):
		if runtimeapi.IsIdempotent(path) {
			return http.StatusServiceUnavailable
		}
		return http.StatusBadGateway
	}
	return http.StatusInternalServerError
}

// grpcUnavailable 报告错误链上有 gRPC Unavailable：任务队列的 matching 分区正在换主或连接不上。
func grpcUnavailable(err error) bool {
	var grpcErr interface{ GRPCStatus() *status.Status }
	return errors.As(err, &grpcErr) && grpcErr.GRPCStatus().Code() == codes.Unavailable
}

func isAny(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// runtimeOK 把只返回 error 的调用适配成 {"ok":true} 应答。
func runtimeOK(err error) (runtimeapi.RuntimeOKResponse, error) {
	return runtimeapi.RuntimeOKResponse{OK: true}, err
}

func runtimePollContext(parent context.Context, timeoutMillis int64) (context.Context, context.CancelFunc) {
	if timeoutMillis <= 0 {
		return context.WithTimeout(parent, runtimeapi.DefaultRuntimePollTimeout)
	}
	if timeoutMillis > runtimeapi.MaxRuntimePollTimeout.Milliseconds() {
		timeoutMillis = runtimeapi.MaxRuntimePollTimeout.Milliseconds()
	}
	return context.WithTimeout(parent, time.Duration(timeoutMillis)*time.Millisecond)
}

// runtimePollError 落实长轮询的 wire 语义：服务端超时与停机（Server.StopLongPolls）应答 204 且无 body，
// 客户端已取消时不写应答，其余错误交给统一映射。
func runtimePollError(c *gin.Context, err error) error {
	if errors.Is(err, protocol.ErrTaskPollTimeout) || longPollEnded(c, err) {
		c.Status(http.StatusNoContent)
		return errSkipResponse
	}
	if errors.Is(err, context.Canceled) && c.Request.Context().Err() != nil {
		return errSkipResponse
	}
	return err
}

// longPollEnded 报告长轮询因服务端的期限或停机结束，而请求方仍在等待。
func longPollEnded(c *gin.Context, err error) bool {
	return c.Request.Context().Err() == nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
}

// signRuntimeTaskLease 把绑定队列租约的 task token 写入租约的任务。
func signRuntimeTaskLease(codec *dispatch.TaskTokenCodec, lease protocol.TaskLease) (protocol.TaskLease, error) {
	token, err := codec.EncodeLease(lease.Task, lease.Token)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	lease.Task.TaskToken = token
	return lease, nil
}

func verifyRuntimeTaskLease(codec *dispatch.TaskTokenCodec, lease protocol.TaskLease, expectedType protocol.TaskType) (protocol.TaskLease, error) {
	token := lease.Task.TaskToken
	if token == "" {
		return protocol.TaskLease{}, protocol.InvalidRequestf("task lease token is empty")
	}
	task, err := codec.DecodeLease(token, lease.Token)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	if task.Type != expectedType {
		return protocol.TaskLease{}, protocol.InvalidRequestf("task token type %s is invalid for %s lease", task.Type, expectedType)
	}
	task.TaskToken = token
	lease.Task = task
	return lease, nil
}
