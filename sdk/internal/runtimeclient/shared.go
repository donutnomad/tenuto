package runtimeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// RuntimeHTTPClientOptions 配置 runtime HTTP 客户端：长轮询时长、遥测与瞬时错误的重试。
type RuntimeHTTPClientOptions struct {
	PollTimeout time.Duration
	Telemetry   tenutotel.Providers
	Retry       RetryPolicy
}

func normalizeRuntimeHTTPClientOptions(options RuntimeHTTPClientOptions) RuntimeHTTPClientOptions {
	options.Retry = options.Retry.normalized()
	if options.PollTimeout <= 0 {
		options.PollTimeout = runtimeapi.DefaultRuntimePollTimeout
	}
	if options.PollTimeout > runtimeapi.MaxRuntimePollTimeout {
		options.PollTimeout = runtimeapi.MaxRuntimePollTimeout
	}
	return options
}

func runtimeHTTPClient(client *http.Client, providers tenutotel.Providers) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	if providers.TracerProvider == nil && providers.MeterProvider == nil && providers.Propagator == nil {
		return client
	}
	providers = tenutotel.Resolve(providers)
	cloned := *client
	transport := cloned.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	cloned.Transport = otelhttp.NewTransport(
		transport,
		otelhttp.WithTracerProvider(providers.TracerProvider),
		otelhttp.WithMeterProvider(providers.MeterProvider),
		otelhttp.WithPropagators(providers.Propagator),
	)
	return &cloned
}

// StatusError 是非 2xx 应答：保留状态码与响应体（4xx 与 504 是错误文本，update 拒绝是 Failure JSON），
// 由 sdk/client 映射成 SDK 的错误类型。
type StatusError struct {
	API    string
	Status string
	Code   int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.API, e.Status, e.Body)
}

// Is 让由请求本身引起、重发也不会成功的应答（400、404、409、413、422）满足 errors.Is(err, protocol.ErrInvalidRequest)，
// 与进程内调用的错误一致：Worker 据此不立即退回这份投递。413 另外满足 protocol.ErrPayloadTooLarge：Workflow Task
// 的提交因此记为 payload_size_limit_exceeded 失败。
func (e *StatusError) Is(target error) bool {
	if target == protocol.ErrPayloadTooLarge {
		return e.Code == http.StatusRequestEntityTooLarge
	}
	if target != protocol.ErrInvalidRequest {
		return false
	}
	switch e.Code {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

// workerInfoHeaders 把 ctx 携带的 Worker 信息写进请求头；ctx 没有 Worker 信息时返回 nil。
func workerInfoHeaders(ctx context.Context) func(*http.Request) {
	info, ok := protocol.WorkerInfoFrom(ctx)
	if !ok {
		return nil
	}
	return func(httpReq *http.Request) {
		protocol.ApplyWorkerInfoHeaders(httpReq, info)
	}
}

// runtimePoll 发起长轮询，瞬时错误按 retry 重试（长轮询可以重发：连接断开时 Server 交还已领取的任务）。
// ctx 在应答到达之前取消（Worker 停止）时中止请求：Server 看到连接断开，把这次 poll
// 已领取的任务立即交还。应答的首个字节到达之后任务已经交出，ctx 取消不再中止读取，lease 照常返回给 Worker 处理。
func runtimePoll(ctx context.Context, retry RetryPolicy, client *http.Client, baseURL, path, apiName string, applyHeaders func(*http.Request), req, resp any) error {
	return retry.do(ctx, retryIdempotent, func() error {
		return runtimePollOnce(ctx, client, baseURL, path, apiName, applyHeaders, req, resp)
	})
}

func runtimePollOnce(ctx context.Context, client *http.Client, baseURL, path, apiName string, applyHeaders func(*http.Request), req, resp any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requestCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()
	stopAbort := context.AfterFunc(ctx, abort)
	defer stopAbort()
	requestCtx = httptrace.WithClientTrace(requestCtx, &httptrace.ClientTrace{
		GotFirstResponseByte: func() { stopAbort() },
	})
	err := runtimePost(requestCtx, client, baseURL, path, apiName, applyHeaders, req, resp)
	if err != nil && ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return ctx.Err()
	}
	return err
}

// runtimePostRetrying 是全部 runtime 调用的唯一出口：按 mode 与 retry 重试 runtimePost 的瞬时错误。
func runtimePostRetrying(ctx context.Context, retry RetryPolicy, mode retryMode, client *http.Client, baseURL, path, apiName string, applyHeaders func(*http.Request), req, resp any) error {
	return retry.do(ctx, mode, func() error {
		return runtimePost(ctx, client, baseURL, path, apiName, applyHeaders, req, resp)
	})
}

// runtimePost 发一次 JSON POST。apiName 前缀非 2xx 的错误文本；applyHeaders 可以为 nil，非 nil 时写入额外的请求头
// （Worker 信息）。连接失败、连接中断、读应答体失败与 2xx 空 body 包成 *transportError，供重试判定。
func runtimePost(ctx context.Context, client *http.Client, baseURL, path, apiName string, applyHeaders func(*http.Request), req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if applyHeaders != nil {
		applyHeaders(httpReq)
	}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		return &transportError{err: err}
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode == http.StatusNoContent {
		return protocol.ErrTaskPollTimeout
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(httpResp.Body, 8192))
		return &StatusError{
			API:    apiName,
			Status: httpResp.Status,
			Code:   httpResp.StatusCode,
			Body:   strings.TrimSpace(string(data)),
		}
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		return &transportError{err: err}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		// 每个成功应答都带 JSON body（204 已在上面处理）；空 body 的 2xx 说明应答没有完整送达或 Server 没有处理请求，
		// 不能当作成功。
		return &transportError{err: fmt.Errorf("%s: HTTP %d with empty body: %w", apiName, httpResp.StatusCode, io.ErrUnexpectedEOF)}
	}
	if resp == nil {
		return nil
	}
	// 应答里的用户载荷（Activity 输入、心跳详情、Query 结果、Update 结果、History）保留数字的精确值。
	return jsonvalue.Decode(bytes.NewReader(data), resp)
}
