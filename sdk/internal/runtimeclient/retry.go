package runtimeclient

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// RetryPolicy 是 runtime HTTP 调用遇到瞬时错误时的重试策略，对应 Temporal Go SDK 的 gRPC 重试拦截器：
// 指数退避加 ±20% 抖动，ctx 有 deadline 时重试到 deadline，没有时从第一次失败起重试 MaximumElapsed。
// 零值字段取缺省值。
type RetryPolicy struct {
	// Disabled 关闭重试：每个调用只发一次。
	Disabled bool
	// InitialInterval 是第一次重试之前的等待，缺省 200ms。
	InitialInterval time.Duration
	// MaximumInterval 是两次尝试之间等待的上限，缺省 10s。
	MaximumInterval time.Duration
	// BackoffCoefficient 是每次重试等待的倍数，缺省 2。
	BackoffCoefficient float64
	// MaximumElapsed 是 ctx 没有 deadline 时从第一次失败起继续重试的时长，缺省 60s。
	MaximumElapsed time.Duration
}

const (
	defaultRetryInitialInterval = 200 * time.Millisecond
	defaultRetryMaximumInterval = 10 * time.Second
	defaultRetryCoefficient     = 2
	defaultRetryMaximumElapsed  = time.Minute
	retryJitter                 = 0.2
)

func (p RetryPolicy) normalized() RetryPolicy {
	if p.InitialInterval <= 0 {
		p.InitialInterval = defaultRetryInitialInterval
	}
	if p.MaximumInterval <= 0 {
		p.MaximumInterval = defaultRetryMaximumInterval
	}
	p.MaximumInterval = max(p.MaximumInterval, p.InitialInterval)
	if p.BackoffCoefficient < 1 {
		p.BackoffCoefficient = defaultRetryCoefficient
	}
	if p.MaximumElapsed <= 0 {
		p.MaximumElapsed = defaultRetryMaximumElapsed
	}
	return p
}

// retryMode 是一个调用能重试哪些失败。
type retryMode int

const (
	// retryIdempotent：重发不会重复生效的调用（读取、长轮询、按 ID 或 request ID 去重的写入、task token 定界的
	// Worker 调用）。连接失败与中断、429、502、503、504 都重试。
	retryIdempotent retryMode = iota
	// retryUnsent：重发会重复生效的调用。只在请求确定没有被处理时重试：连接没有建立、429、503。
	retryUnsent
)

// transportError 是请求没有得到完整应答的失败：连接失败、连接中断、读应答体失败。
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// unsent 报告请求确定没有到达 Server：连接（含 DNS 解析）没有建立。
func (e *transportError) unsent() bool {
	var opErr *net.OpError
	return errors.As(e.err, &opErr) && opErr.Op == "dial"
}

// tlsRejected 报告 TLS 握手被拒：本地不信任 Server 证书，或 Server 以 TLS alert 拒绝了连接（没有出示或出示了
// 不受信任的客户端证书）。这是配置错误，重试不会改变结果。
func (e *transportError) tlsRejected() bool {
	var verifyErr *tls.CertificateVerificationError
	var alertErr tls.AlertError
	if errors.As(e.err, &verifyErr) || errors.As(e.err, &alertErr) {
		return true
	}
	// TCP 上对端发来的 alert 包成 Op 为 "remote error" 的 *net.OpError；它可能在 url.Error 等多层包装之内。
	for err := e.err; err != nil; err = errors.Unwrap(err) {
		if opErr, ok := err.(*net.OpError); ok && opErr.Op == "remote error" {
			return true
		}
	}
	return false
}

// connection 报告失败来自网络连接，而不是调用方自己的 RoundTripper（鉴权、代理配置等）返回的错误。
func (e *transportError) connection() bool {
	err := e.err
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) ||
		strings.Contains(err.Error(), "server closed idle connection")
}

func retryable(err error, mode retryMode) bool {
	var status *StatusError
	if errors.As(err, &status) {
		switch status.Code {
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return true
		case http.StatusBadGateway, http.StatusGatewayTimeout:
			return mode == retryIdempotent
		}
		return false
	}
	var transport *transportError
	if !errors.As(err, &transport) || transport.tlsRejected() {
		return false
	}
	if transport.unsent() {
		return true
	}
	return mode == retryIdempotent && transport.connection()
}

// do 执行 attempt，按 mode 与策略重试瞬时错误，返回最后一次的结果。ctx 在等待重试时结束则返回 ctx 的错误；
// 下一次重试会越过 ctx 的 deadline（或没有 deadline 时越过 MaximumElapsed）时不再等待，返回这次的错误。
func (p RetryPolicy) do(ctx context.Context, mode retryMode, attempt func() error) error {
	interval := p.InitialInterval
	var giveUpAt time.Time
	for {
		err := attempt()
		if err == nil || p.Disabled || ctx.Err() != nil || !retryable(err, mode) {
			return err
		}
		now := time.Now()
		if giveUpAt.IsZero() {
			giveUpAt = now.Add(p.MaximumElapsed)
			if deadline, ok := ctx.Deadline(); ok {
				giveUpAt = deadline
			}
		}
		delay := time.Duration(float64(interval) * (1 + retryJitter*(2*rand.Float64()-1)))
		if now.Add(delay).After(giveUpAt) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		interval = min(time.Duration(float64(interval)*p.BackoffCoefficient), p.MaximumInterval)
	}
}
