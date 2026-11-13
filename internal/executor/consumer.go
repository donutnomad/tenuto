package executor

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// 轮询失败后的等待从 pollRetryInitialDelay 起翻倍到 pollRetryMaxDelay，带 ±20% 抖动。远程 Worker 的传输层已经按
// client.Options.Retry 重试过瞬时错误，这里是它放弃之后（或进程内 Worker）的第二层退避，避免 Server 不可用时空转。
const (
	pollRetryInitialDelay = 100 * time.Millisecond
	pollRetryMaxDelay     = 10 * time.Second
)

// Consume polls one task stream until pollCtx ends and runs each leased task
// under handleCtx. pollCtx carries worker routing metadata; cancelling it stops
// polling (an in-flight long poll returns promptly) while the task being handled
// keeps running until it finishes or handleCtx is cancelled. A lease returned by
// a poll that raced with the cancellation is still handled. Poll errors other than
// cancellation and poll timeout are retried with exponential backoff: the first
// failure of a run of failures is logged at WARN, the rest at DEBUG, and the first
// successful poll afterwards at INFO.
func Consume(
	pollCtx context.Context,
	handleCtx context.Context,
	logger *slog.Logger,
	taskKind string,
	poll func(context.Context) (protocol.TaskLease, error),
	handle func(context.Context, protocol.TaskLease),
) {
	backoff := pollBackoff{logger: logger, taskKind: taskKind}
	for pollCtx.Err() == nil {
		lease, err := poll(pollCtx)
		if errors.Is(err, protocol.ErrTaskPollTimeout) {
			backoff.recovered()
			continue
		}
		if err != nil {
			if !backoff.wait(pollCtx, err) {
				return
			}
			continue
		}
		backoff.recovered()
		handle(handleCtx, lease)
	}
}

// pollBackoff 是一个轮询循环连续失败的状态。
type pollBackoff struct {
	logger   *slog.Logger
	taskKind string
	failures int
}

// wait 记录一次失败并等待退避；ctx 结束时返回 false。
func (b *pollBackoff) wait(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	b.failures++
	delay := min(pollRetryInitialDelay<<min(b.failures-1, 16), pollRetryMaxDelay)
	delay = time.Duration(float64(delay) * (0.8 + 0.4*rand.Float64()))
	if b.failures == 1 {
		b.logger.Warn("poll failed; retrying with backoff", "task_kind", b.taskKind, "err", err)
	} else {
		b.logger.Debug("poll failed again", "task_kind", b.taskKind, "failures", b.failures, "next_retry", delay, "err", err)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// recovered 在一次成功的轮询（含长轮询到期）之后结束连续失败。
func (b *pollBackoff) recovered() {
	if b.failures > 0 {
		b.logger.Info("poll recovered", "task_kind", b.taskKind, "failures", b.failures)
		b.failures = 0
	}
}
