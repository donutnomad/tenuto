package executor

import (
	"context"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

const (
	// defaultHeartbeatThrottle 是没有 HeartbeatTimeout 的 Activity 两次心跳 RPC 的最小间隔。
	defaultHeartbeatThrottle = 30 * time.Second
	// maxHeartbeatThrottle 是两次心跳 RPC 最小间隔的上限。
	maxHeartbeatThrottle = time.Minute
)

// heartbeatThrottle 是两次心跳 RPC 的最小间隔：HeartbeatTimeout 的 80%，没有 HeartbeatTimeout 时 30s，上限 60s。
func heartbeatThrottle(heartbeatTimeout time.Duration) time.Duration {
	if heartbeatTimeout <= 0 {
		return defaultHeartbeatThrottle
	}
	return min(heartbeatTimeout*4/5, maxHeartbeatThrottle)
}

// heartbeater 合并一个 attempt 的心跳。窗口外的心跳在调用方的 goroutine 里同步发出并开启一个节流窗口；
// 窗口内的心跳只保留最新的 details，窗口结束时发出并开启下一个窗口。心跳应答带回取消请求时取消 Activity
// context；设置了 HeartbeatTimeout 且超过它仍没有一次心跳送达时，Server 已判这次 attempt 超时，
// 以 errActivityAbandoned 取消 Activity context。
type heartbeater struct {
	runtime          ActivityRuntime
	task             protocol.Task
	ctx              context.Context
	cancel           context.CancelCauseFunc
	interval         time.Duration
	heartbeatTimeout time.Duration
	// payloadLimit 是 Server 在开始结果里告诉的载荷上限，0 表示不检查。
	payloadLimit int

	mu         sync.Mutex
	throttled  bool
	pending    map[string]any
	hasPending bool
	stopped    bool
	timer      *time.Timer
	lastOK     time.Time
	sending    sync.WaitGroup
}

func newHeartbeater(ctx context.Context, cancel context.CancelCauseFunc, runtime ActivityRuntime, task protocol.Task, payloadLimit int) *heartbeater {
	return &heartbeater{
		runtime: runtime, task: task, ctx: ctx, cancel: cancel,
		interval: heartbeatThrottle(task.HeartbeatTimeout), heartbeatTimeout: task.HeartbeatTimeout,
		payloadLimit: payloadLimit, lastOK: time.Now(),
	}
}

// record 是 activity.RecordHeartbeat 的实现。
func (h *heartbeater) record(details map[string]any) {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return
	}
	if h.throttled {
		h.pending, h.hasPending = details, true
		h.mu.Unlock()
		return
	}
	h.throttled = true
	h.mu.Unlock()
	h.send(h.ctx, details)
	h.startWindow()
}

func (h *heartbeater) startWindow() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.stopped {
		h.timer = time.AfterFunc(h.interval, h.windowEnd)
	}
}

// windowEnd 发出窗口内最新的 details；没有时结束节流，下一次心跳立即发出。
func (h *heartbeater) windowEnd() {
	h.mu.Lock()
	if h.stopped || !h.hasPending {
		h.throttled = false
		h.mu.Unlock()
		return
	}
	details := h.pending
	h.pending, h.hasPending = nil, false
	h.sending.Add(1)
	h.mu.Unlock()
	h.send(h.ctx, details)
	h.sending.Done()
	h.startWindow()
}

// send 发出一次心跳，等待不超过节流间隔：超过间隔仍未送达的心跳由窗口内更新的一次接替。
// details 超过载荷上限时不发出心跳：上报一次不可重试的 PayloadSizeLimitExceeded 失败，结束这次 attempt，
// 以 errActivityAbandoned 取消 Activity context（Temporal 的 Server 对超限心跳做同样的处理）。
func (h *heartbeater) send(ctx context.Context, details map[string]any) {
	if limitErr := protocol.CheckPayloadSize("activity heartbeat details", details, h.payloadLimit); limitErr != nil {
		reportCtx, cancelReport := reportContext(ctx, time.Time{})
		defer cancelReport()
		_ = h.runtime.FailActivityTask(reportCtx, h.task, protocol.PayloadSizeLimitFailure(limitErr))
		h.cancel(errActivityAbandoned)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, h.interval)
	defer cancel()
	result, err := h.runtime.RecordActivityHeartbeat(ctx, h.task, details)
	if err != nil {
		h.mu.Lock()
		expired := h.heartbeatTimeout > 0 && time.Since(h.lastOK) >= h.heartbeatTimeout
		h.mu.Unlock()
		if expired {
			h.cancel(errActivityAbandoned)
		}
		return
	}
	h.mu.Lock()
	h.lastOK = time.Now()
	h.mu.Unlock()
	if !result.CancelRequested {
		return
	}
	if result.Reason == protocol.ActivityCancelRequestedReason {
		h.cancel(errActivityCancelRequested)
		return
	}
	h.cancel(errActivityAbandoned)
}

// stop 结束节流并等待窗口结束时正在发出的心跳，返回窗口内尚未发出的最新 details。
func (h *heartbeater) stop() (details map[string]any, pending bool) {
	h.mu.Lock()
	h.stopped = true
	if h.timer != nil {
		h.timer.Stop()
	}
	details, pending = h.pending, h.hasPending
	h.pending, h.hasPending = nil, false
	h.mu.Unlock()
	h.sending.Wait()
	return details, pending
}

// flush 在 Activity 以失败、取消或交还结束时发出窗口内尚未发出的最新 details：下一次 attempt 从它们恢复进度。
func (h *heartbeater) flush(ctx context.Context) {
	if details, pending := h.stop(); pending {
		h.send(ctx, details)
	}
}
