package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

func WorkerVersionHeartbeatInterval(interval, ttl time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}
	if ttl > 0 {
		return ttl / 2
	}
	return protocol.DefaultActivityWorkerVersionTTL / 2
}

// RunActivityWorkerVersionRegistry 登记 Worker 的 Activity 路由，之后每个 interval 心跳续期，直到 ctx 结束。
// 登记成功（或 ctx 先结束）时调用一次 ready。心跳得到 protocol.ErrActivityWorkerVersionNotRegistered 时（登记在
// 长时间失联后已被 Server 清理）立即重新登记：Worker 是否在线只由它自己的持续上报决定。
func RunActivityWorkerVersionRegistry(ctx context.Context, logger *slog.Logger, worker protocol.ActivityWorkerVersion, registry ActivityWorkerVersionRegistryAPI, interval time.Duration, ready func(error)) {
	if !registerActivityWorkerVersion(ctx, logger, worker, registry, interval) {
		ready(ctx.Err())
		return
	}
	ready(nil)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := registry.HeartbeatActivityWorkerVersionInNamespace(ctx, protocol.NormalizeNamespace(worker.Namespace), worker.Identity, worker.BuildID)
			if err == nil {
				continue
			}
			if !errors.Is(err, protocol.ErrActivityWorkerVersionNotRegistered) {
				logger.Error("heartbeat activity worker version failed", "err", err)
				continue
			}
			logger.Warn("activity worker version is no longer registered; registering again", "err", err)
			if !registerActivityWorkerVersion(ctx, logger, worker, registry, interval) {
				return
			}
		}
	}
}

// registerActivityWorkerVersion 登记 worker，失败时按 registerRetryBase 起步、每次加倍、不超过 interval 的退避重试，
// 直到成功（返回 true）或 ctx 结束（返回 false）。
func registerActivityWorkerVersion(ctx context.Context, logger *slog.Logger, worker protocol.ActivityWorkerVersion, registry ActivityWorkerVersionRegistryAPI, interval time.Duration) bool {
	delay := min(registerRetryBase, interval)
	for {
		err := registry.RegisterActivityWorkerVersion(ctx, worker)
		if err == nil {
			return true
		}
		logger.Error("register activity worker version failed", "err", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		delay = min(2*delay, interval)
	}
}

// registerRetryBase 是登记失败后第一次重试前的等待。
const registerRetryBase = time.Second

// NewActivityWorkerVersion 从 Worker 配置和 Activity 注册表生成版本声明，契约取自注册表。
func NewActivityWorkerVersion(opts WorkerOptions, activities *activity.Registry) protocol.ActivityWorkerVersion {
	worker := protocol.ActivityWorkerVersion{
		Namespace:     protocol.NormalizeNamespace(opts.Namespace),
		Identity:      opts.Identity,
		BuildID:       opts.BuildID,
		TaskQueues:    append([]string(nil), opts.ActivityTaskQueues...),
		ActivityTypes: activities.Types(),
		Contracts:     activities.Contracts(),
		Default:       opts.ActivityWorkerVersionDefault,
		Weight:        opts.ActivityWorkerVersionWeight,
	}
	if opts.ActivityWorkerVersionHeartbeatTTL > 0 {
		worker.HeartbeatTTL = opts.ActivityWorkerVersionHeartbeatTTL.String()
	}
	return worker
}
