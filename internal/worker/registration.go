package worker

import (
	"context"
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

func RunActivityWorkerVersionRegistry(ctx context.Context, logger *slog.Logger, worker protocol.ActivityWorkerVersion, registry ActivityWorkerVersionRegistryAPI, interval time.Duration, ready func(error)) {
	for {
		if err := registry.RegisterActivityWorkerVersion(ctx, worker); err != nil {
			logger.Error("register activity worker version failed", "err", err)
			if !waitWorkerVersionInterval(ctx, interval) {
				ready(ctx.Err())
				return
			}
			continue
		}
		ready(nil)
		break
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := registry.HeartbeatActivityWorkerVersionInNamespace(ctx, protocol.NormalizeNamespace(worker.Namespace), worker.Identity, worker.BuildID); err != nil {
				logger.Error("heartbeat activity worker version failed", "err", err)
			}
		}
	}
}

func waitWorkerVersionInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

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
