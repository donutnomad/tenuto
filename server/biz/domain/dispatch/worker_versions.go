package dispatch

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// WorkerVersionTTL 返回 Worker 登记的心跳 TTL，未登记时采用 DefaultActivityWorkerVersionTTL。
func WorkerVersionTTL(worker protocol.ActivityWorkerVersion) (time.Duration, error) {
	return ResolveHeartbeatTTL(worker.HeartbeatTTL, protocol.DefaultActivityWorkerVersionTTL)
}

// ResolveHeartbeatTTL 解析登记的心跳 TTL，空值采用 fallback。
func ResolveHeartbeatTTL(heartbeatTTL string, fallback time.Duration) (time.Duration, error) {
	if heartbeatTTL == "" {
		return fallback, nil
	}
	ttl, err := time.ParseDuration(heartbeatTTL)
	if err != nil {
		return 0, protocol.InvalidRequestf("parse activity worker heartbeat ttl: %w", err)
	}
	if ttl <= 0 {
		return 0, protocol.InvalidRequestf("activity worker heartbeat ttl must be positive")
	}
	return ttl, nil
}
