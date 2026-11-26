package dispatch

import (
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// ValidateWorkerVersionIdentifiers 检查 Worker 登记里各标识的长度（tenuto_activity_worker_versions 的列宽）。
func ValidateWorkerVersionIdentifiers(worker protocol.ActivityWorkerVersion) error {
	errs := []error{
		protocol.CheckLength("namespace", worker.Namespace, protocol.MaxNamespaceLength),
		protocol.CheckLength("app", worker.App, protocol.MaxWorkflowTypeLength),
		protocol.CheckLength("identity", worker.Identity, protocol.MaxWorkerIdentityLength),
		protocol.CheckLength("build id", worker.BuildID, protocol.MaxBuildIDLength),
	}
	for _, queue := range worker.TaskQueues {
		errs = append(errs, protocol.CheckLength("task queue", queue, protocol.MaxTaskQueueLength))
	}
	for _, activityType := range worker.ActivityTypes {
		errs = append(errs, protocol.CheckLength("activity type", activityType, protocol.MaxActivityTypeLength))
	}
	return errors.Join(errs...)
}

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
