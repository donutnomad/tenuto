package engine

import (
	"context"
	"time"
)

// *Server 实现保留期回收用例消费的两个端口：retention.WorkflowLocker 与 retention.Metrics。

// LockWorkflow 取 (namespace, workflowID) 的进程内 keyedMu：只与本副本上该 workflow 的写操作互斥。
func (s *Server) LockWorkflow(ctx context.Context, namespace, workflowID string) (func(), error) {
	return s.keyedMu.Lock(ctx, workflowLockKey(namespace, workflowID))
}

// RecordGCReclaimed、RecordGCFailure 与 RecordGCBacklog 在 Server 未配置 telemetry 时不记录。
func (s *Server) RecordGCReclaimed(ctx context.Context, object string, count int) {
	if s.telemetry != nil {
		s.telemetry.recordGCReclaimed(ctx, object, count)
	}
}

func (s *Server) RecordGCFailure(ctx context.Context, outcome string) {
	if s.telemetry != nil {
		s.telemetry.recordGCFailure(ctx, outcome)
	}
}

func (s *Server) RecordGCBacklog(ctx context.Context, overdue int, oldestAge time.Duration) {
	if s.telemetry != nil {
		s.telemetry.recordGCBacklog(ctx, overdue, oldestAge)
	}
}
