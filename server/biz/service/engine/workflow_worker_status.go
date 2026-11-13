package engine

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
)

const workflowWorkerHeartbeatTTL = time.Minute

// recordWorkflowWorker 记下领取 Workflow Task 的 Worker。记录只在本副本的内存里：多副本时每个副本只知道
// 在它这里 poll 过的 Workflow Worker。
func (s *Server) recordWorkflowWorker(info protocol.WorkerInfo, now time.Time) {
	if info.Identity == "" {
		return
	}
	info.Namespace = protocol.NormalizeNamespace(info.Namespace)
	key := info.Namespace + "\x00" + info.Identity + "\x00" + info.BuildID
	s.mu.Lock()
	s.workflowWorkers[key] = executionview.WorkflowWorkerStatus{Info: info, LastHeartbeatAt: now, ExpiresAt: now.Add(workflowWorkerHeartbeatTTL)}
	s.mu.Unlock()
}

func (s *Server) listWorkflowWorkers(namespace string, now time.Time) []executionview.WorkflowWorkerStatus {
	namespace = protocol.NormalizeNamespace(namespace)
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]executionview.WorkflowWorkerStatus, 0)
	for key, worker := range s.workflowWorkers {
		if !worker.ExpiresAt.After(now) {
			delete(s.workflowWorkers, key)
			continue
		}
		if protocol.NormalizeNamespace(worker.Info.Namespace) == namespace {
			result = append(result, worker)
		}
	}
	return result
}
