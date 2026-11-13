package engine

import (
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
)

// ListWorkers 列出 namespace 内的活跃 Worker：Activity Worker 取自数据库里的登记，Workflow Worker 取自本副本观察到的 poll。
func (s *Server) ListWorkers(filter executionview.WorkerListFilter) (executionview.WorkerListResult, error) {
	return s.view.ListWorkers(filter, s.listWorkflowWorkers(filter.Namespace, s.clock.Now()))
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
