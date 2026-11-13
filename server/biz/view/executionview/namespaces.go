package executionview

import (
	"sort"

	"github.com/donutnomad/tenuto/protocol"
)

type NamespaceInfo struct {
	Name          string `json:"name"`
	RunCount      int    `json:"run_count"`
	WorkflowCount int    `json:"workflow_count"`
	WorkerCount   int    `json:"worker_count"`
	ScheduleCount int    `json:"schedule_count"`
}

type NamespaceListResult struct {
	Items []NamespaceInfo `json:"items"`
	Total int             `json:"total"`
}

// ListNamespaces 列出有 Run 或活跃 Activity Worker 的 namespace 及其各项计数，按名称排序。
// WorkerCount 是活跃 Worker 的个数，同一 (identity, build) 的多条路由算一个 Worker。
func (v *View) ListNamespaces() (NamespaceListResult, error) {
	nsMap := make(map[string]*NamespaceInfo)
	ensureNS := func(ns string) *NamespaceInfo {
		ns = protocol.NormalizeNamespace(ns)
		info, ok := nsMap[ns]
		if !ok {
			info = &NamespaceInfo{Name: ns}
			nsMap[ns] = info
		}
		return info
	}
	counts, err := v.runs.CountRuns()
	if err != nil {
		return NamespaceListResult{}, err
	}
	for _, item := range counts {
		ensureNS(item.Namespace).RunCount += item.Count
	}
	workers, err := v.workers.ListActiveActivityWorkerVersions(v.clock.Now())
	if err != nil {
		return NamespaceListResult{}, err
	}
	type workerKey struct{ namespace, identity, buildID string }
	seenWorkers := make(map[workerKey]bool)
	for _, worker := range workers {
		key := workerKey{protocol.NormalizeNamespace(worker.Namespace), worker.Identity, worker.BuildID}
		info := ensureNS(key.namespace)
		if !seenWorkers[key] {
			seenWorkers[key] = true
			info.WorkerCount++
		}
	}
	// namespace 集合齐了之后再算各 namespace 的 Workflow 与 Schedule 数。
	for ns, info := range nsMap {
		refs, err := v.configs.ListWorkflowConfigRefsInNamespace(ns)
		if err != nil {
			return NamespaceListResult{}, err
		}
		workflows := make(map[string]bool)
		for _, ref := range refs {
			workflows[ref.WorkflowID] = true
		}
		info.WorkflowCount = len(workflows)
		schedules, err := v.schedules.ListSchedules(ns)
		if err != nil {
			return NamespaceListResult{}, err
		}
		info.ScheduleCount = len(schedules)
	}
	items := make([]NamespaceInfo, 0, len(nsMap))
	for _, info := range nsMap {
		items = append(items, *info)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return NamespaceListResult{Items: items, Total: len(items)}, nil
}
