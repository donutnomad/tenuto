package executionview

import (
	"sort"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// WorkerListFilter、WorkerListItem、WorkerListResult 是活跃 Worker 列表的查询条件与结果。
type WorkerListFilter struct {
	Namespace string `json:"namespace,omitempty"`
}

// WorkerListItem 是一个 Worker（namespace、identity、build）。Activity 部分来自数据库里的登记；
// Workflow Worker 的心跳与 StickyCache 来自应答这个请求的副本：多副本时只包含在这个副本上 poll 过的 Workflow Worker。
type WorkerListItem struct {
	Namespace       string                           `json:"namespace,omitempty"`
	App             string                           `json:"app,omitempty"`
	Identity        string                           `json:"identity,omitempty"`
	BuildID         string                           `json:"build_id,omitempty"`
	TaskQueues      []string                         `json:"task_queues,omitempty"`
	ActivityTypes   []string                         `json:"activity_types,omitempty"`
	Default         bool                             `json:"default,omitempty"`
	Weight          int                              `json:"weight,omitempty"`
	LastHeartbeatAt time.Time                        `json:"last_heartbeat_at,omitempty"`
	ExpiresAt       time.Time                        `json:"expires_at,omitempty"`
	StickyCache     *protocol.ExecutionCacheSnapshot `json:"sticky_cache,omitempty"`
}

type WorkerListResult struct {
	Items []WorkerListItem `json:"items"`
	Total int              `json:"total"`
}

// WorkflowWorkerStatus 是一个副本观察到的 Workflow Worker：最近一次 poll 带来的 WorkerInfo 与有效期。
type WorkflowWorkerStatus struct {
	Info            protocol.WorkerInfo
	LastHeartbeatAt time.Time
	ExpiresAt       time.Time
}

// ListWorkers 合并 namespace 内的活跃 Activity Worker 与 workflowWorkers，按 (identity, build) 排序。
func (v *View) ListWorkers(filter WorkerListFilter, workflowWorkers []WorkflowWorkerStatus) (WorkerListResult, error) {
	filter.Namespace = protocol.NormalizeNamespace(filter.Namespace)
	workers, err := v.workers.ListActiveActivityWorkerVersions(v.clock.Now())
	if err != nil {
		return WorkerListResult{}, err
	}
	type workerKey struct {
		Namespace string
		Identity  string
		BuildID   string
	}
	grouped := make(map[workerKey]*WorkerListItem)
	for _, worker := range dispatch.SummarizeActivityWorkers(workers, filter.Namespace) {
		key := workerKey{Namespace: worker.Namespace, Identity: worker.Identity, BuildID: worker.BuildID}
		grouped[key] = &WorkerListItem{
			Namespace: worker.Namespace, App: worker.App, Identity: worker.Identity,
			BuildID: worker.BuildID, TaskQueues: worker.TaskQueues,
			ActivityTypes: worker.ActivityTypes, Default: worker.Default,
			Weight: worker.Weight, LastHeartbeatAt: worker.LastHeartbeatAt,
			ExpiresAt: worker.ExpiresAt,
		}
	}
	for _, worker := range workflowWorkers {
		info := worker.Info
		key := workerKey{Namespace: protocol.NormalizeNamespace(info.Namespace), Identity: info.Identity, BuildID: info.BuildID}
		if key.Namespace != filter.Namespace {
			continue
		}
		item, ok := grouped[key]
		if !ok {
			item = &WorkerListItem{Namespace: key.Namespace, Identity: key.Identity, BuildID: key.BuildID}
			grouped[key] = item
		}
		item.LastHeartbeatAt = worker.LastHeartbeatAt
		item.ExpiresAt = worker.ExpiresAt
		if info.StickyCache != nil {
			snapshot := *info.StickyCache
			snapshot.Evictions = make(map[protocol.ExecutionCacheEvictionReason]uint64, len(info.StickyCache.Evictions))
			for reason, count := range info.StickyCache.Evictions {
				snapshot.Evictions[reason] = count
			}
			item.StickyCache = &snapshot
		}
	}
	items := make([]WorkerListItem, 0, len(grouped))
	for _, item := range grouped {
		sort.Strings(item.TaskQueues)
		sort.Strings(item.ActivityTypes)
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Identity != items[j].Identity {
			return items[i].Identity < items[j].Identity
		}
		return items[i].BuildID < items[j].BuildID
	})
	return WorkerListResult{Items: items, Total: len(items)}, nil
}

// ListTaskQueues 列出 namespace 内活跃 Activity Worker 订阅的任务队列。
func (v *View) ListTaskQueues(namespace string) (dispatch.TaskQueueListResult, error) {
	workers, err := v.workers.ListActiveActivityWorkerVersions(v.clock.Now())
	if err != nil {
		return dispatch.TaskQueueListResult{}, err
	}
	return dispatch.ListTaskQueues(workers, namespace), nil
}
