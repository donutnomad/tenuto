package dispatch

import (
	"slices"
	"sort"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

type ActivityWorkerSummary struct {
	Namespace       string
	App             string
	Identity        string
	BuildID         string
	TaskQueues      []string
	ActivityTypes   []string
	Default         bool
	Weight          int
	LastHeartbeatAt time.Time
	ExpiresAt       time.Time
}

func SummarizeActivityWorkers(workers []protocol.ActivityWorkerVersion, namespace string) []ActivityWorkerSummary {
	namespace = protocol.NormalizeNamespace(namespace)
	type key struct{ identity, buildID string }
	grouped := make(map[key]*ActivityWorkerSummary)
	for _, worker := range workers {
		if protocol.NormalizeNamespace(worker.Namespace) != namespace {
			continue
		}
		k := key{worker.Identity, worker.BuildID}
		summary, ok := grouped[k]
		if !ok {
			summary = &ActivityWorkerSummary{
				Namespace: namespace, App: worker.App, Identity: worker.Identity,
				BuildID: worker.BuildID, Default: worker.Default, Weight: worker.Weight,
				LastHeartbeatAt: worker.LastHeartbeatAt, ExpiresAt: worker.ExpiresAt,
			}
			grouped[k] = summary
		}
		for _, queue := range worker.TaskQueues {
			if !slices.Contains(summary.TaskQueues, queue) {
				summary.TaskQueues = append(summary.TaskQueues, queue)
			}
		}
		for _, activityType := range worker.ActivityTypes {
			if !slices.Contains(summary.ActivityTypes, activityType) {
				summary.ActivityTypes = append(summary.ActivityTypes, activityType)
			}
		}
		if worker.LastHeartbeatAt.After(summary.LastHeartbeatAt) {
			summary.LastHeartbeatAt = worker.LastHeartbeatAt
		}
		if worker.ExpiresAt.After(summary.ExpiresAt) {
			summary.ExpiresAt = worker.ExpiresAt
		}
	}
	result := make([]ActivityWorkerSummary, 0, len(grouped))
	for _, summary := range grouped {
		sort.Strings(summary.TaskQueues)
		sort.Strings(summary.ActivityTypes)
		result = append(result, *summary)
	}
	return result
}

type TaskQueueInfo struct {
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace"`
	ActiveWorkers int      `json:"active_workers"`
	WorkerIDs     []string `json:"worker_ids,omitempty"`
	ActivityTypes []string `json:"activity_types,omitempty"`
}

type TaskQueueListResult struct {
	Items []TaskQueueInfo `json:"items"`
	Total int             `json:"total"`
}

func ListTaskQueues(workers []protocol.ActivityWorkerVersion, namespace string) TaskQueueListResult {
	namespace = protocol.NormalizeNamespace(namespace)
	type queueData struct {
		workerIDs     map[string]bool
		activityTypes map[string]bool
	}
	queues := make(map[string]*queueData)
	for _, worker := range workers {
		if protocol.NormalizeNamespace(worker.Namespace) != namespace {
			continue
		}
		for _, name := range worker.TaskQueues {
			q, ok := queues[name]
			if !ok {
				q = &queueData{workerIDs: make(map[string]bool), activityTypes: make(map[string]bool)}
				queues[name] = q
			}
			q.workerIDs[worker.Identity] = true
			for _, activityType := range worker.ActivityTypes {
				q.activityTypes[activityType] = true
			}
		}
	}
	items := make([]TaskQueueInfo, 0, len(queues))
	for name, q := range queues {
		ids := make([]string, 0, len(q.workerIDs))
		for id := range q.workerIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		activityTypes := make([]string, 0, len(q.activityTypes))
		for activityType := range q.activityTypes {
			activityTypes = append(activityTypes, activityType)
		}
		sort.Strings(activityTypes)
		items = append(items, TaskQueueInfo{
			Name: name, Namespace: namespace, ActiveWorkers: len(ids),
			WorkerIDs: ids, ActivityTypes: activityTypes,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return TaskQueueListResult{Items: items, Total: len(items)}
}
