package executionview

import (
	"sort"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

type WorkflowConfigSummary struct {
	TargetWorkerIdentity string `json:"target_worker_identity,omitempty"`
	TargetWorkerStatus   string `json:"target_worker_status,omitempty"`
	WorkflowType         string `json:"workflow_type"`
	Namespace            string `json:"namespace"`
	Name                 string `json:"name,omitempty"`
	LatestVersion        string `json:"latest_version"`
	Policy               string `json:"policy,omitempty"`
	WorkflowVersion      string `json:"workflow_version,omitempty"`
	VersionCount         int    `json:"version_count"`
	ActivityCount        int    `json:"activity_count"`
	Active               bool   `json:"active"`
}

type WorkflowConfigListResult struct {
	Items []WorkflowConfigSummary `json:"items"`
	Total int                     `json:"total"`
}

// WorkflowConfigSummaries 按 workflow 汇总 namespace 内已存的配置版本，顺序不保证。
func (v *View) WorkflowConfigSummaries(namespace string) ([]WorkflowConfigSummary, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	active, err := v.configs.ActiveWorkflowConfigRefsInNamespace(namespace)
	if err != nil {
		return nil, err
	}
	stored, err := v.configs.ListWorkflowConfigRefsInNamespace(namespace)
	if err != nil {
		return nil, err
	}
	activeRefs := make(map[string]definition.WorkflowConfigRef)
	for _, ref := range active {
		activeRefs[ref.WorkflowType] = ref
	}
	grouped := make(map[string][]definition.WorkflowConfigRef)
	for _, ref := range stored {
		grouped[ref.WorkflowType] = append(grouped[ref.WorkflowType], ref)
	}
	summaries := make([]WorkflowConfigSummary, 0, len(grouped))
	for workflowType, versions := range grouped {
		// active 版本是该 workflow 的 latest；尚未激活时展示已存的第一个内容版本。
		latest, active := activeRefs[workflowType]
		if !active {
			latest = versions[0]
		}
		summary := WorkflowConfigSummary{
			WorkflowType: workflowType, Namespace: namespace,
			VersionCount: len(versions), LatestVersion: latest.Version,
			Active: active,
		}
		if wf, err := v.configs.GetWorkflowConfig(latest); err == nil {
			summary.Name = wf.Name
			summary.TargetWorkerIdentity = wf.TargetWorkerIdentity
			summary.ActivityCount = len(wf.Activities)
			summary.Policy = wf.Policy
			summary.WorkflowVersion = wf.Version
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

// ListWorkflowConfigs 返回按 Workflow Type 排序的配置汇总，并标出目标 Worker 的在线状态：
// default（未指定目标）、online、offline。
func (v *View) ListWorkflowConfigs(namespace string) (WorkflowConfigListResult, error) {
	summaries, err := v.WorkflowConfigSummaries(namespace)
	if err != nil {
		return WorkflowConfigListResult{}, err
	}
	workers, err := v.workers.ListActiveActivityWorkerVersions(v.clock.Now())
	if err != nil {
		return WorkflowConfigListResult{}, err
	}
	for i := range summaries {
		summaries[i].TargetWorkerStatus = "default"
		if target := summaries[i].TargetWorkerIdentity; target != "" {
			summaries[i].TargetWorkerStatus = "offline"
			for _, worker := range workers {
				if protocol.NormalizeNamespace(worker.Namespace) == protocol.NormalizeNamespace(namespace) && worker.Identity == target {
					summaries[i].TargetWorkerStatus = "online"
					break
				}
			}
		}
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].WorkflowType < summaries[j].WorkflowType })
	return WorkflowConfigListResult{Items: summaries, Total: len(summaries)}, nil
}
