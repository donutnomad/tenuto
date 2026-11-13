package execution

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type RunListFilter struct {
	Namespace        string            `json:"namespace,omitempty"`
	RunIDs           []string          `json:"run_ids,omitempty"`
	WorkflowID       string            `json:"workflow_id,omitempty"`
	Statuses         []history.Status  `json:"statuses,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	IncludeTerminal  bool              `json:"include_terminal,omitempty"`
	Offset           int               `json:"offset,omitempty"`
	Limit            int               `json:"limit,omitempty"`
	OrderDesc        bool              `json:"order_desc,omitempty"`
	BlockedOnly      bool              `json:"blocked_only,omitempty"`
}

type RunListResult struct {
	Items []RunListItem `json:"items"`
	Total int           `json:"total"`
}

type RunListItem struct {
	RunID                 string         `json:"run_id"`
	Namespace             string         `json:"namespace,omitempty"`
	WorkflowID            string         `json:"workflow_id"`
	WorkflowType          string         `json:"workflow_type"`
	Status                history.Status `json:"status"`
	WorkflowConfigVersion string         `json:"workflow_config_version,omitempty"`
	ParentRunID           string         `json:"parent_run_id,omitempty"`
	ResetRunID            string         `json:"reset_run_id,omitempty"`
	IsReset               bool           `json:"is_reset,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	// ClosedAt 是 Run 结束的时间；Run 未结束时省略。
	ClosedAt         *time.Time        `json:"closed_at,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	CurrentWaiting   WaitingState      `json:"current_waiting,omitempty"`
	FailureReason    string            `json:"failure_reason,omitempty"`
	HistoryLength    int               `json:"history_length"`
	Blocked          *BlockedInfo      `json:"blocked,omitempty"`
}

type RunStats struct {
	Total          int `json:"total"`
	Running        int `json:"running"`
	Completed      int `json:"completed"`
	Failed         int `json:"failed"`
	Cancelled      int `json:"cancelled"`
	Terminated     int `json:"terminated"`
	ContinuedAsNew int `json:"continued_as_new"`
}

type VisibilityStore interface {
	List(filter StoreListFilter) (StoreListResult, error)
	ListWorkflowExecutionStates(runIDs []string) (map[string]*WorkflowExecutionState, error)
}

// MaxRunListPageSize 是 Run List 一页的上限，也是一次按 RunIDs 查询的 ID 数上限。
// 存储层按页读出完整的 Run 行（含输入），不设上限时一次请求可以把整个 Namespace 读进内存。
const MaxRunListPageSize = 1000

// ListRuns 把全部过滤条件交给存储层，分页与 Total 都基于过滤后的集合。
// Limit 为 0 时用缺省页大小 50；Limit 超过 MaxRunListPageSize、Offset 为负或 RunIDs 超过
// MaxRunListPageSize 个时返回 protocol.ErrInvalidRequest。
func ListRuns(store VisibilityStore, filter RunListFilter) (RunListResult, error) {
	if filter.Limit < 0 || filter.Limit > MaxRunListPageSize {
		return RunListResult{}, protocol.InvalidRequestf("run list limit must be between 0 and %d", MaxRunListPageSize)
	}
	if filter.Offset < 0 {
		return RunListResult{}, protocol.InvalidRequestf("run list offset must not be negative")
	}
	if len(filter.RunIDs) > MaxRunListPageSize {
		return RunListResult{}, protocol.InvalidRequestf("run list accepts at most %d run_ids", MaxRunListPageSize)
	}
	filter.Namespace = protocol.NormalizeNamespace(filter.Namespace)
	listFilter := StoreListFilter{
		Namespace: filter.Namespace, RunIDs: filter.RunIDs, WorkflowID: filter.WorkflowID,
		Statuses: filter.Statuses, SearchAttributes: filter.SearchAttributes, BlockedOnly: filter.BlockedOnly,
		IncludeTerminal: filter.IncludeTerminal,
		OrderDesc:       filter.OrderDesc, Offset: filter.Offset, Limit: filter.Limit,
	}
	if len(filter.RunIDs) > 0 {
		// 指定 RunIDs 时一次返回全部命中项，已结束的 Run 也在内。
		listFilter.Offset, listFilter.Limit = 0, len(filter.RunIDs)
		listFilter.IncludeTerminal = true
	}
	storeResult, err := store.List(listFilter)
	if err != nil {
		return RunListResult{}, err
	}
	runIDs := make([]string, 0, len(storeResult.Runs))
	for _, item := range storeResult.Runs {
		runIDs = append(runIDs, item.ID)
	}
	projected, err := store.ListWorkflowExecutionStates(runIDs)
	if err != nil {
		return RunListResult{}, err
	}
	items := make([]RunListItem, 0, len(storeResult.Runs))
	for _, item := range storeResult.Runs {
		// execution state 与 Run 行同生同灭；读不到说明 Run 在两次读取之间被删除。
		state := projected[item.ID]
		if state == nil {
			continue
		}
		view := RunListItem{
			RunID: item.ID, Namespace: item.Namespace, WorkflowID: item.WorkflowID,
			WorkflowType: item.WorkflowType, Status: state.Status,
			WorkflowConfigVersion: item.WorkflowConfigVersion, ParentRunID: item.ParentRunID,
			ResetRunID: item.ResetRunID, IsReset: state.IsReset, CreatedAt: item.CreatedAt, ClosedAt: state.ClosedAt,
			SearchAttributes: state.SearchAttributes, CurrentWaiting: state.CurrentWaiting,
			FailureReason: state.FailureReason, HistoryLength: int(state.HistoryEventCount),
		}
		if state.Blocked.Blocked {
			blocked := state.Blocked
			view.Blocked = &blocked
		}
		items = append(items, view)
	}
	return RunListResult{Items: items, Total: storeResult.Total}, nil
}

// MatchSearchAttributes 是搜索属性过滤的唯一判据，两个存储后端共用：
// 普通键按值全等（缺失的键等同空串），ChangeVersionSearchAttribute 按列表成员匹配。
func MatchSearchAttributes(attrs, filter map[string]string) bool {
	for key, value := range filter {
		if key == history.ChangeVersionSearchAttribute {
			if !changeVersionsContain(attrs[key], value) {
				return false
			}
			continue
		}
		if attrs[key] != value {
			return false
		}
	}
	return true
}

// changeVersionsContain 按列表成员匹配，对应 Temporal 的 KeywordList 查询语义。
func changeVersionsContain(encoded, value string) bool {
	var values []string
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		return false
	}
	return slices.Contains(values, value)
}

// GetRunStats 按 Run 行状态聚合 namespace 内的 Run 数量，与 ListRuns 的状态过滤同源。
func GetRunStats(store interface{ CountRuns() ([]RunCount, error) }, namespace string) (RunStats, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	counts, err := store.CountRuns()
	if err != nil {
		return RunStats{}, err
	}
	var stats RunStats
	for _, item := range counts {
		if protocol.NormalizeNamespace(item.Namespace) != namespace {
			continue
		}
		stats.Total += item.Count
		switch item.Status {
		case history.StatusRunning:
			stats.Running += item.Count
		case history.StatusCompleted:
			stats.Completed += item.Count
		case history.StatusFailed:
			stats.Failed += item.Count
		case history.StatusCancelled:
			stats.Cancelled += item.Count
		case history.StatusTerminated:
			stats.Terminated += item.Count
		case history.StatusContinuedAsNew:
			stats.ContinuedAsNew += item.Count
		}
	}
	return stats, nil
}
