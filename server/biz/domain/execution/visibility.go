package execution

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"maps"
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
	Limit            int               `json:"limit,omitempty"`
	OrderDesc        bool              `json:"order_desc,omitempty"`
	BlockedOnly      bool              `json:"blocked_only,omitempty"`
	// SkipTotal 为 true 时不计算 Total（应答 0），翻页看 NextPageToken。
	SkipTotal bool `json:"skip_total,omitempty"`
	// PageToken 是上一页的 RunListResult.NextPageToken，为空时从第一页开始。它绑定签发时的过滤条件，
	// 条件改变后再用返回 protocol.ErrInvalidRequest。
	PageToken string `json:"page_token,omitempty"`
}

// RunListResult 是一页 Run。Total 是满足条件的 Run 总数（SkipTotal 时为 0，与翻页无关），HasMore 表示这一页之后还有 Run，
// 此时 NextPageToken 是下一页的游标（同 Temporal ListWorkflowExecutions 的 next_page_token）。
type RunListResult struct {
	Items         []RunListItem `json:"items"`
	Total         int           `json:"total"`
	HasMore       bool          `json:"has_more"`
	NextPageToken string        `json:"next_page_token,omitempty"`
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

// ListRuns 把全部过滤条件交给存储层，按 (created_at, id) 游标翻页，Total 基于过滤后的集合。
// Limit 为 0 时用缺省页大小 50；Limit 超过 MaxRunListPageSize、RunIDs 超过 MaxRunListPageSize 个、PageToken 无法解析
// 或与本次过滤条件不符时返回 protocol.ErrInvalidRequest。
func ListRuns(store VisibilityStore, filter RunListFilter) (RunListResult, error) {
	if filter.Limit < 0 || filter.Limit > MaxRunListPageSize {
		return RunListResult{}, protocol.InvalidRequestf("run list limit must be between 0 and %d", MaxRunListPageSize)
	}
	if len(filter.RunIDs) > MaxRunListPageSize {
		return RunListResult{}, protocol.InvalidRequestf("run list accepts at most %d run_ids", MaxRunListPageSize)
	}
	filter.Namespace = protocol.NormalizeNamespace(filter.Namespace)
	fingerprint := filter.fingerprint()
	listFilter := StoreListFilter{
		Namespace: filter.Namespace, RunIDs: filter.RunIDs, WorkflowID: filter.WorkflowID,
		Statuses: filter.Statuses, SearchAttributes: filter.SearchAttributes, BlockedOnly: filter.BlockedOnly,
		IncludeTerminal: filter.IncludeTerminal,
		OrderDesc:       filter.OrderDesc, Limit: filter.Limit, SkipTotal: filter.SkipTotal,
	}
	if filter.PageToken != "" {
		cursor, err := decodeRunPageToken(filter.PageToken, fingerprint)
		if err != nil {
			return RunListResult{}, err
		}
		listFilter.After = &cursor
	}
	if len(filter.RunIDs) > 0 {
		// 指定 RunIDs 时一次返回全部命中项，已结束的 Run 也在内。
		listFilter.Limit = len(filter.RunIDs)
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
	result := RunListResult{Items: items, Total: storeResult.Total, HasMore: storeResult.HasMore}
	if storeResult.HasMore && len(storeResult.Runs) > 0 {
		result.NextPageToken = encodeRunPageToken(CursorOf(storeResult.Runs[len(storeResult.Runs)-1]), fingerprint)
	}
	return result, nil
}

// runPageToken 是 NextPageToken 的内容：最后一行的排序键与签发时过滤条件的指纹。
type runPageToken struct {
	CreatedAt time.Time `json:"c"`
	RunID     string    `json:"r"`
	Filter    string    `json:"f"`
}

func encodeRunPageToken(cursor RunCursor, fingerprint string) string {
	data, _ := json.Marshal(runPageToken{CreatedAt: cursor.CreatedAt, RunID: cursor.RunID, Filter: fingerprint})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeRunPageToken(token, fingerprint string) (RunCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(token)
	var decoded runPageToken
	if err != nil || json.Unmarshal(data, &decoded) != nil || decoded.RunID == "" {
		return RunCursor{}, protocol.InvalidRequestf("invalid run list page_token")
	}
	if decoded.Filter != fingerprint {
		return RunCursor{}, protocol.InvalidRequestf("run list page_token was issued for a different filter")
	}
	return RunCursor{CreatedAt: decoded.CreatedAt, RunID: decoded.RunID}, nil
}

// fingerprint 摘要决定结果集与顺序的条件（namespace、Run ID、Workflow ID、状态、搜索属性、是否含已结束、blocked、方向）；
// Limit、SkipTotal 与 PageToken 不参与，翻页时可以改页大小。
func (f RunListFilter) fingerprint() string {
	statuses := slices.Clone(f.Statuses)
	slices.Sort(statuses)
	data, _ := json.Marshal([]any{
		f.Namespace, f.RunIDs, f.WorkflowID, statuses, f.SearchAttributes, f.IncludeTerminal, f.BlockedOnly, f.OrderDesc,
	})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:12])
}

// SearchAttributeTerm 是搜索属性过滤的匹配单位：过滤条件的每个键值是一个 term，Run 有这个 term 才匹配。
type SearchAttributeTerm struct {
	Key   string
	Value string
}

// SearchAttributeTerms 返回 attrs 能匹配的全部 term（去重、有序）。普通键是 (键, 值)，值为空串时同样是一个 term：
// 只有显式写成空串的键匹配空串过滤，缺失的键不匹配（同 Temporal SQL visibility 的 `= ”`）。
// ChangeVersionSearchAttribute 的值是 JSON 数组，每个成员各是一个 term，对应 Temporal 的 KeywordList 成员匹配；
// 无法解析的值没有 term。
func SearchAttributeTerms(attrs map[string]string) []SearchAttributeTerm {
	terms := make([]SearchAttributeTerm, 0, len(attrs))
	for key, value := range attrs {
		if key != history.ChangeVersionSearchAttribute {
			terms = append(terms, SearchAttributeTerm{Key: key, Value: value})
			continue
		}
		var members []string
		if err := json.Unmarshal([]byte(value), &members); err != nil {
			continue
		}
		for _, member := range members {
			terms = append(terms, SearchAttributeTerm{Key: key, Value: member})
		}
	}
	slices.SortFunc(terms, func(a, b SearchAttributeTerm) int {
		return cmp.Or(cmp.Compare(a.Key, b.Key), cmp.Compare(a.Value, b.Value))
	})
	return slices.Compact(terms)
}

// FilterSearchAttributeTerms 返回过滤条件的 term，按键排序：存储层以第一个 term 走索引。
func FilterSearchAttributeTerms(filter map[string]string) []SearchAttributeTerm {
	terms := make([]SearchAttributeTerm, 0, len(filter))
	for _, key := range slices.Sorted(maps.Keys(filter)) {
		terms = append(terms, SearchAttributeTerm{Key: key, Value: filter[key]})
	}
	return terms
}

// MatchSearchAttributes 是搜索属性过滤的唯一判据，两个存储后端共用：attrs 含有过滤条件的全部 term 时匹配。
func MatchSearchAttributes(attrs, filter map[string]string) bool {
	terms := SearchAttributeTerms(attrs)
	for _, want := range FilterSearchAttributeTerms(filter) {
		if _, found := slices.BinarySearchFunc(terms, want, func(a, b SearchAttributeTerm) int {
			return cmp.Or(cmp.Compare(a.Key, b.Key), cmp.Compare(a.Value, b.Value))
		}); !found {
			return false
		}
	}
	return true
}

// GetRunStats 按 Run 行状态聚合 namespace 内的 Run 数量，与 ListRuns 的状态过滤同源。
func GetRunStats(store interface {
	CountRuns(namespace string) ([]RunCount, error)
}, namespace string) (RunStats, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	counts, err := store.CountRuns(namespace)
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
