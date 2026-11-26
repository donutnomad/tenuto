package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/donutnomad/tenuto/protocol"
)

type HistoryBranch struct {
	Version   int                  `json:"v"`
	TreeID    string               `json:"tree_id"`
	BranchID  string               `json:"branch_id"`
	Ancestors []HistoryBranchRange `json:"ancestors,omitempty"`
}

type HistoryBranchRange struct {
	BranchID    string `json:"branch_id"`
	BeginNodeID int64  `json:"begin_node_id"`
	EndNodeID   int64  `json:"end_node_id"`
}

func GetBeginNodeID(b *HistoryBranch) int64 {
	if len(b.Ancestors) == 0 {
		return 1
	}
	return b.Ancestors[len(b.Ancestors)-1].EndNodeID
}

// ForkHistoryBranch 返回从 source 分出的新 branch：新 branch 共享 source 中 node_id <= forkNodeID 的
// 历史（作为祖先区间），自己的事件从 forkNodeID+1 开始。
func ForkHistoryBranch(source *HistoryBranch, forkNodeID int64, newBranchID string) *HistoryBranch {
	end := forkNodeID + 1
	sourceBegin := GetBeginNodeID(source)
	var ancestors []HistoryBranchRange
	if end <= sourceBegin {
		for _, a := range source.Ancestors {
			if a.EndNodeID <= end {
				ancestors = append(ancestors, a)
				continue
			}
			ancestors = append(ancestors, HistoryBranchRange{BranchID: a.BranchID, BeginNodeID: a.BeginNodeID, EndNodeID: end})
			break
		}
	} else {
		ancestors = append(ancestors, source.Ancestors...)
		ancestors = append(ancestors, HistoryBranchRange{BranchID: source.BranchID, BeginNodeID: sourceBegin, EndNodeID: end})
	}
	return &HistoryBranch{TreeID: source.TreeID, BranchID: newBranchID, Ancestors: ancestors}
}

func SortAncestors(ancestors []HistoryBranchRange) {
	sort.Slice(ancestors, func(i, j int) bool {
		return ancestors[i].EndNodeID < ancestors[j].EndNodeID
	})
	if len(ancestors) > 0 {
		ancestors[0].BeginNodeID = 1
		for i := 1; i < len(ancestors); i++ {
			ancestors[i].BeginNodeID = ancestors[i-1].EndNodeID
		}
	}
}

func SerializeBranchToken(b *HistoryBranch) ([]byte, error) {
	b.Version = 1
	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("serialize branch token: %w", err)
	}
	return data, nil
}

// ErrInvalidBranchToken 表示 Run 保存的 branch token 解不出 History tree 与 branch：数据损坏，重试不会恢复。
var ErrInvalidBranchToken = errors.New("invalid branch token")

func DeserializeBranchToken(data []byte) (*HistoryBranch, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty", ErrInvalidBranchToken)
	}
	var b HistoryBranch
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidBranchToken, err)
	}
	return &b, nil
}

type ForkBranchRequest struct {
	SourceRunID string
	ForkNodeID  int64
	NewRunID    string
	NewBranchID string
	Info        string
}

type ForkBranchResponse struct {
	BranchToken []byte
}

// HistoryBranchStore 从已有 Run 的 History 前缀派生新 Run 的 History，供 reset 使用。
// History 的回收属于 retention 仓储，不经这个接口。
type HistoryBranchStore interface {
	ForkBranch(req ForkBranchRequest) (*ForkBranchResponse, error)
}

type ResetType string

var ResetTypeEnums = struct {
	FirstWorkflowTask ResetType
	LastWorkflowTask  ResetType
}{FirstWorkflowTask: "first_workflow_task", LastWorkflowTask: "last_workflow_task"}

type ResetReapplyExcludeType string

var ResetReapplyExcludeTypeEnums = struct {
	Signal ResetReapplyExcludeType
	Update ResetReapplyExcludeType
}{Signal: "signal", Update: "update"}

// ResetWorkflowRequest 是一次 Reset：Namespace 与 RunID 寻址被 reset 的 Run（Namespace 为空表示默认 namespace）。
type ResetWorkflowRequest struct {
	Namespace           string
	RunID               string
	Reason              string
	ResetType           ResetType
	ResetEventID        int64
	ReapplyExcludeTypes []string
	// RequestID 是可选的幂等键：同一个 base Run 上带同一个 RequestID 的 Reset 只生效一次，重复的请求返回第一次
	// reset 出的 Run（对标 Temporal ResetWorkflowExecution 的 request_id）。为空时不去重。
	RequestID string
}

// Validate requires exactly one reset point: a reset type or a reset event ID.
func (r ResetWorkflowRequest) Validate() error {
	if err := ValidateRequestID(r.RequestID); err != nil {
		return err
	}
	switch {
	case r.ResetType == "" && r.ResetEventID <= 0:
		return protocol.InvalidRequestf("reset requires reset_type (first_workflow_task or last_workflow_task) or reset_event_id")
	case r.ResetType != "" && r.ResetEventID > 0:
		return protocol.InvalidRequestf("reset_type and reset_event_id are mutually exclusive")
	case r.ResetType != "" && r.ResetType != ResetTypeEnums.FirstWorkflowTask && r.ResetType != ResetTypeEnums.LastWorkflowTask:
		return protocol.InvalidRequestf("unknown reset type: %s", r.ResetType)
	}
	return nil
}
