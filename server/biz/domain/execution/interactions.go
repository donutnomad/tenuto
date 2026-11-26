package execution

import (
	"errors"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type EventAppender interface {
	Append(runID, treeID, branchID string, event history.Event) (int64, error)
}

func EnsureActive(current *Run) error {
	if history.IsTerminalStatus(current.Status) {
		return protocol.InvalidRequestf("workflow %s is %s", current.ID, current.Status)
	}
	return nil
}

func RequestCancellation(writer EventAppender, current *Run, treeID, branchID, reason string) error {
	if current.Status != history.StatusRunning {
		return protocol.InvalidRequestf("workflow %s is %s", current.ID, current.Status)
	}
	_, err := writer.Append(current.ID, treeID, branchID, history.NewWorkflowCancellationRequestedEvent(reason))
	return err
}

// ValidateRequestID 拒绝超过 protocol.MaxRequestIDLength 字节的请求幂等键；空值表示不去重。
func ValidateRequestID(requestID string) error {
	if len(requestID) > protocol.MaxRequestIDLength {
		return protocol.InvalidRequestf("request id is longer than %d bytes", protocol.MaxRequestIDLength)
	}
	return nil
}

// RecordSignal 在 Run 上记一条 WorkflowSignaled。requestID 非空时同一个 Run 上同一个 requestID 只记一条：
// 重发的请求返回第一次记下的事件 ID。
func RecordSignal(tx ExecutionTx, current *Run, treeID, branchID, name, requestID string, input map[string]any) (int64, error) {
	if err := EnsureActive(current); err != nil {
		return 0, err
	}
	event := history.NewWorkflowSignaledEvent(name, input)
	if requestID == "" {
		return tx.Append(current.ID, treeID, branchID, event)
	}
	event.WorkflowSignaled.RequestID = requestID
	id, _, err := tx.AppendIdempotent(current.ID, treeID, branchID, event)
	return id, err
}

func AcceptUpdate(tx ExecutionTx, current *Run, treeID, branchID, name, updateID string, args history.Payloads) (int64, error) {
	if err := EnsureActive(current); err != nil {
		return 0, err
	}
	id, _, err := tx.AppendIdempotent(current.ID, treeID, branchID, history.NewWorkflowUpdateAcceptedEvent(name, updateID, args))
	return id, err
}

func mergeSearchAttributes(current, attrs map[string]string) map[string]string {
	merged := make(map[string]string, len(current)+len(attrs))
	for key, value := range current {
		merged[key] = value
	}
	for key, value := range attrs {
		if key != "" {
			merged[key] = value
		}
	}
	return merged
}

// StartSearchAttributes 返回 WorkflowStarted 事件记录的启动搜索属性。Run 与它的 WorkflowStarted
// 同事务写入，History 里没有这条事件说明数据损坏。
func StartSearchAttributes(events []history.Event) (map[string]string, error) {
	for _, event := range events {
		if event.WorkflowStarted != nil {
			return cloneStringMap(event.WorkflowStarted.SearchAttributes), nil
		}
	}
	return nil, errWorkflowStartedMissing
}

var errWorkflowStartedMissing = errors.New("history has no WorkflowStarted event")

// StartRequestID 返回 History 开头的 WorkflowStarted 记录的启动幂等键。重试与 continue-as-new 派生的 Run 把它写进
// 自己的 WorkflowStarted：按 current Run 的第一个事件去重的启动请求在接续链推进之后仍能认出这条链。
func StartRequestID(events []history.Event) string {
	if len(events) == 0 || events[0].WorkflowStarted == nil {
		return ""
	}
	return events[0].WorkflowStarted.RequestID
}

// SearchAttributesAtEvent 重建 Run 在 throughEventID 时的搜索属性，供 Reset 新 Run 使用：
// 启动值加 through 及之前的全部写入，与 Temporal 重建 mutable state 一致。
func SearchAttributesAtEvent(events []history.Event, throughEventID int64) (map[string]string, error) {
	start, err := StartSearchAttributes(events)
	if err != nil {
		return nil, err
	}
	prefix := make([]history.Event, 0, len(events))
	for _, event := range events {
		if event.ID <= throughEventID {
			prefix = append(prefix, event)
		}
	}
	return SearchAttributesFromHistory(start, prefix), nil
}

func UpsertSearchAttributes(tx ExecutionTx, current *Run, treeID, branchID string, attrs map[string]string) error {
	for key := range attrs {
		if history.IsReservedSearchAttribute(key) {
			return protocol.InvalidRequestf("%s is a reserved key that cannot be set, please use other key", key)
		}
	}
	merged := mergeSearchAttributes(current.SearchAttributes, attrs)
	values := make(map[string]any, len(attrs))
	for key, value := range attrs {
		values[key] = value
	}
	if _, err := tx.Append(current.ID, treeID, branchID, history.NewSearchAttributesUpsertedEvent(values)); err != nil {
		return err
	}
	current.setSearchAttributes(merged)
	return tx.UpdateRun(current)
}
