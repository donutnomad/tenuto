package protocol

// ContinueAsNewReason 是服务端建议 Workflow 执行 continue-as-new 的原因。
type ContinueAsNewReason string

const (
	ContinueAsNewReasonHistoryEventCount ContinueAsNewReason = "history_event_count"
	ContinueAsNewReasonHistorySizeBytes  ContinueAsNewReason = "history_size_bytes"
)

// WorkflowInfo is an immutable view of the committed history at activation start.
type WorkflowInfo struct {
	CurrentHistoryLength int64
	CurrentHistorySize   int64
	ContinueAsNewReasons []ContinueAsNewReason
}

func (i WorkflowInfo) GetCurrentHistoryLength() int64 { return i.CurrentHistoryLength }

func (i WorkflowInfo) GetCurrentHistorySize() int64 { return i.CurrentHistorySize }

func (i WorkflowInfo) GetContinueAsNewSuggested() bool { return len(i.ContinueAsNewReasons) > 0 }

func (i WorkflowInfo) GetContinueAsNewReasons() []ContinueAsNewReason {
	return append([]ContinueAsNewReason(nil), i.ContinueAsNewReasons...)
}
