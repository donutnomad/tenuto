package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/donutnomad/tenuto/protocol"
)

const (
	ContinueAsNewReasonHistoryEventCount = protocol.ContinueAsNewReasonHistoryEventCount
	ContinueAsNewReasonHistorySizeBytes  = protocol.ContinueAsNewReasonHistorySizeBytes
)

type HistoryLimits struct {
	WarningEvents       int64
	WarningBytes        int64
	HardEvents          int64
	HardBytes           int64
	ReservedCloseEvents int64
	ReservedCloseBytes  int64
}

func DefaultHistoryLimits() HistoryLimits {
	return HistoryLimits{
		WarningEvents:       10_240,
		WarningBytes:        10 << 20,
		HardEvents:          51_200,
		HardBytes:           50 << 20,
		ReservedCloseEvents: 32,
		ReservedCloseBytes:  64 << 10,
	}
}

func (l HistoryLimits) Validate() error {
	if l.WarningEvents <= 0 || l.WarningBytes <= 0 || l.HardEvents <= 0 || l.HardBytes <= 0 {
		return errors.New("history limits must be positive")
	}
	if l.WarningEvents >= l.HardEvents || l.WarningBytes >= l.HardBytes {
		return errors.New("history warning limits must be below hard limits")
	}
	if l.ReservedCloseEvents < 0 || l.ReservedCloseBytes < 0 || l.ReservedCloseEvents >= l.HardEvents || l.ReservedCloseBytes >= l.HardBytes {
		return errors.New("history close reserve must be within hard limits")
	}
	return nil
}

type HistoryMetrics struct {
	EventCount int64 `json:"event_count"`
	SizeBytes  int64 `json:"size_bytes"`
}

// ContinueAsNewReasonsForMetrics 返回 History 达到 limits 的告警阈值时建议 continue-as-new 的原因。
func ContinueAsNewReasonsForMetrics(metrics HistoryMetrics, limits HistoryLimits) ([]ContinueAsNewReason, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	var reasons []ContinueAsNewReason
	if metrics.EventCount >= limits.WarningEvents {
		reasons = append(reasons, ContinueAsNewReasonHistoryEventCount)
	}
	if metrics.SizeBytes >= limits.WarningBytes {
		reasons = append(reasons, ContinueAsNewReasonHistorySizeBytes)
	}
	return reasons, nil
}

// PopulateWorkflowTaskStartedHistoryInfo 给 WorkflowTaskStarted 写入包含它自己的 History 快照，以及按 limits
// 得出的 continue-as-new 建议。limits 是存储执行的上限。
func PopulateWorkflowTaskStartedHistoryInfo(event *Event, nextEventID int64, current HistoryMetrics, limits HistoryLimits) error {
	if event == nil || event.WorkflowTaskStarted == nil {
		return errors.New("workflow task started event is required")
	}
	if nextEventID <= 0 {
		return errors.New("workflow task started event ID must be positive")
	}
	if current.EventCount < 0 || current.SizeBytes < 0 {
		return errors.New("workflow history metrics must be non-negative")
	}
	if err := limits.Validate(); err != nil {
		return err
	}

	originalID := event.ID
	event.ID = nextEventID
	defer func() { event.ID = originalID }()
	event.WorkflowTaskStarted.HistoryEventCount = current.EventCount + 1
	event.WorkflowTaskStarted.HistorySizeBytes = current.SizeBytes
	event.WorkflowTaskStarted.ContinueAsNewReasons = nil
	for range 16 {
		size, err := CanonicalEventSize(*event)
		if err != nil {
			return err
		}
		metrics := HistoryMetrics{EventCount: current.EventCount + 1, SizeBytes: current.SizeBytes + size}
		reasons, err := ContinueAsNewReasonsForMetrics(metrics, limits)
		if err != nil {
			return err
		}
		started := event.WorkflowTaskStarted
		if started.HistorySizeBytes == metrics.SizeBytes && slices.Equal(started.ContinueAsNewReasons, reasons) {
			return nil
		}
		started.HistorySizeBytes = metrics.SizeBytes
		started.ContinueAsNewReasons = reasons
	}
	return errors.New("workflow task started history snapshot did not converge")
}

type HistoryLimitExceededError struct {
	Metric    ContinueAsNewReason
	Projected int64
	Limit     int64
}

func (e *HistoryLimitExceededError) Error() string {
	return fmt.Sprintf("history limit exceeded: %s projected=%d limit=%d", e.Metric, e.Projected, e.Limit)
}

// Is 把它归为请求引起的失败：这次写入会让 History 超过上限。
func (e *HistoryLimitExceededError) Is(target error) bool {
	return target == protocol.ErrInvalidRequest
}

func CanonicalEventJSON(event Event) ([]byte, error) {
	return json.Marshal(event)
}

func CanonicalEventSize(event Event) (int64, error) {
	data, err := CanonicalEventJSON(event)
	return int64(len(data)), err
}

// CheckHistoryBudget 报告在 current 之上追加 eventSizes 这些事件是否超过硬上限：普通事件不能用掉为关闭事件预留的余量，
// serverClose 为 true 的关闭事件可以用到完整的硬上限。
func CheckHistoryBudget(current HistoryMetrics, eventSizes []int64, limits HistoryLimits, serverClose bool) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	projected := current
	projected.EventCount += int64(len(eventSizes))
	for _, size := range eventSizes {
		if size < 0 {
			return errors.New("event size must be non-negative")
		}
		projected.SizeBytes += size
	}
	eventLimit, byteLimit := limits.HardEvents-limits.ReservedCloseEvents, limits.HardBytes-limits.ReservedCloseBytes
	if serverClose {
		eventLimit, byteLimit = limits.HardEvents, limits.HardBytes
	}
	if projected.EventCount > eventLimit {
		return &HistoryLimitExceededError{Metric: ContinueAsNewReasonHistoryEventCount, Projected: projected.EventCount, Limit: eventLimit}
	}
	if projected.SizeBytes > byteLimit {
		return &HistoryLimitExceededError{Metric: ContinueAsNewReasonHistorySizeBytes, Projected: projected.SizeBytes, Limit: byteLimit}
	}
	return nil
}

func UsesReservedHistoryCloseBudget(event Event) bool {
	return IsWorkflowTaskCloseEventType(event.EventType()) ||
		event.WorkflowCompleted != nil || event.WorkflowFailed != nil ||
		event.WorkflowCancelled != nil || event.WorkflowTerminated != nil ||
		event.WorkflowContinuedAsNew != nil
}
