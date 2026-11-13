package engine

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 验证 Reset reapply 的 exclude 语义：
// 默认 reapply 所有 Signal/Update，excludeTypes 排除指定类型。

// reapplyFixtureEvents：构造 resetNodeID=10 之后的待 reapply 事件序列。
//
//	11 WorkflowSignaled
//	12 WorkflowUpdateAccepted
//	13 ActivityCompleted        ← 既非 Signal 也非 Update，永不 reapply
//	14 WorkflowSignaled
//
// resetNodeID 之前的事件（id<=10）不参与 reapply，这里不构造。
func reapplyFixtureEvents() []history.Event {
	return []history.Event{
		eventWithID(11, history.NewWorkflowSignaledEvent("s1", nil)),
		eventWithID(12, history.NewWorkflowUpdateAcceptedEvent("up1", "u1", nil)),
		eventWithID(13, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: "a1"})),
		eventWithID(14, history.NewWorkflowSignaledEvent("s2", nil)),
	}
}

// runReapply 返回 execution.ResetReapplyEvents 选出的事件类型序列。
func runReapply(t *testing.T, excludeTypes []string) []history.EventType {
	t.Helper()
	var seq []history.EventType
	for _, e := range execution.ResetReapplyEvents(reapplyFixtureEvents(), 10, excludeTypes) {
		if e.WorkflowSignaled != nil {
			seq = append(seq, history.EventWorkflowSignaled)
		} else if e.WorkflowUpdateAccepted != nil {
			seq = append(seq, history.EventWorkflowUpdateAccepted)
		}
	}
	return seq
}

func countType(seq []history.EventType, typ history.EventType) int {
	n := 0
	for _, t := range seq {
		if t == typ {
			n++
		}
	}
	return n
}

func TestReapplyEvents_AllEligible_NoExclude_BackwardCompatible(t *testing.T) {
	// Happy Path：空 exclude → reapply 全部 Signal+Update（2 Signal + 1 Update）
	seq := runReapply(t, nil)
	if got := countType(seq, history.EventWorkflowSignaled); got != 2 {
		t.Fatalf("expected 2 signals reapplied, got %d", got)
	}
	if got := countType(seq, history.EventWorkflowUpdateAccepted); got != 1 {
		t.Fatalf("expected 1 update reapplied, got %d", got)
	}
}

func TestReapplyEvents_AllEligible_ExcludeSignal(t *testing.T) {
	// Edge Case：exclude{signal} → 只 reapply Update，Signal 被反向排除
	seq := runReapply(t, []string{string(execution.ResetReapplyExcludeTypeEnums.Signal)})
	if got := countType(seq, history.EventWorkflowSignaled); got != 0 {
		t.Fatalf("exclude signal: expected 0 signals, got %d", got)
	}
	if got := countType(seq, history.EventWorkflowUpdateAccepted); got != 1 {
		t.Fatalf("exclude signal: expected 1 update, got %d", got)
	}
}

func TestReapplyEvents_AllEligible_ExcludeUpdate(t *testing.T) {
	// Edge Case：exclude{update} → 只 reapply Signal，Update 被反向排除
	seq := runReapply(t, []string{string(execution.ResetReapplyExcludeTypeEnums.Update)})
	if got := countType(seq, history.EventWorkflowSignaled); got != 2 {
		t.Fatalf("exclude update: expected 2 signals, got %d", got)
	}
	if got := countType(seq, history.EventWorkflowUpdateAccepted); got != 0 {
		t.Fatalf("exclude update: expected 0 updates, got %d", got)
	}
}

func TestReapplyEvents_AllEligible_ExcludeBoth(t *testing.T) {
	// Edge Case：exclude{signal,update} → 全部排除，无事件 reapply
	seq := runReapply(t,
		[]string{string(execution.ResetReapplyExcludeTypeEnums.Signal), string(execution.ResetReapplyExcludeTypeEnums.Update)})
	if len(seq) != 0 {
		t.Fatalf("exclude both: expected 0 reapplied, got %d (%v)", len(seq), seq)
	}
}
