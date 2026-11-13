package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// splitBatches 批次切分单测
// ============================================
// 测试策略:
// - Happy Path: 无 WFTC = 单批；标准双批
// - Edge Case: gormstore 形态（命令块被外部事件劈开）位置无关归位
// - Edge Case: UpdateAccepted 是外部事件开新批
// - Edge Case: 超时/失败 WFT 不切批
// - Edge Case: 历史以 WFTC+命令结尾 → 空 live 末批
// ============================================

func evStarted() history.Event {
	return history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
}
func evWFTC(id int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}}
}
func evSched(id int64, stepID string) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: stepID}}
}
func evComp(id, schedID int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: schedID, Output: map[string]any{"ok": true}}}
}
func evSignal(id int64, name string) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: name, Input: map[string]any{}}}
}
func evUpdateAccepted(id int64, name string) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{Name: name, UpdateID: "u1"}}
}
func evWFTTimedOut(id int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowTaskTimedOut: &history.WorkflowTaskTimedOutEvent{}}
}

func batchIDs(b []history.Event) []int64 {
	ids := make([]int64, 0, len(b))
	for _, ev := range b {
		ids = append(ids, ev.ID)
	}
	return ids
}

func assertBatches(t *testing.T, got [][]history.Event, want [][]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("批数 = %d, want %d; got=%v", len(got), len(want), gotIDs(got))
	}
	for i := range want {
		g := batchIDs(got[i])
		if len(g) != len(want[i]) {
			t.Fatalf("批 %d = %v, want %v", i, g, want[i])
		}
		for j := range want[i] {
			if g[j] != want[i][j] {
				t.Fatalf("批 %d = %v, want %v", i, g, want[i])
			}
		}
	}
}

func gotIDs(b [][]history.Event) [][]int64 {
	out := make([][]int64, 0, len(b))
	for _, x := range b {
		out = append(out, batchIDs(x))
	}
	return out
}

// Happy Path: 无 WFTCompleted → 单批（存量测试兼容路径）。
func TestSplitBatches_NoWFTC_SingleBatch(t *testing.T) {
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evSched(2, "#a1"), evComp(3, 2)}
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 3}})
}

// Happy Path: 标准双批——[Started, WFTC₁, cmd] 归批 1，新外部事件开批 2。
func TestSplitBatches_TwoBatches_CommandsFollowDecision(t *testing.T) {
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evWFTC(2), evSched(3, "#a1"), evComp(4, 3)}
	// 批1 = [Started, WFTC₁, sched(命令归批1)]；批2 = [comp(外部)]
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 3}, {4}})
}

// Edge Case: gormstore 形态——外部事件劈开命令块，命令位置无关仍归决策批。
func TestSplitBatches_InterleavedCommand_PositionIndependent(t *testing.T) {
	s := evStarted()
	s.ID = 1
	// WFTC₁(2) 之后: comp(3, 外部) 插在 sched(4, 命令) 之前（gormstore 并发交错形态）
	events := []history.Event{s, evWFTC(2), evComp(3, 99), evSched(4, "#a1"), evSignal(5, "sig")}
	// sched(4) 位置无关归批 1；comp(3)/signal(5) 归批 2
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 4}, {3, 5}})
}

// Edge Case: UpdateAccepted 是外部事件（accept-on-request），开新批，不得归命令侧。
func TestSplitBatches_UpdateAccepted_IsExternal(t *testing.T) {
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evWFTC(2), evSched(3, "#a1"), evUpdateAccepted(4, "u"), evComp(5, 3)}
	// 批1 = [1,2,3]；批2 = [UpdateAccepted(4), comp(5)]
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 3}, {4, 5}})
}

// Edge Case: 超时 WFT 不形成批界（未产生决策）。
func TestSplitBatches_TimedOutWFT_NoBoundary(t *testing.T) {
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evWFTTimedOut(2), evSignal(3, "sig")}
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 3}})
}

// Edge Case: 历史以 WFTC+命令 结尾 → 空 live 末批（每次 Execute 隐含一个进行中的 live WFT，
// 使「末批 = live」判定对任意历史成立；R类护栏在 replay 批的语义因此保持）。
func TestSplitBatches_TrailingDecision_EmptyLiveBatch(t *testing.T) {
	s := evStarted()
	s.ID = 1
	events := []history.Event{s, evWFTC(2), evSched(3, "#a1")}
	assertBatches(t, executor.SplitBatches(events), [][]int64{{1, 2, 3}, {}})
}
