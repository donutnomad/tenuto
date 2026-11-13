package engine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// computeBlockedInfo 派生 blocked 状态测试
// ============================================
// 测试策略（纯读、单趟尾扫，复用 workflowBlockedOnFailedTask 判定）:
// - B1 Happy Path: 尾部 WFT Failed 无 open → Blocked=true，Cause/Reason 取该事件
// - B2 Happy Path: 连续 3 个 WFT Failed → ConsecutiveFailures=3
// - B3 Edge Case:  Failed×2 后 Completed → Blocked=false、streak=0
// - B4 Happy Path: 正常 running（有 open WFT / 无 Failed 尾）→ Blocked=false（不误报）
// - B5 Edge Case:  Since = 连续失败段第一个 Failed 事件时间
// - B6 Edge Case:  Failed 后紧跟新 Scheduled（有 open WFT = 正在恢复）→ Blocked=false
// ============================================

// wftScheduled 构造一个带显式 ID 的 WFT Scheduled 事件（用于 open-WFT 判定）。
func wftScheduled(id int64, ts time.Time) history.Event {
	return history.Event{ID: id, Timestamp: ts, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{Attempt: 1}}
}

func wftStarted(id, scheduledID int64, ts time.Time) history.Event {
	return history.Event{ID: id, Timestamp: ts, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: scheduledID}}
}

func wftCompleted(id, scheduledID int64, ts time.Time) history.Event {
	return history.Event{ID: id, Timestamp: ts, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: scheduledID}}
}

func wftFailed(id, scheduledID int64, cause, reason string, ts time.Time) history.Event {
	return history.Event{ID: id, Timestamp: ts, WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: scheduledID, Cause: cause, Reason: reason}}
}

// B1 Happy Path: 末个 WFT lifecycle 是 Failed 且无 open WFT → Blocked，取该事件 cause/reason。
func TestBlockedInfo_TailIsFailedNoOpenWFT_Blocked(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftStarted(2, 1, base.Add(time.Second)),
		wftFailed(3, 1, "non_deterministic_error", "marker mismatch", base.Add(2*time.Second)),
	}
	info := computeBlockedInfo(events)
	if !info.Blocked {
		t.Fatalf("expected Blocked=true when tail WFT is Failed with no open WFT")
	}
	if info.Cause != "non_deterministic_error" {
		t.Fatalf("expected cause non_deterministic_error, got %q", info.Cause)
	}
	if info.Reason != "marker mismatch" {
		t.Fatalf("expected reason 'marker mismatch', got %q", info.Reason)
	}
	if info.ConsecutiveFailures != 1 {
		t.Fatalf("expected ConsecutiveFailures=1, got %d", info.ConsecutiveFailures)
	}
}

// B2 Happy Path: 连续 3 个 WFT Failed → ConsecutiveFailures=3。
func TestBlockedInfo_Streak_Counts(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "e1", base.Add(time.Second)),
		wftScheduled(3, base.Add(2*time.Second)),
		wftFailed(4, 3, "non_deterministic_error", "e2", base.Add(3*time.Second)),
		wftScheduled(5, base.Add(4*time.Second)),
		wftFailed(6, 5, "non_deterministic_error", "e3", base.Add(5*time.Second)),
	}
	info := computeBlockedInfo(events)
	if !info.Blocked {
		t.Fatalf("expected Blocked=true")
	}
	if info.ConsecutiveFailures != 3 {
		t.Fatalf("expected ConsecutiveFailures=3, got %d", info.ConsecutiveFailures)
	}
}

// B3 Edge Case: Failed×2 后一个 Completed（无后续 open）→ 未卡死、streak=0。
func TestBlockedInfo_WFTCompletedBreaksStreak(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "e1", base.Add(time.Second)),
		wftScheduled(3, base.Add(2*time.Second)),
		wftFailed(4, 3, "non_deterministic_error", "e2", base.Add(3*time.Second)),
		wftScheduled(5, base.Add(4*time.Second)),
		wftCompleted(6, 5, base.Add(5*time.Second)),
	}
	info := computeBlockedInfo(events)
	if info.Blocked {
		t.Fatalf("expected Blocked=false after a Completed WFT closes the streak")
	}
	if info.ConsecutiveFailures != 0 {
		t.Fatalf("expected ConsecutiveFailures=0, got %d", info.ConsecutiveFailures)
	}
}

// B4 Happy Path: 正常 running（无 Failed 尾，末个 WFT Completed）→ 不误报 Blocked。
func TestBlockedInfo_RunningNotBlocked(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftStarted(2, 1, base.Add(time.Second)),
		wftCompleted(3, 1, base.Add(2*time.Second)),
	}
	info := computeBlockedInfo(events)
	if info.Blocked {
		t.Fatalf("expected Blocked=false for healthy running run")
	}
}

// B5 Edge Case: Since = 连续失败段第一个 Failed 事件的时间戳。
func TestBlockedInfo_Since_FirstOfStreak(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	firstFailTs := base.Add(time.Second)
	events := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "e1", firstFailTs),
		wftScheduled(3, base.Add(2*time.Second)),
		wftFailed(4, 3, "non_deterministic_error", "e2", base.Add(3*time.Second)),
	}
	info := computeBlockedInfo(events)
	if info.Since == nil || !info.Since.Equal(firstFailTs) {
		t.Fatalf("expected Since=%v (first of streak), got %v", firstFailTs, info.Since)
	}
}

// B6 Edge Case（误报防线专项）: WFT Failed 后紧跟新 Scheduled（有 open WFT = 正在恢复重试）→ Blocked=false。
func TestBlockedInfo_FailedThenRescheduled_NotBlocked(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "e1", base.Add(time.Second)),
		// 新 Scheduled（ID=3）未 close → open WFT，说明正在恢复重试，不算卡死。
		wftScheduled(3, base.Add(2*time.Second)),
	}
	info := computeBlockedInfo(events)
	if info.Blocked {
		t.Fatalf("expected Blocked=false when a new WFT is scheduled (recovering, open WFT present)")
	}
}

// Edge Case（wire 卫生，代码审查发现 1）: 未 blocked 的 BlockedInfo 序列化不得泄漏 since 零值时间。
// Since 用 *time.Time + omitempty（time.Time 是结构体，omitempty 对其无效，指针才生效）。
func TestBlockedInfo_NotBlocked_JSONOmitsSince(t *testing.T) {
	data, err := json.Marshal(execution.BlockedInfo{Blocked: false})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "since") {
		t.Fatalf("未 blocked 的响应不应含 since 字段，got %s", data)
	}
}
