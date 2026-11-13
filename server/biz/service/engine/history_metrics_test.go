package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

func TestCheckHistoryBudget_ExactBoundaries(t *testing.T) {
	// Edge Case: 普通事件可用区间扣除 close reserve，服务端 close 可消费完整 hard budget。
	limits := history.HistoryLimits{WarningEvents: 5, WarningBytes: 50, HardEvents: 10, HardBytes: 100, ReservedCloseEvents: 2, ReservedCloseBytes: 20}
	if err := history.CheckHistoryBudget(history.HistoryMetrics{EventCount: 4, SizeBytes: 40}, []int64{10}, limits, false); err != nil {
		t.Fatal(err)
	}
	err := history.CheckHistoryBudget(history.HistoryMetrics{EventCount: 8, SizeBytes: 80}, []int64{1}, limits, false)
	var exceeded *history.HistoryLimitExceededError
	if !errors.As(err, &exceeded) || exceeded.Metric != history.ContinueAsNewReasonHistoryEventCount {
		t.Fatalf("expected event limit error, got %v", err)
	}
	if err := history.CheckHistoryBudget(history.HistoryMetrics{EventCount: 8, SizeBytes: 80}, []int64{20, 0}, limits, true); err != nil {
		t.Fatalf("server close should consume reserve: %v", err)
	}
}

func TestApplyEventToExecutionState_ParallelWaitsAndBlockedLifecycle(t *testing.T) {
	// Happy Path: 并发等待点关闭一条后保留另一条，WFT 失败段形成精确 blocked projection。
	run := &execution.Run{ID: "run-1", Status: history.StatusRunning}
	state := execution.NewWorkflowExecutionState(run)
	activityA := history.Event{ID: 1, Timestamp: time.Unix(1, 0), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "a"}}
	activityB := history.Event{ID: 2, Timestamp: time.Unix(2, 0), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "b"}}
	execution.ApplyEventToExecutionState(state, activityA, 11)
	execution.ApplyEventToExecutionState(state, activityB, 13)
	execution.ApplyEventToExecutionState(state, history.Event{ID: 3, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2}}, 17)
	if state.CurrentWaiting.ActivityID != "a" || state.HistoryEventCount != 3 || state.HistorySizeBytes != 41 {
		t.Fatalf("unexpected wait projection: %+v", state)
	}
	failedAt := time.Unix(4, 0)
	execution.ApplyEventToExecutionState(state, history.Event{ID: 4, Timestamp: failedAt, WorkflowTaskFailed: &history.WorkflowTaskFailedEvent{ScheduledEventID: 9, Cause: "panic", Reason: "boom"}}, 19)
	if !state.Blocked.Blocked || state.Blocked.ConsecutiveFailures != 1 || state.Blocked.Since == nil || !state.Blocked.Since.Equal(failedAt) {
		t.Fatalf("unexpected blocked projection: %+v", state.Blocked)
	}
	execution.ApplyEventToExecutionState(state, history.Event{ID: 5, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}}, 23)
	if state.Blocked.Blocked || state.Blocked.ConsecutiveFailures != 1 {
		t.Fatalf("scheduled WFT should clear active block and retain streak: %+v", state.Blocked)
	}
}

func TestHistoryPageToken_TamperExpiryAndBinding(t *testing.T) {
	// Error Case: token 签名与过期时间形成可验证的 opaque cursor。
	secret := []byte("01234567890123456789012345678901")
	now := time.Unix(100, 0)
	token, err := execution.EncodeHistoryPageToken(secret, execution.HistoryPageCursor{Version: 1, RunID: "run-1", Direction: execution.HistoryDirectionAscending, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := execution.DecodeHistoryPageToken(secret, token, now)
	if err != nil || cursor.RunID != "run-1" {
		t.Fatalf("decode cursor: %+v, %v", cursor, err)
	}
	if _, err := execution.DecodeHistoryPageToken(secret, token+"x", now); !errors.Is(err, execution.ErrInvalidHistoryPageToken) {
		t.Fatalf("tampered token error = %v", err)
	}
	if _, err := execution.DecodeHistoryPageToken(secret, token, now.Add(2*time.Minute)); !errors.Is(err, execution.ErrExpiredHistoryPageToken) {
		t.Fatalf("expired token error = %v", err)
	}
}
