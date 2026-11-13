package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// Happy Path: 哨兵值必须对齐 Temporal common/constants.go：
// EmptyEventID=0（未 started），TransientEventID=-124（transient started 未写 history）。
// 这两个必须是两个不同的值——绝不能复用同一个 -1，否则 transient 补写会错乱。
func TestActivityStateSentinels(t *testing.T) {
	if execution.EmptyEventID != 0 {
		t.Fatalf("EmptyEventID must be 0 (Temporal alignment), got %d", execution.EmptyEventID)
	}
	if execution.TransientEventID != -124 {
		t.Fatalf("TransientEventID must be -124 (Temporal alignment), got %d", execution.TransientEventID)
	}
	if execution.EmptyEventID == execution.TransientEventID {
		t.Fatal("EmptyEventID and TransientEventID must be distinct sentinels")
	}
}

// Happy Path: 新建一个 scheduled ActivityState，约定初始值：
// Attempt=1, Stamp=1, StartedEventID=EmptyEventID, Status=Scheduled。
func TestNewActivityStateDefaults(t *testing.T) {
	st := &execution.ActivityState{
		RunID:            "run-1",
		ScheduledEventID: 10,
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   execution.EmptyEventID,
		Status:           execution.ActivityStatusEnums.Scheduled,
		ScheduledTime:    time.Now(),
	}
	if st.Status != execution.ActivityStatusEnums.Scheduled {
		t.Fatalf("status = %q", st.Status)
	}
	if st.StartedEventID != execution.EmptyEventID {
		t.Fatalf("startedEventID = %d, want EmptyEventID(0)", st.StartedEventID)
	}
	if !st.IsScheduled() || st.IsTerminal() {
		t.Fatalf("predicate mismatch: scheduled=%v terminal=%v", st.IsScheduled(), st.IsTerminal())
	}
}

// Edge Case（🔴-2）: 同 schedID+attempt 但不同 stamp（reset 前后）的 started 事件
// 必须是不同 dedupe key——否则 post-reset 事件撞 pre-reset 键、不写 history。
func TestDedupeKeyIncludesStampForActivityEvents(t *testing.T) {
	pre := history.NewActivityStartedEvent(history.ActivityStartedEvent{ScheduledEventID: 10, Attempt: 1, Stamp: 1})
	post := history.NewActivityStartedEvent(history.ActivityStartedEvent{ScheduledEventID: 10, Attempt: 1, Stamp: 3})
	kPre, _ := history.DedupeKey(pre)
	kPost, _ := history.DedupeKey(post)
	if kPre == kPost {
		t.Fatalf("reset generation collision: pre=%q post=%q must differ", kPre, kPost)
	}
	// attemptFailed 同理
	fPre := history.NewActivityAttemptFailedEvent(history.ActivityAttemptFailedEvent{ScheduledEventID: 10, Attempt: 1, Stamp: 1})
	fPost := history.NewActivityAttemptFailedEvent(history.ActivityAttemptFailedEvent{ScheduledEventID: 10, Attempt: 1, Stamp: 3})
	kFPre, _ := history.DedupeKey(fPre)
	kFPost, _ := history.DedupeKey(fPost)
	if kFPre == kFPost {
		t.Fatalf("attempt-failed reset generation collision: pre=%q post=%q must differ", kFPre, kFPost)
	}
}
