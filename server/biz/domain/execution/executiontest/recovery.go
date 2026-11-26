package executiontest

import (
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RecoveryRunIDsContract 断言 ListRecoveryRunIDs 只返回有到期或待处理事项的 Running Run：截止时间、timer、
// Started Activity 的超时都按 dueBefore 判定，Scheduled Activity、open WFT、待调度 WFT、blocked 总是返回，未到期的、已结束的事项与已结束的 Run 不返回；心跳把 Activity 的
// 超时截止时间推后之后它离开候选。
func RecoveryRunIDsContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	now := contractNow.Add(time.Hour)
	past, future := now.Add(-time.Second), now.Add(time.Hour)

	// newRun 新建 Running 的 Run，deadline 与 runDeadline 是执行截止时间与 Run 截止时间（nil 表示没有）。
	newRun := func(runID string, deadline, runDeadline *time.Time) {
		t.Helper()
		run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowConfigVersion: "v1", CreatedAt: contractNow, DeadlineAt: deadline})
		run.RunDeadlineAt = runDeadline
		createRun(t, manager, run)
	}
	timer := func(runID string, scheduledEventID int64, deadline time.Time, status execution.TimerStatus) {
		t.Helper()
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			return tx.UpsertTimerState(&execution.TimerState{
				RunID: runID, ScheduledEventID: scheduledEventID, TimerID: "t", Deadline: deadline, Status: status,
				Kind: execution.TimerKindEnums.UserTimer, CreatedAt: contractNow, UpdatedAt: contractNow,
			})
		})
	}
	// startedActivity 的 attempt 在 startedAt 开始，StartToClose 1h，心跳超时 heartbeat（0 表示没有）。
	startedActivity := func(runID string, startedAt time.Time, heartbeat time.Duration) {
		t.Helper()
		inTx(t, manager, func(tx execution.ExecutionTx) error {
			return tx.UpsertActivityState(&execution.ActivityState{
				RunID: runID, ScheduledEventID: 5, ActivityID: "a", ActivityType: "A", TaskQueue: "q",
				Attempt: 1, Stamp: 1, StartedEventID: execution.TransientEventID, Status: execution.ActivityStatusEnums.Started,
				ScheduledTime: startedAt, StartedTime: &startedAt, WorkerIdentity: "w",
				StartToCloseTimeoutMs: time.Hour.Milliseconds(), HeartbeatTimeoutMs: heartbeat.Milliseconds(),
				CreatedAt: startedAt, UpdatedAt: startedAt,
			})
		})
	}

	// 没有到期事项：timer 与 Activity 超时都在 dueBefore 之后，已触发的 timer 早已过期也不算。
	newRun("run-idle", nil, &future)
	timer("run-idle", 3, future, execution.TimerStatusEnums.Scheduled)
	timer("run-idle", 4, past, execution.TimerStatusEnums.Fired)
	startedActivity("run-idle", now, 0)

	newRun("run-deadline", &past, nil)
	newRun("run-run-deadline", nil, &past)
	newRun("run-timer", nil, nil)
	timer("run-timer", 3, past, execution.TimerStatusEnums.Scheduled)
	newRun("run-activity-scheduled", nil, nil)
	inTx(t, manager, func(tx execution.ExecutionTx) error {
		return tx.UpsertActivityState(&execution.ActivityState{
			RunID: "run-activity-scheduled", ScheduledEventID: 5, ActivityID: "a", ActivityType: "A", TaskQueue: "q",
			Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now,
			NextAttemptAt: &future, CreatedAt: now, UpdatedAt: now,
		})
	})
	newRun("run-activity-timeout", nil, nil)
	startedActivity("run-activity-timeout", now.Add(-2*time.Hour), 0)
	newRun("run-activity-heartbeat", nil, nil)
	startedActivity("run-activity-heartbeat", now.Add(-time.Minute), 30*time.Second)
	newRun("run-open-wft", nil, nil)
	if _, _, err := manager.AppendWorkflowTaskScheduled("run-open-wft", "run-open-wft", "run-open-wft", history.NewWorkflowTaskScheduledEvent(1000)); err != nil {
		t.Fatal(err)
	}
	// 信号已提交、WFT 还没调度（调度失败的窗口）：NeedsWorkflowTask。
	newRun("run-needs-wft", nil, nil)
	if _, err := manager.Append("run-needs-wft", "run-needs-wft", "run-needs-wft", history.NewWorkflowSignaledEvent("s", nil)); err != nil {
		t.Fatal(err)
	}
	newRun("run-blocked", nil, nil)
	scheduledID, _, err := manager.AppendWorkflowTaskScheduled("run-blocked", "run-blocked", "run-blocked", history.NewWorkflowTaskScheduledEvent(1000))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.AppendWorkflowTaskClosed("run-blocked", "run-blocked", "run-blocked", history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: scheduledID, Cause: "workflow_task_failed", Reason: "panic",
	})); err != nil {
		t.Fatal(err)
	}
	// 已结束的 Run 即使留有到期的 timer 也不返回。
	newRun("run-closed", nil, &past)
	timer("run-closed", 3, past, execution.TimerStatusEnums.Scheduled)
	closed, err := manager.Get("run-closed")
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, manager, func(tx execution.ExecutionTx) error {
		if _, err := tx.Append("run-closed", "run-closed", "run-closed", history.NewWorkflowCompletedEvent(nil)); err != nil {
			return err
		}
		if err := closed.MarkTerminal(history.StatusCompleted, nil); err != nil {
			return err
		}
		return tx.UpdateRun(closed)
	})

	requireRecoveryRunIDs := func(dueBefore time.Time, want ...string) {
		t.Helper()
		got, err := manager.ListRecoveryRunIDs(dueBefore)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Sort(want); !slices.Equal(got, want) {
			t.Fatalf("ListRecoveryRunIDs(%s):\n got %v\nwant %v", dueBefore, got, want)
		}
	}
	requireRecoveryRunIDs(now, "run-activity-heartbeat", "run-activity-scheduled", "run-activity-timeout",
		"run-blocked", "run-deadline", "run-needs-wft", "run-open-wft", "run-run-deadline", "run-timer")
	// dueBefore 推后两小时：run-idle 的 Run 截止时间、timer 与 Activity 超时都进入窗口。
	requireRecoveryRunIDs(future.Add(time.Hour), "run-activity-heartbeat", "run-activity-scheduled", "run-activity-timeout",
		"run-blocked", "run-deadline", "run-idle", "run-needs-wft", "run-open-wft", "run-run-deadline", "run-timer")

	// 心跳把心跳超时推到 now+30s 之后，最早的截止时间变成它，Run 离开 now 的候选。
	inTx(t, manager, func(tx execution.ExecutionTx) error {
		state, err := execution.RecordActivityHeartbeat(tx, execution.ActivityTransition{
			RunID: "run-activity-heartbeat", TreeID: "run-activity-heartbeat", BranchID: "run-activity-heartbeat",
			ScheduledEventID: 5, Attempt: 1, Stamp: 1, Now: now,
		}, map[string]any{"progress": 1})
		if err == nil && state == nil {
			t.Fatal("heartbeat was not recorded")
		}
		return err
	})
	requireRecoveryRunIDs(now, "run-activity-scheduled", "run-activity-timeout",
		"run-blocked", "run-deadline", "run-needs-wft", "run-open-wft", "run-run-deadline", "run-timer")
}

// RunScanWithoutTotalContract 断言 SkipTotal 不计算 Total，按游标翻页仍覆盖每一个 Run。
func RunScanWithoutTotalContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	want := seedRunsSharingCreatedAt(t, manager)
	filter := execution.StoreListFilter{Statuses: []history.Status{history.StatusRunning}, Limit: 5, SkipTotal: true}
	var got []string
	for {
		page, err := manager.List(filter)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 0 {
			t.Fatalf("SkipTotal must leave Total at 0, got %d", page.Total)
		}
		if len(page.Runs) == 0 {
			break
		}
		for _, run := range page.Runs {
			got = append(got, run.ID)
		}
		cursor := execution.CursorOf(page.Runs[len(page.Runs)-1])
		filter.After = &cursor
	}
	if !slices.Equal(got, want) {
		t.Fatalf("cursor scan:\n got %v\nwant %v", got, want)
	}
}
