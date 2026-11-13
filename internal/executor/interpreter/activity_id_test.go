package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// ActivityOptions.ActivityID：缺省值、显式值、运行期计算值、关闭后复用与重放比对。

func activityIDs(commands []history.Command) []string {
	var ids []string
	for _, command := range findScheduleCmds(commands) {
		ids = append(ids, command.ActivityID)
	}
	return ids
}

func assertActivityIDs(t *testing.T, commands []history.Command, want ...string) {
	t.Helper()
	got := activityIDs(commands)
	if len(got) != len(want) {
		t.Fatalf("ActivityIDs = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ActivityIDs = %q, want %q", got, want)
		}
	}
}

// 没有指定 ActivityID 时取调用在协程内的位置；指定了的调用不改变其他调用的缺省值。
func TestActivityID_DefaultAndExplicit(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	f1 := workflow.ExecuteActivity(ctx, activities.A, input, opts)
	f2 := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{ActivityID: "charge", StartToCloseTimeout: time.Minute})
	f3 := workflow.ExecuteActivity(ctx, activities.A, input, opts)
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		workflow.ExecuteActivity(ctx, activities.A, input, opts)
	})
	var out map[string]any
	_ = f1.Get(ctx, &out)
	_ = f2.Get(ctx, &out)
	_ = f3.Get(ctx, &out)
	wg.Wait(ctx)
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	commands, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	assertActivityIDs(t, commands, "#a1", "charge", "#a3", "/0#a1")
}

// ActivityID 可以是运行期计算的值：循环里 fmt.Sprintf 生成的 ID 原样进入调度命令。
func TestActivityID_ComputedWithSprintf(t *testing.T) {
	const src = `package flows
import (
	"fmt"
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var futures []workflow.ActivityFuture
	for i := 0; i < 3; i++ {
		futures = append(futures, workflow.ExecuteActivity(ctx, activities.Charge, input, workflow.ActivityOptions{
			ActivityID:          fmt.Sprintf("charge-%d", i),
			StartToCloseTimeout: time.Minute,
		}))
	}
	for _, f := range futures {
		var out map[string]any
		if err := f.Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	commands, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	assertActivityIDs(t, commands, "charge-0", "charge-1", "charge-2")
}

// 同一 ActivityID 在前一个关闭后再次调度：重放按历史顺序把两个调度事件分别交给两次调用，
// 第二次调用拿到的是第二个 Activity 的结果。
func TestActivityID_ReuseAfterCloseBindsInOrder(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	opts := workflow.ActivityOptions{ActivityID: "poll", StartToCloseTimeout: time.Minute}
	var out map[string]any
	for i := 0; i < 2; i++ {
		if err := workflow.ExecuteActivity(ctx, activities.Poll, input, opts).Get(ctx, &out); err != nil {
			return err
		}
	}
	var done map[string]any
	return workflow.ExecuteActivity(ctx, activities.Report, out, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &done)
}
`
	now := time.Now()
	events := []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0),
		wftc(2),
		{ID: 3, Timestamp: now, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "poll", ActivityType: "activities.Poll"}},
		{ID: 4, Timestamp: now, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"tag": "first"}}},
		wftc(5),
		{ID: 6, Timestamp: now, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "poll", ActivityType: "activities.Poll"}},
		{ID: 7, Timestamp: now, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 6, Output: map[string]any{"tag": "second"}}},
	}
	commands, err := probeExecFix(t, src, events)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	scheduled := findScheduleCmds(commands)
	if len(scheduled) != 1 || scheduled[0].ActivityType != "activities.Report" || scheduled[0].ActivityID != "#a3" {
		t.Fatalf("live commands = %+v, want one Report activity #a3", commands)
	}
	if got := scheduled[0].Input["tag"]; got != "second" {
		t.Fatalf("Report input tag = %v, want the second Poll result", got)
	}
}

// 重放时同一位置的调用算出的 ActivityID 与历史不同 → 非确定性错误，与换了 Activity 类型相同。
func TestActivityID_ChangedOnReplayIsNonDeterministic(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		ActivityID:          "charge-v2",
		StartToCloseTimeout: time.Minute,
	}).Get(ctx, &out)
}
`
	now := time.Now()
	events := []history.Event{
		history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0),
		wftc(2),
		{ID: 3, Timestamp: now, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "charge-v1", ActivityType: "activities.A"}},
		{ID: 4, Timestamp: now, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"ok": true}}},
		wftc(5),
	}
	_, err := probeExecFix(t, src, events)
	assertNonDeterministic(t, err, "history event is ActivityScheduled(id=charge-v1, type=activities.A), replay command is ScheduleActivity(id=charge-v2, type=activities.A)")
}
