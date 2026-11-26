package interpreter

// 扇出的 Activity 在 Workflow Task 进行期间陆续完成：Server 照常提交非终态决策，进行期间到达的完成事件排在
// WorkflowTaskCompleted 之前（ID 大于 SeenThroughEventID），留给下一个 Workflow Task。重放按 SeenThroughEventID
// 分批，与 live 看到的事件一致：sticky 会话、非 sticky 全量路径与冷重放产出同样的命令。

import (
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// 每个 Activity 完成后立即调度一个 Follow，Follow 的输入记下完成顺序。哪几个完成在同一个 Workflow Task 里被看到，
// 决定了每个 Workflow Task 产出哪些 Follow。
const fanOutSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (string, error) {
	fa := workflow.ExecuteActivity(ctx, activities.A, input)
	fb := workflow.ExecuteActivity(ctx, activities.B, input)
	fc := workflow.ExecuteActivity(ctx, activities.C, input)
	order := ""
	var follows []workflow.ActivityFuture
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(ctx, fa, func(f workflow.ActivityFuture) {
		order = order + "a"
		follows = append(follows, workflow.ExecuteActivity(ctx, activities.Follow, map[string]any{"after": order}))
	})
	sel.AddFuture(ctx, fb, func(f workflow.ActivityFuture) {
		order = order + "b"
		follows = append(follows, workflow.ExecuteActivity(ctx, activities.Follow, map[string]any{"after": order}))
	})
	sel.AddFuture(ctx, fc, func(f workflow.ActivityFuture) {
		order = order + "c"
		follows = append(follows, workflow.ExecuteActivity(ctx, activities.Follow, map[string]any{"after": order}))
	})
	for i := 0; i < 3; i++ {
		sel.Select(ctx)
	}
	for _, f := range follows {
		var out map[string]any
		if err := f.Get(ctx, &out); err != nil {
			return "", err
		}
	}
	return order, nil
}
`

type fanOutHistory struct {
	events []history.Event
}

func (h *fanOutHistory) add(event history.Event) history.Event {
	event.ID = int64(len(h.events) + 1)
	event.Timestamp = nowTestStart.Add(time.Duration(event.ID) * time.Millisecond)
	h.events = append(h.events, event)
	return event
}

func (h *fanOutHistory) since(id int64) []history.Event {
	return append([]history.Event(nil), h.events[id:]...)
}

func (h *fanOutHistory) last() int64 { return int64(len(h.events)) }

// commit 追加 WorkflowTaskCompleted（看到 seen 为止）与命令事件，返回这些提交事件。
func (h *fanOutHistory) commit(scheduledEventID, seen int64, commands []history.Command) []history.Event {
	completed := history.NewWorkflowTaskCompletedEvent(scheduledEventID, "worker-a")
	completed.WorkflowTaskCompleted.SeenThroughEventID = seen
	committed := []history.Event{h.add(completed)}
	for _, command := range commands {
		committed = append(committed, h.add(history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
			ActivityID: command.ActivityID, ActivityType: command.ActivityType, Input: command.Input,
		})))
	}
	return committed
}

func (h *fanOutHistory) scheduledEventID(activityID string) int64 {
	for _, event := range h.events {
		if event.ActivityScheduled != nil && event.ActivityScheduled.ActivityID == activityID {
			return event.ID
		}
	}
	return 0
}

func (h *fanOutHistory) complete(activityID string) {
	h.add(history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: activityID, ScheduledEventID: h.scheduledEventID(activityID), Output: map[string]any{}}))
}

// startTask 追加 WorkflowTaskScheduled 与 WorkflowTaskStarted，返回调度事件 ID。
func (h *fanOutHistory) startTask() int64 {
	scheduled := h.add(history.NewWorkflowTaskScheduledEvent(0))
	h.add(history.NewWorkflowTaskStartedEvent(scheduled.ID, "worker-a", "", 0))
	return scheduled.ID
}

func followInputs(t *testing.T, commands []history.Command) []string {
	t.Helper()
	var after []string
	for _, command := range commands {
		if command.Type != history.CmdScheduleActivity || command.ActivityType != "activities.Follow" {
			t.Fatalf("unexpected command %+v", command)
		}
		after = append(after, command.Input["after"].(string))
	}
	return after
}

func TestFanOutCompletionsDuringWorkflowTaskReplayLikeLive(t *testing.T) {
	nodes := compileSource(t, fanOutSrc)
	session := restoredNowSession(t, nodes)
	h := &fanOutHistory{}
	h.add(history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0))
	wft := h.startTask()

	// 第一个 Workflow Task 调度 A、B、C。
	first := activateNowSession(t, session, "wft1", h.since(0))
	if len(first.Commands) != 3 {
		t.Fatalf("first task commands = %+v", first.Commands)
	}
	seen := h.last()
	commitNowSession(t, session, first, h.commit(wft, seen, first.Commands))
	activities := map[string]string{}
	for _, command := range first.Commands {
		activities[command.ActivityType] = command.ActivityID
	}

	// A 完成，第二个 Workflow Task 开始；它进行期间 B 完成（Worker 没有看到）。
	h.complete(activities["activities.A"])
	wft = h.startTask()
	applied := session.AppliedThroughEventID()
	second := activateNowSession(t, session, "wft2", h.since(applied))
	if got := followInputs(t, second.Commands); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("second task follows = %v, want [a]", got)
	}
	nonSticky, err := NewWorkflowExecutor(nodes, h.events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := followInputs(t, nonSticky); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("full-history second task follows = %v, want [a]", got)
	}
	seen = h.last()
	h.complete(activities["activities.B"])
	commitNowSession(t, session, second, h.commit(wft, seen, second.Commands))
	if session.AppliedThroughEventID() != seen {
		t.Fatalf("session cursor = %d, want %d (B's completion is still unseen)", session.AppliedThroughEventID(), seen)
	}

	// C 完成，第三个 Workflow Task 看到 B 与 C。
	h.complete(activities["activities.C"])
	wft = h.startTask()
	third := activateNowSession(t, session, "wft3", h.since(session.AppliedThroughEventID()))
	want := []string{"ab", "abc"}
	if got := followInputs(t, third.Commands); !slices.Equal(got, want) {
		t.Fatalf("sticky third task follows = %v, want %v", got, want)
	}
	// 非 sticky Worker 从完整 History 重放前两个任务，第三个任务看到同样的事件。
	nonSticky, err = NewWorkflowExecutor(nodes, h.events).Execute()
	if err != nil {
		t.Fatalf("full-history third task: %v", err)
	}
	if got := followInputs(t, nonSticky); !slices.Equal(got, want) {
		t.Fatalf("full-history third task follows = %v, want %v", got, want)
	}
	seen = h.last()
	commitNowSession(t, session, third, h.commit(wft, seen, third.Commands))

	// Follow 全部完成，Workflow 结束；冷重放整份 History 与 sticky 会话结果相同。
	for _, event := range slices.Clone(h.events) {
		if scheduled := event.ActivityScheduled; scheduled != nil && scheduled.ActivityType == "activities.Follow" {
			h.complete(scheduled.ActivityID)
		}
	}
	h.startTask()
	last := activateNowSession(t, session, "wft4", h.since(session.AppliedThroughEventID()))
	cold, err := NewWorkflowExecutor(nodes, h.events).Execute()
	if err != nil {
		t.Fatalf("cold replay: %v", err)
	}
	if len(last.Commands) != 1 || len(cold) != 1 || last.Commands[0].Type != history.CmdCompleteWorkflow || cold[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("final commands sticky=%+v cold=%+v", last.Commands, cold)
	}
	if last.Commands[0].Output["value"] != "abc" || cold[0].Output["value"] != "abc" {
		t.Fatalf("outputs sticky=%v cold=%v, want abc", last.Commands[0].Output, cold[0].Output)
	}
}
