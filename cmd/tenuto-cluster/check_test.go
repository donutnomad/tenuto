package main

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/protocol/history"
)

// validHistory 是一个合法的 Run：一个 Activity、一个 Timer、一个子 Workflow，两个 Workflow Task。
func validHistory() []history.Event {
	return []history.Event{
		{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
		{ID: 2, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 3, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 2}},
		{ID: 4, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 2}},
		{ID: 5, ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}},
		{ID: 6, TimerScheduled: &history.TimerScheduledEvent{TimerID: "#t1"}},
		{ID: 7, ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", ChildRunID: "child-run"}},
		{ID: 8, ActivityTimedOut: &history.ActivityTimedOutEvent{ScheduledEventID: 5, Attempt: 1}},
		{ID: 9, ActivityAttemptFailed: &history.ActivityAttemptFailedEvent{ScheduledEventID: 5, Attempt: 1}},
		{ID: 10, ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 5, Attempt: 2}},
		{ID: 11, ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 5, Attempt: 2}},
		{ID: 12, TimerFired: &history.TimerFiredEvent{ScheduledEventID: 6}},
		{ID: 13, ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ScheduledEventID: 7, Output: map[string]any{"total": 1}}},
		{ID: 14, WorkflowSignaled: &history.WorkflowSignaledEvent{Name: workload.SignalName}},
		{ID: 15, WorkflowTaskScheduled: &history.WorkflowTaskScheduledEvent{}},
		{ID: 16, WorkflowTaskStarted: &history.WorkflowTaskStartedEvent{ScheduledEventID: 15}},
		{ID: 17, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{ScheduledEventID: 15}},
		{ID: 18, WorkflowCompleted: &history.WorkflowCompletedEvent{}},
	}
}

func TestCheckHistoryAcceptsValidHistory(t *testing.T) {
	if problems := checkHistory(validHistory()); len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	facts := extractFacts(validHistory())
	if len(facts.ScheduledActivities) != 1 || facts.CompletedAttempts["#a1"] != 2 || facts.Signals != 1 ||
		facts.TimersScheduled != 1 || facts.TimersFired != 1 || len(facts.Children) != 1 ||
		facts.Children[0].ChildRunID != "child-run" || facts.Children[0].Output["total"] != 1 || facts.Closing.WorkflowCompleted == nil {
		t.Fatalf("facts = %+v", facts)
	}
}

func TestCheckHistoryFindsViolations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]history.Event) []history.Event
		want   string
	}{
		{"gap in event IDs", func(e []history.Event) []history.Event { e[9].ID = 30; return e }, "not contiguous"},
		{"two activity terminals", func(e []history.Event) []history.Event {
			return insertBefore(e, 17, history.Event{ActivityFailed: &history.ActivityFailedEvent{ScheduledEventID: 5}})
		}, "2 terminal events"},
		{"timer fires twice", func(e []history.Event) []history.Event {
			return insertBefore(e, 17, history.Event{TimerFired: &history.TimerFiredEvent{ScheduledEventID: 6}})
		}, "fired or cancelled 2 times"},
		{"child closes twice", func(e []history.Event) []history.Event {
			return insertBefore(e, 17, history.Event{ChildWorkflowFailed: &history.ChildWorkflowFailedEvent{ScheduledEventID: 7}})
		}, "closed 2 times"},
		{"duplicate open ActivityID", func(e []history.Event) []history.Event {
			return insertBefore(e, 7, history.Event{ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1"}})
		}, "still open"},
		{"reference to a missing scheduled event", func(e []history.Event) []history.Event {
			e[10].ActivityCompleted.ScheduledEventID = 99
			return e
		}, "references ActivityScheduled event 99"},
		{"two closing events", func(e []history.Event) []history.Event {
			return insertBefore(e, 17, history.Event{WorkflowFailed: &history.WorkflowFailedEvent{}})
		}, "2 closing events"},
		{"no closing event", func(e []history.Event) []history.Event { return e[:len(e)-1] }, "0 closing events"},
		{"workflow task closed twice", func(e []history.Event) []history.Event {
			return insertBefore(e, 17, history.Event{WorkflowTaskTimedOut: &history.WorkflowTaskTimedOutEvent{ScheduledEventID: 15}})
		}, "closed 2 times"},
		{"activity attempt started twice", func(e []history.Event) []history.Event {
			return insertBefore(e, 10, history.Event{ActivityStarted: &history.ActivityStartedEvent{ScheduledEventID: 5, Attempt: 2}})
		}, "attempt 2 started 2 times"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			problems := checkHistory(tc.mutate(validHistory()))
			if !strings.Contains(strings.Join(problems, "\n"), tc.want) {
				t.Fatalf("problems = %v, want one containing %q", problems, tc.want)
			}
		})
	}
}

// insertBefore 把 event 插到下标 index 之前，并把事件 ID 重新编为连续值。
func insertBefore(events []history.Event, index int, event history.Event) []history.Event {
	out := append(append(append([]history.Event(nil), events[:index]...), event), events[index:]...)
	renumber := map[int64]int64{}
	for i := range out {
		if out[i].ID != 0 {
			renumber[out[i].ID] = int64(i + 1)
		}
	}
	for i := range out {
		out[i].ID = int64(i + 1)
		remap(&out[i], renumber)
	}
	return out
}

// remap 改写事件里引用的 scheduled event ID。测试只用到下面这些事件。
func remap(event *history.Event, ids map[int64]int64) {
	fix := func(id *int64) {
		if next, ok := ids[*id]; ok {
			*id = next
		}
	}
	switch {
	case event.WorkflowTaskStarted != nil:
		copy := *event.WorkflowTaskStarted
		fix(&copy.ScheduledEventID)
		event.WorkflowTaskStarted = &copy
	case event.WorkflowTaskCompleted != nil:
		copy := *event.WorkflowTaskCompleted
		fix(&copy.ScheduledEventID)
		event.WorkflowTaskCompleted = &copy
	case event.WorkflowTaskTimedOut != nil:
		copy := *event.WorkflowTaskTimedOut
		fix(&copy.ScheduledEventID)
		event.WorkflowTaskTimedOut = &copy
	case event.ActivityStarted != nil:
		copy := *event.ActivityStarted
		fix(&copy.ScheduledEventID)
		event.ActivityStarted = &copy
	case event.ActivityTimedOut != nil:
		copy := *event.ActivityTimedOut
		fix(&copy.ScheduledEventID)
		event.ActivityTimedOut = &copy
	case event.ActivityAttemptFailed != nil:
		copy := *event.ActivityAttemptFailed
		fix(&copy.ScheduledEventID)
		event.ActivityAttemptFailed = &copy
	case event.ActivityCompleted != nil:
		copy := *event.ActivityCompleted
		fix(&copy.ScheduledEventID)
		event.ActivityCompleted = &copy
	case event.ActivityFailed != nil:
		copy := *event.ActivityFailed
		fix(&copy.ScheduledEventID)
		event.ActivityFailed = &copy
	case event.TimerFired != nil:
		copy := *event.TimerFired
		fix(&copy.ScheduledEventID)
		event.TimerFired = &copy
	case event.ChildWorkflowCompleted != nil:
		copy := *event.ChildWorkflowCompleted
		fix(&copy.ScheduledEventID)
		event.ChildWorkflowCompleted = &copy
	case event.ChildWorkflowFailed != nil:
		copy := *event.ChildWorkflowFailed
		fix(&copy.ScheduledEventID)
		event.ChildWorkflowFailed = &copy
	}
}

func TestParseExecutionsSkipsTruncatedLine(t *testing.T) {
	input := `{"phase":"start","worker":"worker-0","workflow_id":"w","run_id":"r","activity_id":"#a1","attempt":1,"time":"2026-11-12T10:00:00Z"}
{"phase":"end","worker":"worker-0","workflow_id":"w","run_id":"r","activity_id":"#a1","attempt":1,"time":"2026-11-12T10:00:01Z"}
{"phase":"start","worker":"worker-0","workflow_id":"w","run_id":"r","activi`
	records, malformed, err := parseExecutions(strings.NewReader(input))
	if err != nil || len(records) != 2 || malformed != 1 {
		t.Fatalf("records = %d, malformed = %d, err = %v", len(records), malformed, err)
	}
	grouped := groupExecutions(records)
	list := grouped[activityKey{RunID: "r", ActivityID: "#a1"}]
	if len(list) != 1 || list[0].End == nil {
		t.Fatalf("executions = %+v, want one finished execution", list)
	}
}

func TestAnalyzeExecutions(t *testing.T) {
	base := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return base.Add(time.Duration(seconds) * time.Second) }
	record := func(phase, worker, activityID string, attempt, second int) workload.Execution {
		return workload.Execution{Phase: phase, Worker: worker, RunID: "run", ActivityID: activityID, Attempt: attempt, Time: at(second)}
	}
	faults := []faultEvent{
		{At: at(5), Action: actionKillWorker, Target: "worker-0"},
		{At: at(8), Action: actionRestart, Target: "worker-0"},
		{At: at(30), Action: actionKillLeader, Target: "replica-1"},
	}
	records := []workload.Execution{
		// #a1：worker-0 在执行中被 kill，attempt 2 在 worker-1 上完成。
		record("start", "worker-0", "#a1", 1, 1),
		record("start", "worker-1", "#a1", 2, 15),
		record("end", "worker-1", "#a1", 2, 16),
		// #a2：执行完了但结果没被记录（副本被 kill），attempt 2 再执行。
		record("start", "worker-1", "#a2", 1, 29),
		record("end", "worker-1", "#a2", 1, 30),
		record("start", "worker-1", "#a2", 2, 45),
		record("end", "worker-1", "#a2", 2, 46),
		// #a3：执行一次。
		record("start", "worker-1", "#a3", 1, 2),
		record("end", "worker-1", "#a3", 1, 3),
		// 不在核对范围内的 Run。
		{Phase: "start", Worker: "worker-1", RunID: "other", ActivityID: "#a1", Attempt: 1, Time: at(1)},
	}
	completed := map[activityKey]int{
		{RunID: "run", ActivityID: "#a1"}: 2,
		{RunID: "run", ActivityID: "#a2"}: 2,
		{RunID: "run", ActivityID: "#a3"}: 1,
	}
	report := analyzeExecutions(completed, groupExecutions(records), faults)
	if len(report.Violations) != 0 {
		t.Fatalf("violations = %v", report.Violations)
	}
	if report.Activities != 3 || report.Executions != 5 || report.Unmatched != 1 || report.Distribution["1"] != 1 || report.Distribution["2"] != 2 {
		t.Fatalf("report = %+v", report)
	}
	if report.CausesSummary["worker crashed mid-attempt (kill-worker worker-0)"] != 1 ||
		report.CausesSummary["result not recorded after kill-leader replica-1"] != 1 {
		t.Fatalf("causes = %v", report.CausesSummary)
	}

	// 同一 attempt 执行两次、完成的 attempt 没有执行记录都是违规。
	records = append(records, record("start", "worker-0", "#a3", 1, 4))
	completed[activityKey{RunID: "run", ActivityID: "#a4"}] = 1
	report = analyzeExecutions(completed, groupExecutions(records), faults)
	joined := strings.Join(report.Violations, "\n")
	if !strings.Contains(joined, "#a3 attempt 1 executed more than once") || !strings.Contains(joined, "#a4 completed at attempt 1 without an execution record") {
		t.Fatalf("violations = %v", report.Violations)
	}
}

// soak2 的 44 次「无故障」重复执行都开始于 api-blackhole 注入之后、恢复之前：结果上报卡在被黑洞的副本上，
// attempt 超时后重新执行。归因看故障的持续区间，不只看注入时刻。
func TestInferCauseCountsFaultsStillActive(t *testing.T) {
	base := time.Date(2026, 11, 12, 14, 5, 30, 0, time.UTC)
	at := func(seconds float64) time.Time { return base.Add(time.Duration(seconds * float64(time.Second))) }
	faults := []faultEvent{
		{At: at(0), Action: actionAPIBlackhole, Target: "replica-2"},
		{At: at(20), Action: actionHeal, Target: "replica-2"},
		{At: at(120), Action: "db-drop (20 connections)", Target: "replica-1"},
		{At: at(240), Action: actionAPIBlackhole, Target: "replica-0"},
	}
	end := at(5)
	during := execution{Worker: "worker-0", Attempt: 1, Start: at(4), End: &end}
	if got := inferCause(during, execution{Worker: "worker-1", Attempt: 2, Start: at(15)}, faults); got != "result not recorded after api-blackhole replica-2" {
		t.Errorf("attempt during blackhole: %q", got)
	}
	// 恢复之后开始的 attempt 与它无关；瞬时的 db-drop 只在注入时刻生效；没有恢复事件的故障在注入时刻结束。
	afterEnd := at(30)
	after := execution{Worker: "worker-0", Attempt: 1, Start: at(25), End: &afterEnd}
	if got := inferCause(after, execution{Worker: "worker-0", Attempt: 2, Start: at(40)}, faults); got != "result not recorded (no fault in window)" {
		t.Errorf("attempt after heal: %q", got)
	}
	dropEnd := at(126)
	afterDrop := execution{Worker: "worker-0", Attempt: 1, Start: at(125), End: &dropEnd}
	if got := inferCause(afterDrop, execution{Worker: "worker-0", Attempt: 2, Start: at(136)}, faults); got != "result not recorded (no fault in window)" {
		t.Errorf("attempt after db-drop: %q", got)
	}
}

func TestExpectedResults(t *testing.T) {
	params := workload.OrderParams{Key: "order-1", Sequential: 2, Parallel: 3}
	want := workload.StepValue("order-1", "seq-0") + workload.StepValue("order-1", "seq-1") +
		workload.StepValue("order-1", "par-0") + workload.StepValue("order-1", "par-1") + workload.StepValue("order-1", "par-2")
	if got := workload.ExpectedOrderTotal(params); got != want || workload.OrderActivityCount(params) != 5 {
		t.Fatalf("ExpectedOrderTotal = %d, want %d", got, want)
	}
	if v := workload.StepValue("k", "s"); v < 0 || v >= 1000 || v != workload.StepValue("k", "s") {
		t.Fatalf("StepValue = %d", v)
	}
}
