package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 本文件是不需要数据库的核对逻辑：History 不变式、重放、Activity 执行记录分析。

// checkHistory 返回 events 违反的 History 不变式，events 是一个 Run 的完整 History。
func checkHistory(events []history.Event) []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if len(events) == 0 {
		return []string{"history is empty"}
	}
	if err := history.ValidateEventSequence(0, events); err != nil {
		add("event IDs are not contiguous from 1: %v", err)
	}
	if events[0].WorkflowStarted == nil {
		add("first event is %s, want WorkflowStarted", events[0].EventType())
	}

	byID := make(map[int64]history.Event, len(events))
	closing := 0
	activityTerminal := map[int64]int{}
	activityStarted := map[string]int{}
	openActivityIDs := map[string]int64{}
	timerClosed := map[int64]int{}
	childClosed := map[int64]int{}
	wftClosed := map[int64]int{}
	wftStarted := map[int64]int{}
	for i, event := range events {
		byID[event.ID] = event
		ref := func(kind string, scheduledID int64, want func(history.Event) bool) bool {
			scheduled, ok := byID[scheduledID]
			if !ok || !want(scheduled) {
				add("event %d (%s) references %s event %d that is not earlier in the history", event.ID, event.EventType(), kind, scheduledID)
				return false
			}
			return true
		}
		isActivity := func(e history.Event) bool { return e.ActivityScheduled != nil }
		closeActivity := func(scheduledID int64) {
			if !ref("ActivityScheduled", scheduledID, isActivity) {
				return
			}
			activityTerminal[scheduledID]++
			if activityTerminal[scheduledID] > 1 {
				add("activity scheduled at event %d has %d terminal events", scheduledID, activityTerminal[scheduledID])
			}
			delete(openActivityIDs, byID[scheduledID].ActivityScheduled.ActivityID)
		}
		switch {
		case event.WorkflowStarted != nil:
			if i != 0 {
				add("WorkflowStarted at event %d, want only at event 1", event.ID)
			}
		case isClosingEvent(event):
			closing++
			if i != len(events)-1 {
				add("closing event %s at event %d is not the last event", event.EventType(), event.ID)
			}
		case event.WorkflowTaskStarted != nil:
			id := event.WorkflowTaskStarted.ScheduledEventID
			if ref("WorkflowTaskScheduled", id, func(e history.Event) bool { return e.WorkflowTaskScheduled != nil }) {
				wftStarted[id]++
				if wftStarted[id] > 1 {
					add("workflow task scheduled at event %d started %d times", id, wftStarted[id])
				}
			}
		case event.WorkflowTaskCompleted != nil, event.WorkflowTaskFailed != nil, event.WorkflowTaskTimedOut != nil:
			id := workflowTaskScheduledID(event)
			if ref("WorkflowTaskScheduled", id, func(e history.Event) bool { return e.WorkflowTaskScheduled != nil }) {
				wftClosed[id]++
				if wftClosed[id] > 1 {
					add("workflow task scheduled at event %d closed %d times", id, wftClosed[id])
				}
			}
		case event.ActivityScheduled != nil:
			activityID := event.ActivityScheduled.ActivityID
			if previous, open := openActivityIDs[activityID]; open {
				add("ActivityScheduled %q at event %d while the one scheduled at event %d is still open", activityID, event.ID, previous)
			}
			openActivityIDs[activityID] = event.ID
		case event.ActivityStarted != nil:
			if ref("ActivityScheduled", event.ActivityStarted.ScheduledEventID, isActivity) {
				key := fmt.Sprintf("%d/%d", event.ActivityStarted.ScheduledEventID, event.ActivityStarted.Attempt)
				activityStarted[key]++
				if activityStarted[key] > 1 {
					add("activity scheduled at event %d attempt %d started %d times", event.ActivityStarted.ScheduledEventID, event.ActivityStarted.Attempt, activityStarted[key])
				}
				if activityTerminal[event.ActivityStarted.ScheduledEventID] > 0 {
					add("ActivityStarted at event %d after the activity closed", event.ID)
				}
			}
		case event.ActivityCompleted != nil:
			closeActivity(event.ActivityCompleted.ScheduledEventID)
		case event.ActivityFailed != nil:
			closeActivity(event.ActivityFailed.ScheduledEventID)
		case event.ActivityCancelled != nil:
			closeActivity(event.ActivityCancelled.ScheduledEventID)
		case event.ActivityTimedOut != nil:
			// RetryState 为空的超时之后还有下一次 attempt，不是终态。
			if event.ActivityTimedOut.RetryState != "" {
				closeActivity(event.ActivityTimedOut.ScheduledEventID)
			} else {
				ref("ActivityScheduled", event.ActivityTimedOut.ScheduledEventID, isActivity)
			}
		case event.ActivityAttemptFailed != nil:
			ref("ActivityScheduled", event.ActivityAttemptFailed.ScheduledEventID, isActivity)
		case event.TimerFired != nil, event.TimerCancelled != nil:
			id := timerScheduledID(event)
			if ref("TimerScheduled", id, func(e history.Event) bool { return e.TimerScheduled != nil }) {
				timerClosed[id]++
				if timerClosed[id] > 1 {
					add("timer scheduled at event %d fired or cancelled %d times", id, timerClosed[id])
				}
			}
		case event.ChildWorkflowCompleted != nil, event.ChildWorkflowFailed != nil,
			event.ChildWorkflowCancelled != nil, event.ChildWorkflowTerminated != nil:
			id := childScheduledID(event)
			if ref("ChildWorkflowStarted", id, func(e history.Event) bool { return e.ChildWorkflowStarted != nil }) {
				childClosed[id]++
				if childClosed[id] > 1 {
					add("child workflow started at event %d closed %d times", id, childClosed[id])
				}
			}
		}
	}
	if closing != 1 {
		add("history has %d closing events, want 1", closing)
	}
	return problems
}

func isClosingEvent(event history.Event) bool {
	return event.WorkflowCompleted != nil || event.WorkflowFailed != nil || event.WorkflowCancelled != nil ||
		event.WorkflowTerminated != nil || event.WorkflowContinuedAsNew != nil
}

func workflowTaskScheduledID(event history.Event) int64 {
	switch {
	case event.WorkflowTaskCompleted != nil:
		return event.WorkflowTaskCompleted.ScheduledEventID
	case event.WorkflowTaskFailed != nil:
		return event.WorkflowTaskFailed.ScheduledEventID
	case event.WorkflowTaskTimedOut != nil:
		return event.WorkflowTaskTimedOut.ScheduledEventID
	}
	return 0
}

func timerScheduledID(event history.Event) int64 {
	if event.TimerFired != nil {
		return event.TimerFired.ScheduledEventID
	}
	return event.TimerCancelled.ScheduledEventID
}

func childScheduledID(event history.Event) int64 {
	switch {
	case event.ChildWorkflowCompleted != nil:
		return event.ChildWorkflowCompleted.ScheduledEventID
	case event.ChildWorkflowFailed != nil:
		return event.ChildWorkflowFailed.ScheduledEventID
	case event.ChildWorkflowCancelled != nil:
		return event.ChildWorkflowCancelled.ScheduledEventID
	case event.ChildWorkflowTerminated != nil:
		return event.ChildWorkflowTerminated.ScheduledEventID
	}
	return 0
}

// checkPads 核对填充载荷完整：每个 ActivityScheduled 的输入与 ActivityCompleted 的输出里 pad 都是 want 字节。
func checkPads(events []history.Event, want int) []string {
	var problems []string
	padLen := func(payload map[string]any) int {
		pad, _ := payload["pad"].(string)
		return len(pad)
	}
	for _, event := range events {
		switch {
		case event.ActivityScheduled != nil:
			if got := padLen(event.ActivityScheduled.Input); got != want {
				problems = append(problems, fmt.Sprintf("event %d ActivityScheduled input pad has %d bytes, want %d", event.ID, got, want))
			}
		case event.ActivityCompleted != nil:
			if got := padLen(event.ActivityCompleted.Output); got != want {
				problems = append(problems, fmt.Sprintf("event %d ActivityCompleted output pad has %d bytes, want %d", event.ID, got, want))
			}
		}
	}
	return problems
}

// historyFacts 是核对执行记录与结果时用到的 History 摘要。
type historyFacts struct {
	ScheduledActivities []string // 按 History 顺序的 ActivityID
	// CompletedAttempts 是 ActivityID → ActivityCompleted 的 attempt。
	CompletedAttempts map[string]int
	Signals           int
	TimersScheduled   int
	TimersFired       int
	Children          []childFact
	Closing           history.Event
}

// childFact 是 History 里一个子 Workflow：ChildWorkflowStarted 的 ID 与子 Run，以及 ChildWorkflowCompleted 记下的结果。
type childFact struct {
	ChildID    string
	ChildRunID string
	Output     map[string]any
}

func extractFacts(events []history.Event) historyFacts {
	facts := historyFacts{CompletedAttempts: map[string]int{}}
	scheduled := map[int64]string{}
	children := map[int64]int{}
	for _, event := range events {
		switch {
		case event.ActivityScheduled != nil:
			scheduled[event.ID] = event.ActivityScheduled.ActivityID
			facts.ScheduledActivities = append(facts.ScheduledActivities, event.ActivityScheduled.ActivityID)
		case event.ActivityCompleted != nil:
			facts.CompletedAttempts[scheduled[event.ActivityCompleted.ScheduledEventID]] = history.NormalizeActivityAttempt(event.ActivityCompleted.Attempt)
		case event.WorkflowSignaled != nil:
			facts.Signals++
		case event.TimerScheduled != nil:
			facts.TimersScheduled++
		case event.TimerFired != nil:
			facts.TimersFired++
		case event.ChildWorkflowStarted != nil:
			children[event.ID] = len(facts.Children)
			facts.Children = append(facts.Children, childFact{ChildID: event.ChildWorkflowStarted.ChildID, ChildRunID: event.ChildWorkflowStarted.ChildRunID})
		case event.ChildWorkflowCompleted != nil:
			if i, ok := children[event.ChildWorkflowCompleted.ScheduledEventID]; ok {
				facts.Children[i].Output = event.ChildWorkflowCompleted.Output
			}
		case isClosingEvent(event):
			facts.Closing = event
		}
	}
	return facts
}

// replayHistory 用 Worker 的解释器从头重放一个已关闭 Run 的 History：最后一个 Workflow Task 之前的批次
// 由解释器逐批与 History 对照（不一致时报非确定性错误），最后一个 Workflow Task 产出的命令再与它之后的
// 命令事件和关闭事件对照。
func replayHistory(config *program.WorkflowConfig, events []history.Event) error {
	last := -1
	for i, event := range events {
		if event.WorkflowTaskCompleted != nil {
			last = i
		}
	}
	if last < 0 {
		return fmt.Errorf("history has no completed workflow task")
	}
	commands, err := interpreter.NewWorkflowExecutor(config, events[:last]).Execute()
	if err != nil {
		return err
	}
	var terminal *history.Command
	var rest []history.Command
	for i := range commands {
		switch commands[i].Type {
		case history.CmdCompleteWorkflow, history.CmdFailWorkflow, history.CmdCancelWorkflow, history.CmdContinueAsNew:
			terminal = &commands[i]
		default:
			rest = append(rest, commands[i])
		}
	}
	if err := executor.MatchReplayWithHistory(rest, events[last+1:]); err != nil {
		return err
	}
	closing := events[len(events)-1]
	switch {
	case closing.WorkflowCompleted != nil:
		if terminal == nil || terminal.Type != history.CmdCompleteWorkflow {
			return fmt.Errorf("history closes with WorkflowCompleted, replay produced %s", describeTerminal(terminal))
		}
		if !sameJSON(terminal.Output, closing.WorkflowCompleted.Output) {
			return fmt.Errorf("replayed output %s differs from WorkflowCompleted output %s", compactJSON(terminal.Output), compactJSON(closing.WorkflowCompleted.Output))
		}
	case closing.WorkflowFailed != nil:
		if terminal == nil || terminal.Type != history.CmdFailWorkflow {
			return fmt.Errorf("history closes with WorkflowFailed, replay produced %s", describeTerminal(terminal))
		}
	}
	return nil
}

func describeTerminal(command *history.Command) string {
	if command == nil {
		return "no closing command"
	}
	return string(command.Type)
}

// sameJSON 比较两个值的 JSON 形态（数字统一为 float64）。
func sameJSON(a, b any) bool {
	return reflect.DeepEqual(normalizeJSON(a), normalizeJSON(b))
}

func normalizeJSON(v any) any {
	data, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	var out any
	_ = json.Unmarshal(data, &out)
	return out
}

func compactJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

// ── Activity 执行记录 ──

// execution 是一次 Activity attempt 的执行：一条 start 记录，以及配对上的 end 记录。
type execution struct {
	Worker  string
	RunID   string
	Attempt int
	Start   time.Time
	End     *time.Time
}

// activityKey 标识一个 Activity：Run ID + ActivityID（子 Run 有自己的 Run ID）。
type activityKey struct {
	RunID      string
	ActivityID string
}

// parseExecutions 读取一个 JSONL 文件，返回解析出的记录与不完整行数（kill -9 可能截断最后一行）。
func parseExecutions(r io.Reader) ([]workload.Execution, int, error) {
	var records []workload.Execution
	malformed := 0
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record workload.Execution
		if err := json.Unmarshal([]byte(line), &record); err != nil || record.RunID == "" {
			malformed++
			continue
		}
		records = append(records, record)
	}
	return records, malformed, scanner.Err()
}

// groupExecutions 把 start / end 记录配对成 execution，按 Activity 分组、按开始时间排序。
// end 配对同一 Worker、同一 attempt 最早一条尚未结束的 start。
func groupExecutions(records []workload.Execution) map[activityKey][]execution {
	sorted := slices.Clone(records)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time.Before(sorted[j].Time) })
	out := map[activityKey][]execution{}
	for _, record := range sorted {
		key := activityKey{RunID: record.RunID, ActivityID: record.ActivityID}
		switch record.Phase {
		case "start":
			out[key] = append(out[key], execution{Worker: record.Worker, RunID: record.RunID, Attempt: record.Attempt, Start: record.Time})
		case "end":
			list := out[key]
			for i := range list {
				if list[i].End == nil && list[i].Worker == record.Worker && list[i].Attempt == record.Attempt {
					end := record.Time
					list[i].End = &end
					break
				}
			}
		}
	}
	return out
}

// faultEvent 是 failover 期间注入的一次故障。
type faultEvent struct {
	At     time.Time `json:"at"`
	Action string    `json:"action"`
	Target string    `json:"target"`
}

// executionReport 是执行记录与 History 对照的结果。
type executionReport struct {
	Executions    int            `json:"executions"`
	Activities    int            `json:"activities"`
	Distribution  map[string]int `json:"executions_per_activity"`
	ReExecutions  []reExecution  `json:"re_executions,omitempty"`
	CausesSummary map[string]int `json:"re_execution_causes,omitempty"`
	Violations    []string       `json:"violations,omitempty"`
	// Unmatched 是在核对的 History 里找不到的 Activity 的执行记录数。
	Unmatched int `json:"unmatched_records"`
}

type reExecution struct {
	RunID      string `json:"run_id"`
	ActivityID string `json:"activity_id"`
	Attempt    int    `json:"attempt"`
	Worker     string `json:"worker"`
	Cause      string `json:"cause"`
}

// analyzeExecutions 对照 completed（Activity → History 里 ActivityCompleted 的 attempt）与执行记录：
// History 里完成的 attempt 必须有执行记录；同一 attempt 不能执行两次（Server 只让一次 Start 成功）。
// 重复执行（at-least-once 允许）按前一次执行有没有 end 记录与故障时间线推断原因。
func analyzeExecutions(completed map[activityKey]int, executions map[activityKey][]execution, faults []faultEvent) executionReport {
	report := executionReport{Distribution: map[string]int{}, CausesSummary: map[string]int{}}
	keys := make([]activityKey, 0, len(executions))
	for key := range executions {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].RunID != keys[j].RunID {
			return keys[i].RunID < keys[j].RunID
		}
		return keys[i].ActivityID < keys[j].ActivityID
	})
	for _, key := range keys {
		list := executions[key]
		if _, ok := completed[key]; !ok {
			report.Unmatched += len(list)
			continue
		}
		report.Executions += len(list)
		report.Distribution[fmt.Sprint(len(list))]++
		seen := map[int]bool{}
		for i, current := range list {
			if seen[current.Attempt] {
				report.Violations = append(report.Violations, fmt.Sprintf("run %s activity %s attempt %d executed more than once", key.RunID, key.ActivityID, current.Attempt))
			}
			seen[current.Attempt] = true
			if i == 0 {
				continue
			}
			cause := inferCause(list[i-1], current, faults)
			report.CausesSummary[cause]++
			report.ReExecutions = append(report.ReExecutions, reExecution{RunID: key.RunID, ActivityID: key.ActivityID, Attempt: current.Attempt, Worker: current.Worker, Cause: cause})
		}
	}
	for key, attempt := range completed {
		report.Activities++
		found := false
		for _, item := range executions[key] {
			if item.Attempt == attempt {
				found = true
				break
			}
		}
		if !found {
			report.Violations = append(report.Violations, fmt.Sprintf("run %s activity %s completed at attempt %d without an execution record", key.RunID, key.ActivityID, attempt))
		}
	}
	sort.Strings(report.Violations)
	if len(report.CausesSummary) == 0 {
		report.CausesSummary = nil
	}
	return report
}

// inferCause 推断 current 为什么又执行了一次。previous 没有 end 记录：执行它的 Worker 中途退出；
// 有 end 记录：结果没有被记录（上报丢失或 attempt 已超时），归到这段时间里最近一次 Server 故障。
func inferCause(previous, current execution, faults []faultEvent) string {
	if previous.End == nil {
		for _, fault := range slices.Backward(faults) {
			if isInjectedFault(fault.Action) && fault.Target == previous.Worker && !fault.At.Before(previous.Start) && !fault.At.After(current.Start) {
				return "worker crashed mid-attempt (" + fault.Action + " " + fault.Target + ")"
			}
		}
		return "attempt has no end record (worker exited)"
	}
	// 故障在 previous 开始之前注入、到 previous 开始时仍未恢复的同样算数：api-blackhole 期间开始的 attempt，
	// 结果上报卡在被黑洞的副本上，直到 attempt 超时。
	for i, fault := range slices.Backward(faults) {
		relevant := isInjectedFault(fault.Action) && (strings.Contains(fault.Target, "replica") || fault.Target == previous.Worker)
		if relevant && !faultEndAt(faults, i).Before(previous.Start.Add(-time.Second)) && !fault.At.After(current.Start) {
			return "result not recorded after " + fault.Action + " " + fault.Target
		}
	}
	return "result not recorded (no fault in window)"
}

// faultEndAt 是 faults[i] 恢复的时间：同一目标上的下一个 heal、restart 或 resume。下一个事件是同一目标上的另一个
// 故障，或者没有恢复事件（db-drop 这类瞬时故障）时，故障在注入的时刻结束。
func faultEndAt(faults []faultEvent, i int) time.Time {
	for _, next := range faults[i+1:] {
		if next.Target != faults[i].Target {
			continue
		}
		switch next.Action {
		case actionHeal, actionRestart, actionResume:
			return next.At
		}
		break
	}
	return faults[i].At
}
