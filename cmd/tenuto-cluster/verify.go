package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/client"
)

// programs 是负载两个 Workflow 的 Program，从随二进制发布的定义解出，供重放使用。
type programs struct {
	order *program.WorkflowConfig
	steps *program.WorkflowConfig
}

func loadPrograms() (programs, error) {
	configs := map[string]*program.WorkflowConfig{}
	for _, definition := range workloadDefinitions() {
		id, data, err := definition.Load()
		if err != nil {
			return programs{}, err
		}
		var config program.WorkflowConfig
		if err := json.Unmarshal(data, &config); err != nil {
			return programs{}, fmt.Errorf("decode workflow %s: %w", id, err)
		}
		configs[id] = &config
	}
	return programs{order: configs[workload.OrderWorkflowType], steps: configs[workload.StepsWorkflowType]}, nil
}

// fetchHistory 读取一个 Run 的完整 History。
func fetchHistory(ctx context.Context, c client.Client, workflowID, runID string) ([]history.Event, error) {
	var events []history.Event
	for it := c.GetWorkflowHistory(ctx, workflowID, runID); it.HasNext(); {
		event, err := it.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

// runCheck 是一次 cluster-order Run（含它的子 Run）的核对结果。
type runCheck struct {
	WorkflowID string   `json:"workflow_id"`
	RunID      string   `json:"run_id"`
	Status     string   `json:"status"`
	Problems   []string `json:"problems,omitempty"`
	// Describe 是没有完成的 Run 的 DescribeWorkflowExecution 结果。
	Describe *client.WorkflowExecutionDescription `json:"describe,omitempty"`
	Signal   string                               `json:"signal,omitempty"`
	Signals  int                                  `json:"signals"`

	completed map[activityKey]int
}

// orderOutput 是 cluster-order 输出的 JSON 形态。
type orderOutput struct {
	Total    int64         `json:"total"`
	Children []stepsOutput `json:"children"`
	Signal   string        `json:"signal"`
}

// stepsOutput 是 cluster-steps 输出的 JSON 形态。
type stepsOutput struct {
	Total int64 `json:"total"`
	Steps int   `json:"steps"`
}

// verifyOrder 核对一个 cluster-order Run：已完成且结果正确、History 不变式、Activity 个数、填充载荷完整、
// 重放无误；每个子 Run 同样核对，父 Run 记下的子 Run 结果与子 Run 的结果一致。History 逐个 Run 读取，
// 核对完即丢弃。
func verifyOrder(ctx context.Context, c client.Client, progs programs, workflowID string, params workload.OrderParams) runCheck {
	check := runCheck{WorkflowID: workflowID, completed: map[activityKey]int{}}
	fail := func(format string, args ...any) {
		check.Problems = append(check.Problems, fmt.Sprintf(format, args...))
	}
	description, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		fail("describe: %v", err)
		return check
	}
	check.RunID, check.Status = description.RunID, string(description.Status)
	if description.Status != client.WorkflowExecutionStatusCompleted {
		check.Describe = description
		fail("status is %s, want completed", description.Status)
	}
	events, err := fetchHistory(ctx, c, workflowID, description.RunID)
	if err != nil {
		fail("read history: %v", err)
		return check
	}
	for _, problem := range checkHistory(events) {
		fail("history: %s", problem)
	}
	for _, problem := range checkPads(events, params.PadBytes) {
		fail("history: %s", problem)
	}
	facts := extractFacts(events)
	check.Signals = facts.Signals
	checkActivities(fail, "", facts, workload.OrderActivityCount(params))
	for activityID, attempt := range facts.CompletedAttempts {
		check.completed[activityKey{RunID: description.RunID, ActivityID: activityID}] = attempt
	}
	if description.Status != client.WorkflowExecutionStatusCompleted {
		return check
	}
	if err := replayHistory(progs.order, events); err != nil {
		fail("replay: %v", err)
	}
	var out orderOutput
	if err := decodeOutput(facts.Closing, &out); err != nil {
		fail("%v", err)
		return check
	}
	check.Signal = out.Signal
	if want := workload.ExpectedOrderTotal(params); out.Total != want {
		fail("output total = %d, want %d", out.Total, want)
	}
	if out.Signal != "received" && out.Signal != "timeout" {
		fail("output signal = %q", out.Signal)
	}
	if len(out.Children) != params.Children {
		fail("output has %d children, want %d", len(out.Children), params.Children)
	}
	for i, child := range out.Children {
		if want := workload.ExpectedStepsTotal(workload.ChildKey(params.Key, i), params.ChildSteps); child.Total != want || child.Steps != params.ChildSteps {
			fail("output child %d = %+v, want total %d steps %d", i, child, want, params.ChildSteps)
		}
	}
	if len(facts.Children) != params.Children {
		fail("history starts %d child workflows, want %d", len(facts.Children), params.Children)
	}
	for _, child := range facts.Children {
		verifyChild(ctx, c, progs, description.RunID, child, params, fail, check.completed)
	}
	return check
}

// verifyChild 核对父 Run parentRunID 的一个子 Run。
func verifyChild(ctx context.Context, c client.Client, progs programs, parentRunID string, child childFact, params workload.OrderParams, fail func(string, ...any), completed map[activityKey]int) {
	childEvents, err := fetchHistory(ctx, c, parentRunID+"/"+child.ChildID, child.ChildRunID)
	if err != nil {
		fail("read child %s history: %v", child.ChildID, err)
		return
	}
	for _, problem := range checkHistory(childEvents) {
		fail("child %s history: %s", child.ChildID, problem)
	}
	for _, problem := range checkPads(childEvents, params.PadBytes) {
		fail("child %s history: %s", child.ChildID, problem)
	}
	childFacts := extractFacts(childEvents)
	checkActivities(fail, "child "+child.ChildID+" ", childFacts, params.ChildSteps)
	for activityID, attempt := range childFacts.CompletedAttempts {
		completed[activityKey{RunID: child.ChildRunID, ActivityID: activityID}] = attempt
	}
	if err := replayHistory(progs.steps, childEvents); err != nil {
		fail("child %s replay: %v", child.ChildID, err)
	}
	if childFacts.Closing.WorkflowCompleted == nil {
		fail("child run %s did not complete", child.ChildRunID)
	} else if !sameJSON(childFacts.Closing.WorkflowCompleted.Output, child.Output) {
		fail("parent recorded child output %s, child completed with %s", compactJSON(child.Output), compactJSON(childFacts.Closing.WorkflowCompleted.Output))
	}
}

// verifySteps 核对一个直接启动的 cluster-steps Run（bench 不调用；单元测试用它）。
func verifySteps(ctx context.Context, c client.Client, progs programs, workflowID string, params workload.StepsParams) runCheck {
	check := runCheck{WorkflowID: workflowID, completed: map[activityKey]int{}}
	fail := func(format string, args ...any) {
		check.Problems = append(check.Problems, fmt.Sprintf(format, args...))
	}
	events, err := fetchHistory(ctx, c, workflowID, "")
	if err != nil {
		fail("read history: %v", err)
		return check
	}
	for _, problem := range checkHistory(events) {
		fail("history: %s", problem)
	}
	for _, problem := range checkPads(events, len(params.Pad)) {
		fail("history: %s", problem)
	}
	facts := extractFacts(events)
	checkActivities(fail, "", facts, params.Steps)
	if err := replayHistory(progs.steps, events); err != nil {
		fail("replay: %v", err)
	}
	var out stepsOutput
	if err := decodeOutput(facts.Closing, &out); err != nil {
		fail("%v", err)
	} else if want := workload.ExpectedStepsTotal(params.Key, params.Steps); out.Total != want || out.Steps != params.Steps {
		fail("output = %+v, want total %d steps %d", out, want, params.Steps)
	}
	return check
}

// checkActivities 核对 Activity 调度：个数符合输入、ActivityID 不重复（同一个调用位置只调度一次）、都已完成。
func checkActivities(fail func(string, ...any), prefix string, facts historyFacts, want int) {
	if len(facts.ScheduledActivities) != want {
		fail("%shistory schedules %d activities, want %d", prefix, len(facts.ScheduledActivities), want)
	}
	seen := map[string]bool{}
	for _, id := range facts.ScheduledActivities {
		if seen[id] {
			fail("%sactivity %s is scheduled more than once", prefix, id)
		}
		seen[id] = true
		if _, ok := facts.CompletedAttempts[id]; !ok && facts.Closing.WorkflowCompleted != nil {
			fail("%sactivity %s has no ActivityCompleted", prefix, id)
		}
	}
}

func decodeOutput(closing history.Event, out any) error {
	if closing.WorkflowCompleted == nil {
		return fmt.Errorf("closing event is %s, want WorkflowCompleted", closing.EventType())
	}
	data, err := json.Marshal(closing.WorkflowCompleted.Output)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode output %s: %w", data, err)
	}
	return nil
}

// ── 数据库核对（只读） ──

// dbReport 是运行结束后对库的只读核对。Violations 违反引擎保证；Warnings 是设计上允许、但需要知道的残留。
type dbReport struct {
	ActivityStates   map[string]int64 `json:"activity_states_of_closed_runs"`
	TimerStates      map[string]int64 `json:"timer_states_of_closed_runs"`
	ChildStates      map[string]int64 `json:"child_states_of_closed_runs"`
	Dispatches       map[string]int64 `json:"dispatch_rows_of_closed_runs"`
	ExpiredLeases    int64            `json:"expired_leased_dispatch_rows"`
	OpenWorkflowTask int64            `json:"closed_runs_with_open_workflow_task"`
	TaskQTasks       map[string]int64 `json:"taskq_tasks"`
	Violations       []string         `json:"violations,omitempty"`
	Warnings         []string         `json:"warnings,omitempty"`
}

// checkDatabase 只读查询工具创建的库：Run 行与 execution state 一致；已关闭 Run 的 Activity 与子 Workflow
// state 都是终态（负载等待了每一个）；派发记录、TaskQ 任务与 Timer state 的残留计入报告。
func checkDatabase(ctx context.Context, db *sql.DB, namespace string) (dbReport, error) {
	report := dbReport{}
	var mismatched []string
	rows, err := db.QueryContext(ctx, `SELECT r.id, r.status, COALESCE(s.status, '<missing>') FROM tenuto_runs r
		LEFT JOIN tenuto_execution_states s ON s.run_id = r.id
		WHERE r.namespace = ? AND (s.run_id IS NULL OR s.status <> r.status)`, namespace)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var id, runStatus, stateStatus string
		if err := rows.Scan(&id, &runStatus, &stateStatus); err != nil {
			_ = rows.Close()
			return report, err
		}
		mismatched = append(mismatched, fmt.Sprintf("%s run=%s state=%s", id, runStatus, stateStatus))
	}
	_ = rows.Close()
	if len(mismatched) > 0 {
		report.Violations = append(report.Violations, "run status differs from execution state: "+strings.Join(limit(mismatched, 20), "; "))
	}

	closedRuns := "JOIN tenuto_runs r ON r.id = t.run_id WHERE r.namespace = ? AND r.status <> 'running'"
	if report.ActivityStates, err = countBy(ctx, db, "SELECT t.status, COUNT(*) FROM tenuto_activity_states t "+closedRuns+" GROUP BY t.status", namespace); err != nil {
		return report, err
	}
	if n := report.ActivityStates["scheduled"] + report.ActivityStates["started"]; n > 0 {
		report.Violations = append(report.Violations, fmt.Sprintf("%d activity states of closed runs are not terminal", n))
	}
	if report.ChildStates, err = countBy(ctx, db, "SELECT t.status, COUNT(*) FROM tenuto_child_workflow_states t "+closedRuns+" GROUP BY t.status", namespace); err != nil {
		return report, err
	}
	if n := report.ChildStates["initiated"] + report.ChildStates["started"]; n > 0 {
		report.Violations = append(report.Violations, fmt.Sprintf("%d child workflow states of closed runs are not terminal", n))
	}
	if report.TimerStates, err = countBy(ctx, db, "SELECT t.status, COUNT(*) FROM tenuto_timer_states t "+closedRuns+" GROUP BY t.status", namespace); err != nil {
		return report, err
	}
	if n := report.TimerStates["scheduled"]; n > 0 {
		report.Warnings = append(report.Warnings, fmt.Sprintf("%d timer states of closed runs are still scheduled (the signal-timeout timer is abandoned when the signal arrives first)", n))
	}
	if report.Dispatches, err = countBy(ctx, db, "SELECT CONCAT(t.task_type, '/', t.status), COUNT(*) FROM tenuto_task_dispatches t "+closedRuns+" GROUP BY t.task_type, t.status", namespace); err != nil {
		return report, err
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tenuto_task_dispatches t "+closedRuns+" AND t.status = 'leased' AND t.lease_expires_at < NOW(3)", namespace).Scan(&report.ExpiredLeases); err != nil {
		return report, err
	}
	if report.ExpiredLeases > 0 {
		report.Warnings = append(report.Warnings, fmt.Sprintf("%d dispatch rows of closed runs are still leased with an expired lease (dispatch rows are kept until retention GC; a leased row means the replica died between taking the lease and enqueueing)", report.ExpiredLeases))
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenuto_execution_states t `+closedRuns+` AND t.open_workflow_task_event_id <> 0`, namespace).Scan(&report.OpenWorkflowTask); err != nil {
		return report, err
	}
	if report.OpenWorkflowTask > 0 {
		report.Warnings = append(report.Warnings, fmt.Sprintf("%d closed runs still record an open workflow task", report.OpenWorkflowTask))
	}
	if report.TaskQTasks, err = countBy(ctx, db, "SELECT CONCAT(queue_name, IF(dispatched_at IS NULL, '/pending', '/dispatched')) AS bucket, COUNT(*) FROM taskq_tasks GROUP BY bucket"); err != nil {
		return report, err
	}
	for key, n := range report.TaskQTasks {
		if strings.HasSuffix(key, "/pending") {
			report.Warnings = append(report.Warnings, fmt.Sprintf("%d TaskQ tasks still pending in %s after every run closed", n, strings.TrimSuffix(key, "/pending")))
		}
	}
	return report, nil
}

func countBy(ctx context.Context, db *sql.DB, query string, args ...any) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", query, err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			return nil, err
		}
		out[key] = n
	}
	return out, rows.Err()
}

func limit(items []string, n int) []string {
	if len(items) <= n {
		return items
	}
	return append(items[:n:n], fmt.Sprintf("... and %d more", len(items)-n))
}
