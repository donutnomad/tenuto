package client_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

type greetingOut struct {
	Greeting string `json:"greeting"`
	Big      int64  `json:"big"`
}

// ExecuteWorkflow + Get：结果按 Workflow 的返回值解码，2^53 以上的整数不丢精度。
func TestExecuteWorkflowGetDecodesResult(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("greeting", greetingSrc)
	const big = int64(1)<<53 + 1
	run := env.execute("greeting", "greeting-1", map[string]any{"name": "tenuto", "big": big})
	if run.GetID() != "greeting-1" || run.GetRunID() == "" {
		t.Fatalf("run = %s/%s", run.GetID(), run.GetRunID())
	}
	var out greetingOut
	must(t, get(t, run, &out))
	if out.Greeting != "hello tenuto" || out.Big != big {
		t.Fatalf("result = %+v", out)
	}
	// GetWorkflow 不带 runID 时从当前 Run 读取同一个结果；nil 目标丢弃结果。
	must(t, get(t, env.client.GetWorkflow(context.Background(), "greeting-1", ""), nil))
}

// Workflow 返回应用错误：WorkflowExecutionError 包着 *apperror.Error，Type、NonRetryable、Details 都保留。
func TestGetReturnsApplicationError(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("failing", failingSrc)
	run := env.execute("failing", "failing-1")
	err := get(t, run, nil)
	var executionErr *client.WorkflowExecutionError
	if !errors.As(err, &executionErr) || executionErr.WorkflowType != "failing" || executionErr.RunID != run.GetRunID() {
		t.Fatalf("err = %#v", err)
	}
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Type() != "PaymentDeclined" || !appErr.NonRetryable() || appErr.Message() != "card declined" {
		t.Fatalf("application error = %#v", appErr)
	}
	var detail map[string]string
	if err := appErr.Details(&detail); err != nil || detail["code"] != "insufficient_funds" {
		t.Fatalf("details = %v, %v", detail, err)
	}
	if workflow.IsCanceledError(err) {
		t.Fatal("an application error is not a canceled error")
	}
}

// 取消：Workflow 返回取消错误时 Get 的错误满足 IsCanceledError；Workflow 清理后正常返回时 Get 拿到清理结果。
func TestCancelWorkflow(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("await", awaitSrc)
	env.register("cleanup", cleanupSrc)

	t.Run("canceled error", func(t *testing.T) {
		run := env.execute("await", "cancel-1")
		waitFirstWorkflowTask(t, env, run)
		must(t, env.client.CancelWorkflow(context.Background(), run.GetID(), run.GetRunID()))
		err := get(t, run, nil)
		if !workflow.IsCanceledError(err) || !apperror.IsCanceled(err) {
			t.Fatalf("err = %v, want a canceled error", err)
		}
		description, derr := env.client.DescribeWorkflowExecution(context.Background(), run.GetID(), run.GetRunID())
		if derr != nil || description.Status != client.WorkflowExecutionStatusCanceled {
			t.Fatalf("description = %+v, %v", description, derr)
		}
	})
	t.Run("cleanup result", func(t *testing.T) {
		run := env.execute("cleanup", "cancel-2")
		waitFirstWorkflowTask(t, env, run)
		must(t, env.client.CancelWorkflow(context.Background(), run.GetID(), ""))
		var out map[string]any
		must(t, get(t, run, &out))
		if out["cleaned"] != true {
			t.Fatalf("cleanup result = %v", out)
		}
	})
}

// 终止：Get 的错误原因是 *TerminatedError，带终止原因。
func TestTerminateWorkflow(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	run := env.execute("waiting", "terminate-1")
	must(t, env.client.TerminateWorkflow(context.Background(), run.GetID(), run.GetRunID(), "operator stop"))
	var terminated *client.TerminatedError
	if err := get(t, run, nil); !errors.As(err, &terminated) || terminated.Reason != "operator stop" {
		t.Fatalf("err = %v, want TerminatedError", err)
	}
}

// 执行超时：原因是 TimeoutType 为 TimeoutTypeWorkflowExecution 的 *apperror.Error。
func TestGetReturnsWorkflowTimeout(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("timeout", waitingSrc, func(config *program.WorkflowConfig) { config.ExecutionTimeout = "300ms" })
	err := get(t, env.execute("timeout", "timeout-1"), nil)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.TimeoutType() != client.TimeoutTypeWorkflowExecution {
		t.Fatalf("err = %v, want an execution timeout", err)
	}
}

// continue-as-new：Get 跟到链上最后一个 Run，返回它的结果。
func TestGetFollowsContinueAsNewChain(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("continue", continueAsNewSrc)
	run := env.execute("continue", "continue-1", map[string]any{"n": 0})
	var out struct {
		N int64 `json:"n"`
	}
	must(t, get(t, run, &out))
	if out.N != 2 {
		t.Fatalf("result = %+v, want the third run's result", out)
	}
	list, err := env.client.ListWorkflow(context.Background(), client.ListWorkflowOptions{WorkflowID: "continue-1"})
	must(t, err)
	if list.Total != 3 {
		t.Fatalf("runs in the chain = %d, want 3", list.Total)
	}
}

// Workflow 重试：Get 跟到重试 Run。成功的重试返回结果；重试耗尽时返回最后一个 Run 的失败。
func TestGetFollowsWorkflowRetryChain(t *testing.T) {
	retry := func(config *program.WorkflowConfig) {
		config.RetryPolicy = &program.RetryPolicy{MaxAttempts: 2, InitialInterval: "10ms"}
	}
	t.Run("retry succeeds", func(t *testing.T) {
		env := newTestEnv(t, true)
		env.register("flaky", strings.Replace(countingSrc, "THRESHOLD", "2", 1), retry)
		var out struct {
			N int64 `json:"n"`
		}
		must(t, get(t, env.execute("flaky", "flaky-1"), &out))
		if out.N != 2 {
			t.Fatalf("result = %+v, want the second attempt", out)
		}
	})
	t.Run("retries exhausted", func(t *testing.T) {
		env := newTestEnv(t, true)
		env.register("broken", strings.Replace(countingSrc, "THRESHOLD", "100", 1), retry)
		run := env.execute("broken", "broken-1")
		err := get(t, run, nil)
		var executionErr *client.WorkflowExecutionError
		var appErr *apperror.Error
		if !errors.As(err, &executionErr) || !errors.As(err, &appErr) || appErr.Type() != "Flaky" {
			t.Fatalf("err = %v", err)
		}
		if executionErr.RunID == run.GetRunID() {
			t.Fatalf("error names the first run %s, want the last attempt", executionErr.RunID)
		}
	})
}

// 同一 Workflow ID 有运行中的 Run 时启动失败；Run 结束后可以用同一个 ID 再启动。
func TestExecuteWorkflowDuplicateID(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	first := env.execute("waiting", "dup-1")
	_, err := env.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: "dup-1"}, "waiting")
	if !errors.Is(err, client.ErrWorkflowExecutionAlreadyStarted) || !errors.Is(err, client.ErrConflict) {
		t.Fatalf("err = %v, want ErrWorkflowExecutionAlreadyStarted", err)
	}
	var serviceErr *client.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.StatusCode != 409 {
		t.Fatalf("err = %#v", err)
	}
	must(t, env.client.TerminateWorkflow(context.Background(), "dup-1", "", "done"))
	_ = get(t, first, nil)
	second := env.execute("waiting", "dup-1")
	if second.GetRunID() == first.GetRunID() {
		t.Fatal("a new run must start once the previous one closed")
	}
	if _, err := env.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID: "dup-1", WorkflowIDConflictPolicy: client.WorkflowIDConflictPolicyUseExisting,
	}, "waiting"); err == nil {
		t.Fatal("ExecuteWorkflow accepted WorkflowIDConflictPolicyUseExisting")
	}
}

// Signal、SignalWithStart（启动与复用两个分支）、Query（运行中、已结束、未知类型）。
func TestSignalsAndQueries(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	ctx := context.Background()

	run, err := env.client.SignalWithStartWorkflow(ctx, "sws-1", "add", map[string]any{"value": "a"}, client.StartWorkflowOptions{}, "waiting")
	must(t, err)
	again, err := env.client.SignalWithStartWorkflow(ctx, "sws-1", "add", map[string]any{"value": "b"}, client.StartWorkflowOptions{}, "waiting")
	must(t, err)
	if again.GetRunID() != run.GetRunID() {
		t.Fatalf("second SignalWithStart started run %s, want the running %s", again.GetRunID(), run.GetRunID())
	}
	var out map[string]any
	must(t, get(t, run, &out))
	if out["count"] != int64(2) || out["last"] != "b" {
		t.Fatalf("result = %v", out)
	}

	running := env.execute("waiting", "query-1")
	must(t, env.client.SignalWorkflow(ctx, "query-1", "", "add", map[string]any{"value": "x"}))
	eventually(t, "query sees the signal", func() bool {
		value, err := env.client.QueryWorkflow(ctx, "query-1", "", "count")
		if err != nil {
			return false
		}
		var state struct {
			Count int64  `json:"count"`
			Last  string `json:"last"`
		}
		return value.HasValue() && value.Get(&state) == nil && state.Count == 1 && state.Last == "x"
	})
	_, err = env.client.QueryWorkflow(ctx, "query-1", "", "missing")
	var queryErr *client.QueryFailedError
	if !errors.As(err, &queryErr) || queryErr.QueryType != "missing" || !strings.Contains(queryErr.Message, "unknown queryType") {
		t.Fatalf("unknown query err = %v", err)
	}
	must(t, env.client.SignalWorkflow(ctx, "query-1", running.GetRunID(), "add", map[string]any{"value": "y"}))
	must(t, get(t, running, nil))
	value, err := env.client.QueryWorkflow(ctx, "query-1", running.GetRunID(), "count")
	must(t, err)
	var closed map[string]any
	must(t, value.Get(&closed))
	if closed["count"] != int64(2) {
		t.Fatalf("query on the closed run = %v", closed)
	}
	if err := env.client.SignalWorkflow(ctx, "query-1", running.GetRunID(), "add", map[string]any{"value": "z"}); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("signal to a closed run = %v", err)
	}
}

type limitResult struct {
	Previous int64 `json:"previous"`
	Current  int64 `json:"current"`
}

// Update：accepted 与 completed 两个阶段、handler 返回的应用错误、Run 在 Update 完成之前结束、validator 拒绝。
func TestUpdateWorkflow(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("updates", updatesSrc)
	ctx := context.Background()
	run := env.execute("updates", "updates-1")

	completed, err := env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: "updates-1", UpdateName: "set-limit", Args: []any{int64(500)}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	must(t, err)
	if completed.RunID() != run.GetRunID() || completed.UpdateID() == "" {
		t.Fatalf("handle = %s/%s", completed.RunID(), completed.UpdateID())
	}
	var result limitResult
	must(t, completed.Get(ctx, &result))
	if result != (limitResult{Previous: 100, Current: 500}) {
		t.Fatalf("update result = %+v", result)
	}

	accepted, err := env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: "updates-1", UpdateID: "set-700", UpdateName: "set-limit", Args: []any{int64(700)},
	})
	must(t, err)
	must(t, accepted.Get(ctx, &result))
	if result.Current != 700 || accepted.UpdateID() != "set-700" {
		t.Fatalf("accepted update result = %+v", result)
	}

	failed, err := env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: "updates-1", UpdateName: "set-limit", Args: []any{int64(-1)}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	must(t, err)
	var appErr *apperror.Error
	if err := failed.Get(ctx, &result); !errors.As(err, &appErr) || appErr.Type() != "InvalidLimit" {
		t.Fatalf("failed update err = %v", err)
	}

	env.server.RegisterUpdateValidator("set-limit-validated", func(string, protocol.Payloads) error {
		return apperror.New("limit too large", "LimitTooLarge")
	})
	_, err = env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: "updates-1", UpdateName: "set-limit-validated"})
	if !errors.Is(err, client.ErrUpdateRejected) || !errors.As(err, &appErr) || appErr.Type() != "LimitTooLarge" {
		t.Fatalf("rejected update err = %v", err)
	}

	stuck, err := env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: "updates-1", UpdateName: "stuck"})
	must(t, err)
	finished, err := env.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: "updates-1", UpdateName: "finish", WaitForStage: client.WorkflowUpdateStageCompleted})
	must(t, err)
	must(t, finished.Get(ctx, nil))
	var out map[string]any
	must(t, get(t, run, &out))
	if out["limit"] != int64(700) {
		t.Fatalf("workflow result = %v", out)
	}
	// Run 在 stuck 完成之前结束：Get 返回失败，不会一直等。
	getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := stuck.Get(getCtx, nil); !errors.As(err, &appErr) {
		t.Fatalf("update on a closed run err = %v", err)
	}
}

// Update-With-Start：第一次启动 Run，第二次复用运行中的 Run；启动结果经 WithStartWorkflowOperation 读取。
func TestUpdateWithStartWorkflow(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("updates", updatesSrc)
	ctx := context.Background()
	send := func(limit int64) (client.WorkflowRun, limitResult) {
		t.Helper()
		op := env.client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
			ID: "uws-1", WorkflowIDConflictPolicy: client.WorkflowIDConflictPolicyUseExisting,
		}, "updates")
		handle, err := env.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: op,
			UpdateOptions:          client.UpdateWorkflowOptions{UpdateName: "set-limit", Args: []any{limit}, WaitForStage: client.WorkflowUpdateStageCompleted},
		})
		must(t, err)
		run, err := op.Get(ctx)
		must(t, err)
		var result limitResult
		must(t, handle.Get(ctx, &result))
		if _, err := env.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{StartWorkflowOperation: op}); err == nil {
			t.Fatal("a start operation was executed twice")
		}
		return run, result
	}
	first, result := send(200)
	if result.Previous != 100 || first.GetID() != "uws-1" {
		t.Fatalf("first = %s %+v", first.GetID(), result)
	}
	second, result := send(300)
	if second.GetRunID() != first.GetRunID() || result.Previous != 200 {
		t.Fatalf("second = %s %+v, want the running %s", second.GetRunID(), result, first.GetRunID())
	}
	op := env.client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{ID: "uws-2"}, "updates")
	if _, err := env.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: op, UpdateOptions: client.UpdateWorkflowOptions{UpdateName: "set-limit"},
	}); err == nil {
		t.Fatal("update-with-start accepted a start without WorkflowIDConflictPolicyUseExisting")
	}
}

// Get 在 ctx 到期时立即返回 ctx 的错误。
func TestGetHonoursContext(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	run := env.execute("waiting", "ctx-1")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := run.Get(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Get returned after %s", elapsed)
	}
}

// Describe、UpsertSearchAttributes、List（按 Workflow ID、状态与搜索属性过滤）。
func TestDescribeAndListWorkflow(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	env.register("greeting", greetingSrc)
	ctx := context.Background()
	running := env.execute("waiting", "list-running")
	must(t, get(t, env.execute("greeting", "list-done", map[string]any{"name": "x"}), nil))

	description, err := env.client.DescribeWorkflowExecution(ctx, "list-running", "")
	must(t, err)
	if description.RunID != running.GetRunID() || description.WorkflowType != "waiting" || description.Status != client.WorkflowExecutionStatusRunning ||
		!description.IsCurrent || description.Attempt != 1 || description.StartTime.IsZero() {
		t.Fatalf("description = %+v", description)
	}
	if _, err := env.client.DescribeWorkflowExecution(ctx, "", "missing-run"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("describe missing run err = %v", err)
	}

	all, err := env.client.ListWorkflow(ctx, client.ListWorkflowOptions{})
	must(t, err)
	if all.Total != 2 {
		t.Fatalf("all runs = %+v", all)
	}
	done, err := env.client.ListWorkflow(ctx, client.ListWorkflowOptions{Statuses: []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusCompleted}})
	must(t, err)
	if done.Total != 1 || done.Executions[0].WorkflowID != "list-done" || done.Executions[0].CloseTime == nil {
		t.Fatalf("completed runs = %+v", done)
	}
	must(t, env.client.UpsertSearchAttributes(ctx, "list-running", "", map[string]string{"merchant": "m-42", "region": "eu"}))
	must(t, env.client.UpsertSearchAttributes(ctx, "list-running", "", map[string]string{"region": "us"}))
	tagged, err := env.client.ListWorkflow(ctx, client.ListWorkflowOptions{SearchAttributes: map[string]string{"merchant": "m-42", "region": "us"}})
	must(t, err)
	if tagged.Total != 1 || tagged.Executions[0].RunID != running.GetRunID() {
		t.Fatalf("runs tagged merchant=m-42 region=us = %+v", tagged)
	}
	if err := env.client.UpsertSearchAttributes(ctx, "list-running", "", map[string]string{"TemporalChangeVersion": "x"}); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("reserved search attribute err = %v", err)
	}
	byID, err := env.client.ListWorkflow(ctx, client.ListWorkflowOptions{WorkflowID: "list-running"})
	must(t, err)
	if byID.Total != 1 || byID.Executions[0].RunID != running.GetRunID() || byID.Executions[0].Status != client.WorkflowExecutionStatusRunning {
		t.Fatalf("runs of list-running = %+v", byID)
	}
}

// GetWorkflowHistory 跨页读出完整历史：没有 Worker 时 210 个 Signal 让历史超过一页（200 个事件）。
func TestGetWorkflowHistoryIteratesPages(t *testing.T) {
	env := newTestEnv(t, false)
	env.register("waiting", waitingSrc)
	ctx := context.Background()
	env.execute("waiting", "history-1")
	for i := range 210 {
		must(t, env.client.SignalWorkflow(ctx, "history-1", "", "noise", map[string]any{"i": i}))
	}
	events := env.client.GetWorkflowHistory(ctx, "history-1", "")
	var ids []int64
	signals := 0
	for events.HasNext() {
		event, err := events.Next()
		must(t, err)
		ids = append(ids, event.ID)
		if event.WorkflowSignaled != nil {
			signals++
		}
	}
	if signals != 210 || len(ids) <= 200 {
		t.Fatalf("history has %d events and %d signals", len(ids), signals)
	}
	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("event %d has ID %d: pages are not contiguous", i, id)
		}
	}
	if _, err := events.Next(); err == nil {
		t.Fatal("Next after the last event must fail")
	}
	missing := env.client.GetWorkflowHistory(ctx, "", "missing-run")
	if !missing.HasNext() {
		t.Fatal("a failed page read is reported through Next")
	}
	if _, err := missing.Next(); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("history of a missing run err = %v", err)
	}
}

// Reset 从第一个 Workflow Task 分叉出新 Run，旧 Run 被终止；UpdateWorkflowExecutionOptions 改版本策略。
func TestResetAndUpdateWorkflowExecutionOptions(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	ctx := context.Background()
	run := env.execute("waiting", "reset-1")
	waitFirstWorkflowTask(t, env, run)

	must(t, env.client.UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{WorkflowID: "reset-1", Policy: workflow.PolicyPinned}))
	description, err := env.client.DescribeWorkflowExecution(ctx, "reset-1", "")
	must(t, err)
	if description.Policy != workflow.PolicyPinned {
		t.Fatalf("policy = %q, want pinned", description.Policy)
	}

	newRunID, err := env.client.ResetWorkflowExecution(ctx, client.ResetWorkflowExecutionOptions{
		WorkflowID: "reset-1", Reason: "replay", ResetType: client.ResetTypeFirstWorkflowTask,
	})
	must(t, err)
	if newRunID == "" || newRunID == run.GetRunID() {
		t.Fatalf("reset run = %q", newRunID)
	}
	old, err := env.client.DescribeWorkflowExecution(ctx, "reset-1", run.GetRunID())
	must(t, err)
	if old.Status != client.WorkflowExecutionStatusTerminated {
		t.Fatalf("old run status = %s", old.Status)
	}
	current, err := env.client.DescribeWorkflowExecution(ctx, "reset-1", "")
	must(t, err)
	if current.RunID != newRunID || current.Status != client.WorkflowExecutionStatusRunning {
		t.Fatalf("current run = %+v", current)
	}
	if _, err := env.client.ResetWorkflowExecution(ctx, client.ResetWorkflowExecutionOptions{WorkflowID: "reset-1"}); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("reset without a reset point err = %v", err)
	}
}

// Schedule：创建、读取、修改、暂停、恢复、列出、立即触发、删除。
func TestScheduleClient(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("greeting", greetingSrc)
	ctx := context.Background()
	schedules := env.client.ScheduleClient()
	handle, err := schedules.Create(ctx, client.ScheduleOptions{
		WorkflowType: "greeting", Args: []any{map[string]any{"name": "cron"}}, CronExpression: "0 0 * * *",
		TimeZone: "UTC", Description: "nightly", OverlapPolicy: client.ScheduleOverlapPolicyAllowAll, Paused: true,
	})
	must(t, err)
	description, err := handle.Describe(ctx)
	must(t, err)
	if description.ID != handle.GetID() || !description.Paused || description.WorkflowType != "greeting" || description.Input["name"] != "cron" {
		t.Fatalf("description = %+v", description)
	}
	text := "edited"
	must(t, handle.Update(ctx, client.ScheduleUpdateOptions{Description: &text}))
	must(t, handle.Unpause(ctx))
	description, err = handle.Describe(ctx)
	must(t, err)
	if description.Description != "edited" || description.Paused || description.NextRunAt == nil {
		t.Fatalf("after update and unpause = %+v", description)
	}
	must(t, handle.Pause(ctx))
	paused := true
	list, err := schedules.List(ctx, client.ScheduleListOptions{Paused: &paused})
	must(t, err)
	if list.Total != 1 || list.Schedules[0].ID != handle.GetID() {
		t.Fatalf("paused schedules = %+v", list)
	}
	run, err := handle.Trigger(ctx)
	must(t, err)
	var out greetingOut
	must(t, get(t, run, &out))
	if out.Greeting != "hello cron" || run.GetID() == "" {
		t.Fatalf("triggered run %s result = %+v", run.GetID(), out)
	}
	must(t, handle.Delete(ctx))
	if _, err := schedules.GetHandle(ctx, handle.GetID()).Describe(ctx); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("describe deleted schedule err = %v", err)
	}
}

// 批量操作：受理、读取进度、列出；结束后的批量操作不能再停止。
func TestBatchOperations(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("waiting", waitingSrc)
	ctx := context.Background()
	first := env.execute("waiting", "batch-1")
	second := env.execute("waiting", "batch-2")

	op, err := env.client.StartBatchOperation(ctx, client.StartBatchOperationOptions{Type: client.BatchOperationTypeTerminate, Reason: "cleanup"})
	must(t, err)
	if op.ID == "" || op.State != client.BatchOperationStatePending || op.Type != client.BatchOperationTypeTerminate {
		t.Fatalf("started batch = %+v", op)
	}
	env.server.Batches().ProcessActive(ctx)
	described, err := env.client.DescribeBatchOperation(ctx, op.ID)
	must(t, err)
	if described.State != client.BatchOperationStateCompleted || described.SucceededCount != 2 {
		t.Fatalf("batch = %+v", described)
	}
	for _, run := range []client.WorkflowRun{first, second} {
		var terminated *client.TerminatedError
		if err := get(t, run, nil); !errors.As(err, &terminated) || terminated.Reason != "cleanup" {
			t.Fatalf("run %s err = %v", run.GetID(), err)
		}
	}
	list, err := env.client.ListBatchOperations(ctx, client.ListBatchOperationsOptions{})
	must(t, err)
	if list.Total != 1 || list.Operations[0].ID != op.ID {
		t.Fatalf("batches = %+v", list)
	}
	if _, err := env.client.StopBatchOperation(ctx, op.ID); !errors.Is(err, client.ErrConflict) {
		t.Fatalf("stop a completed batch err = %v", err)
	}
	if _, err := env.client.DescribeBatchOperation(ctx, "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("describe missing batch err = %v", err)
	}
}

// Client 可以被多个 goroutine 并发使用。
func TestClientIsSafeForConcurrentUse(t *testing.T) {
	env := newTestEnv(t, true)
	env.register("greeting", greetingSrc)
	errs := make(chan error, 8)
	for i := range 8 {
		go func() {
			run, err := env.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{}, "greeting", map[string]any{"name": string(rune('a' + i))})
			if err == nil {
				err = get(t, run, nil)
			}
			errs <- err
		}()
	}
	for range 8 {
		must(t, <-errs)
	}
}

// waitFirstWorkflowTask 等 Run 完成第一个 Workflow Task：handler 已登记，取消请求会送到 Workflow 代码。
func waitFirstWorkflowTask(t *testing.T, env *testEnv, run client.WorkflowRun) {
	t.Helper()
	eventually(t, "first workflow task", func() bool {
		events := env.client.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID())
		for events.HasNext() {
			event, err := events.Next()
			if err != nil {
				return false
			}
			if event.EventType() == history.EventWorkflowTaskCompleted {
				return true
			}
		}
		return false
	})
}
