package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// scheduleFailureMatrixActivity schedules 一个带 retry 的 activity，返回 runID + scheduledEventID。
func scheduleFailureMatrixActivity(t *testing.T, srv *Server, mgr execution.ExecutionManager, runID string, retry *protocol.ParsedRetryPolicy) (string, int64) {
	t.Helper()
	seedRunningWorkflowWithActivity(t, srv, mgr, runID)
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, currentWFTScheduledID(t, srv, runID)), runID, currentWFTScheduledID(t, srv, runID), []historypkg.Command{{
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "settle",
		ActivityType:        "Settle",
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         retry,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	states, err := mgr.ListActivityStates(runID)
	if err != nil || len(states) != 1 {
		t.Fatalf("activity states = %d, err = %v", len(states), err)
	}
	return runID, states[0].ScheduledEventID
}

// awaitHistoryEvent 等到历史出现匹配事件，返回当前完整历史。
func awaitHistoryEvent(t *testing.T, mgr execution.ExecutionManager, runID string, match func(historypkg.Event) bool) []historypkg.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if match(event) {
				return events
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected event did not appear for run %s within 5s, history = %+v", runID, events)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func isActivityFailed(event historypkg.Event) bool { return event.ActivityFailed != nil }

func isWorkflowFailed(event historypkg.Event) bool { return event.WorkflowFailed != nil }

// failWorkflowTerminal 驱动 engine 内部终态路径。
//
// 本包测试没有 wlang worker 消费 Activity 失败后产生的 WorkflowTask，因此工作流终态必须
// 由测试显式驱动：先等 Activity 失败落库，再把该失败（或旧入口合成的 message-only 失败）
// 交给 engine 自己的 failWorkflow，走与生产一致的 WorkflowFailed 落库逻辑。
func failWorkflowTerminal(t *testing.T, mgr execution.ExecutionManager, srv *Server, runID string) {
	t.Helper()
	var failure *protocol.Failure
	for _, event := range awaitHistoryEvent(t, mgr, runID, isActivityFailed) {
		if event.ActivityFailed != nil {
			failure = event.ActivityFailed.Failure
		}
	}
	if failure == nil {
		t.Fatal("ActivityFailed event carries no failure payload")
	}
	if err := srv.failWorkflow(runID, *failure, ""); err != nil {
		t.Fatalf("failWorkflow: %v", err)
	}
}

func terminalFailureOf(t *testing.T, srv *Server, runID string) *protocol.Failure {
	t.Helper()
	read, err := srv.View().GetRunWithBlocked("", "", runID)
	if err != nil {
		t.Fatalf("GetRunWithBlocked: %v", err)
	}
	if read.Run == nil {
		t.Fatal("run missing from response")
	}
	return read.TerminalFailure
}

// Activity handler 通过 FailActivityTask 上报结构化失败：中间 attempt 事件、终态事件与
// Run 终态派生失败三处 Type 一致。
func TestAppFailureE2EStructured(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	srv := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	t.Cleanup(srv.Stop)
	runtime := activitytasks.NewUseCase(srv)
	ctx := context.Background()

	runID, schedID := scheduleFailureMatrixActivity(t, srv, mgr, "run-activity-failure-e2e", &protocol.ParsedRetryPolicy{
		MaxAttempts: 2, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1,
	})

	first := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := srv.HandleActivityStartedWithMetadata(first, 5*time.Second, 0); err != nil {
		t.Fatal(err)
	}
	retryable := protocol.Failure{
		Message: "gateway timeout", Type: "GatewayTimeout",
		Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`"card-1"`)}},
	}
	if err := runtime.FailActivityTask(ctx, first, retryable); err != nil {
		t.Fatalf("FailActivityTask attempt 1: %v", err)
	}

	second := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 2 })
	if _, err := srv.HandleActivityStartedWithMetadata(second, 5*time.Second, 0); err != nil {
		t.Fatal(err)
	}
	terminal := protocol.Failure{Message: "card declined", Type: "PaymentDeclined", NonRetryable: true}
	if err := runtime.FailActivityTask(ctx, second, terminal); err != nil {
		t.Fatalf("FailActivityTask attempt 2: %v", err)
	}

	failWorkflowTerminal(t, mgr, srv, runID)
	history := awaitHistoryEvent(t, mgr, runID, isWorkflowFailed)
	var attemptFailed, activityFailed bool
	for _, event := range history {
		if event.ActivityAttemptFailed != nil {
			attemptFailed = true
			if event.ActivityAttemptFailed.Failure == nil {
				t.Fatalf("ActivityAttemptFailed lost its failure: %+v", event.ActivityAttemptFailed)
			}
			if event.ActivityAttemptFailed.Failure.Type != "GatewayTimeout" || event.ActivityAttemptFailed.Failure.Message != "gateway timeout" {
				t.Fatalf("ActivityAttemptFailed failure = %+v", event.ActivityAttemptFailed.Failure)
			}
			if len(event.ActivityAttemptFailed.Failure.Details) != 1 || event.ActivityAttemptFailed.Failure.Details[0].Encoding != protocol.PayloadEncodingJSONPlainV1 {
				t.Fatalf("ActivityAttemptFailed details = %+v", event.ActivityAttemptFailed.Failure.Details)
			}
			if event.ActivityAttemptFailed.Error != "gateway timeout" {
				t.Fatalf("ActivityAttemptFailed error column = %q", event.ActivityAttemptFailed.Error)
			}
		}
		if event.ActivityFailed != nil {
			activityFailed = true
			if event.ActivityFailed.Failure == nil {
				t.Fatalf("ActivityFailed lost its failure: %+v", event.ActivityFailed)
			}
			if event.ActivityFailed.Failure.Type != "PaymentDeclined" || !event.ActivityFailed.Failure.NonRetryable {
				t.Fatalf("ActivityFailed failure = %+v", event.ActivityFailed.Failure)
			}
			if event.ActivityFailed.Error != "card declined" {
				t.Fatalf("ActivityFailed error column = %q", event.ActivityFailed.Error)
			}
		}
	}
	if !attemptFailed || !activityFailed {
		t.Fatalf("history missing attempt/terminal failure events: attemptFailed=%v activityFailed=%v", attemptFailed, activityFailed)
	}
	if schedID == 0 {
		t.Fatal("scheduled event id is zero")
	}

	derived := terminalFailureOf(t, srv, runID)
	if derived == nil {
		t.Fatal("failed run has no TerminalFailure")
	}
	if derived.Type != "PaymentDeclined" || !derived.NonRetryable || derived.Message != "card declined" {
		t.Fatalf("TerminalFailure = %+v", derived)
	}
}

// 旧入口 HandleActivityFailed（message-only）也落到 TerminalFailure，Type 保持空。
func TestAppFailureE2ELegacy(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	srv := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	t.Cleanup(srv.Stop)

	runID, schedID := scheduleFailureMatrixActivity(t, srv, mgr, "run-legacy-failure-e2e", &protocol.ParsedRetryPolicy{
		MaxAttempts: 1, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1,
	})
	task := waitForQueuedActivity(t, activityTasks, func(task protocol.Task) bool { return task.Attempt == 1 })
	if _, err := srv.HandleActivityStartedWithMetadata(task, 5*time.Second, 0); err != nil {
		t.Fatal(err)
	}
	if err := srv.HandleActivityFailed(context.Background(), runID, schedID, 1, 1, protocol.Failure{Message: "legacy boom"}); err != nil {
		t.Fatalf("HandleActivityFailed: %v", err)
	}

	failWorkflowTerminal(t, mgr, srv, runID)
	history := awaitHistoryEvent(t, mgr, runID, isWorkflowFailed)
	for _, event := range history {
		if event.WorkflowFailed == nil {
			continue
		}
		if event.WorkflowFailed.Failure == nil {
			t.Fatalf("WorkflowFailed lost its synthesized failure: %+v", event.WorkflowFailed)
		}
		if event.WorkflowFailed.Failure.Type != "" || event.WorkflowFailed.Failure.NonRetryable {
			t.Fatalf("message-only failure shape = %+v", event.WorkflowFailed.Failure)
		}
		if event.WorkflowFailed.Failure.Message != "legacy boom" || event.WorkflowFailed.Error != "legacy boom" {
			t.Fatalf("message-only scalars: %+v error=%q", event.WorkflowFailed.Failure, event.WorkflowFailed.Error)
		}
	}

	derived := terminalFailureOf(t, srv, runID)
	if derived == nil {
		t.Fatal("failed run has no TerminalFailure")
	}
	if derived.Type != "" || derived.NonRetryable || derived.Message != "legacy boom" {
		t.Fatalf("TerminalFailure = %+v", derived)
	}
}

// Workflow 终态失败：CmdFailWorkflow 携带结构化 Failure 时 TerminalFailure 字段完整。
func TestAppFailureE2ECmdFailWorkflow(t *testing.T) {
	mgr := memory.NewExecutionManager()
	srv := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	t.Cleanup(srv.Stop)

	runID, wftID := startContractRun(t, mgr, srv, contractWorkflowConfig("fail-command-e2e", nil))
	failure := protocol.Failure{
		Message: "card declined", Type: "PaymentDeclined", NonRetryable: true,
		Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`"card-1"`)}},
	}
	if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, []historypkg.Command{{
		Type: historypkg.CmdFailWorkflow, Error: failure.Message, WorkflowFailure: &failure,
	}}, ""); err != nil {
		t.Fatalf("HandleWorkflowTaskCompleted: %v", err)
	}

	history := awaitHistoryEvent(t, mgr, runID, isWorkflowFailed)
	var failed *historypkg.WorkflowFailedEvent
	for _, event := range history {
		if event.WorkflowFailed != nil {
			failed = event.WorkflowFailed
		}
	}
	if failed == nil || failed.Failure == nil {
		t.Fatalf("WorkflowFailed missing or unstructured: %+v", failed)
	}
	if failed.Failure.Type != "PaymentDeclined" || !failed.Failure.NonRetryable || failed.Failure.Message != "card declined" {
		t.Fatalf("WorkflowFailed.Failure = %+v", failed.Failure)
	}
	if failed.Error != "card declined" {
		t.Fatalf("WorkflowFailed error mirror = %q", failed.Error)
	}

	derived := terminalFailureOf(t, srv, runID)
	if derived == nil {
		t.Fatal("failed run has no TerminalFailure")
	}
	if derived.Type != "PaymentDeclined" || !derived.NonRetryable || derived.Message != "card declined" {
		t.Fatalf("TerminalFailure = %+v", derived)
	}
	if len(derived.Details) != 1 || derived.Details[0].Encoding != protocol.PayloadEncodingJSONPlainV1 {
		t.Fatalf("TerminalFailure details = %+v", derived.Details)
	}
}
