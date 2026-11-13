package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 缺陷 6：async completion 的结构化失败
// ============================================
// 单入口语义：failure == nil = 成功，failure != nil = 失败（Message 可空）。
// 覆盖 token 路径与 ByID 路径。
// ============================================

const asyncFailureStepID = "wait-payment"

// seedAsyncCompletionRun 建一个 Running run + 一个 started activity，返回 server、mgr、scheduledEventID。
func seedAsyncCompletionRun(t *testing.T, runID string) (*engine.Server, execution.ExecutionManager, int64) {
	t.Helper()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	run := &execution.Run{
		ID:                    runID,
		WorkflowID:            "order-flow",
		WorkflowType:          "order-flow",
		WorkflowConfigVersion: "prog_v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: asyncFailureStepID}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	seedStartedActivityState(t, mgr, run.ID, scheduledID, asyncFailureStepID)
	return server, mgr, scheduledID
}

// singleActivityFailed 断言历史里恰好一条 ActivityFailed，返回它。
func singleActivityFailed(t *testing.T, history []historypkg.Event) historypkg.ActivityFailedEvent {
	t.Helper()
	var failed []historypkg.ActivityFailedEvent
	for i := range history {
		if history[i].ActivityFailed != nil {
			failed = append(failed, *history[i].ActivityFailed)
		}
	}
	if len(failed) != 1 {
		t.Fatalf("activity failed events = %d, want 1 (history=%+v)", len(failed), history)
	}
	return failed[0]
}

// Happy Path + Error Case（token 路径）：带 Type/NonRetryable 且 Message 为空的结构化失败
// 必须落成失败事件，不能被读成成功。
func TestServerCompleteActivityStructuredFailureByToken(t *testing.T) {
	ctx := context.Background()
	const runID = "run-async-fail-token"
	server, mgr, scheduledID := seedAsyncCompletionRun(t, runID)
	token, err := server.TaskTokens().Encode(protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            runID,
		ActivityID:       asyncFailureStepID,
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
	})
	if err != nil {
		t.Fatal(err)
	}

	failure := &protocol.Failure{Type: "PaymentDeclined", NonRetryable: true}
	if err := server.CompleteActivity(ctx, engine.ActivityTaskToken(token), nil, failure); err != nil {
		t.Fatalf("CompleteActivity: %v", err)
	}

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvents(history, historypkg.EventActivityCompleted); got != 0 {
		t.Fatalf("structured failure must not complete the activity, completed events = %d", got)
	}
	failed := singleActivityFailed(t, history)
	if failed.Failure == nil || failed.Failure.Type != "PaymentDeclined" || !failed.Failure.NonRetryable {
		t.Fatalf("failed event must keep the structured failure, got %+v", failed.Failure)
	}
	if failed.Error != "" {
		t.Fatalf("empty failure message must stay empty, got Error=%q", failed.Error)
	}
}

// Error Case（ByID 路径）：failure 决定失败 presence，空 Message 仍是失败。
func TestServerCompleteActivityByIDStructuredFailure(t *testing.T) {
	ctx := context.Background()
	const runID = "run-async-fail-byid"
	server, mgr, _ := seedAsyncCompletionRun(t, runID)

	failure := &protocol.Failure{Type: "PaymentDeclined", NonRetryable: true}
	if err := server.CompleteActivityByID(ctx, runID, asyncFailureStepID, nil, failure); err != nil {
		t.Fatalf("CompleteActivityByID: %v", err)
	}

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvents(history, historypkg.EventActivityCompleted); got != 0 {
		t.Fatalf("structured failure must not complete the activity, completed events = %d", got)
	}
	failed := singleActivityFailed(t, history)
	if failed.Failure == nil || failed.Failure.Type != "PaymentDeclined" {
		t.Fatalf("failed event must keep the structured failure, got %+v", failed.Failure)
	}
}

// Happy Path：failure == nil 是成功（两条路径都落 ActivityCompleted）。
func TestServerCompleteActivityNilFailureCompletes(t *testing.T) {
	ctx := context.Background()

	t.Run("token", func(t *testing.T) {
		const runID = "run-async-ok-token"
		server, mgr, scheduledID := seedAsyncCompletionRun(t, runID)
		token, err := server.TaskTokens().Encode(protocol.Task{
			Type:             protocol.TaskTypeActivity,
			RunID:            runID,
			ActivityID:       asyncFailureStepID,
			ScheduledEventID: scheduledID,
			Attempt:          1,
			Stamp:            1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := server.CompleteActivity(ctx, engine.ActivityTaskToken(token), map[string]any{"paid": true}, nil); err != nil {
			t.Fatalf("CompleteActivity: %v", err)
		}
		history, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if got := countEvents(history, historypkg.EventActivityCompleted); got != 1 {
			t.Fatalf("nil failure must complete, completed events = %d", got)
		}
	})

	t.Run("by id", func(t *testing.T) {
		const runID = "run-async-ok-byid"
		server, mgr, _ := seedAsyncCompletionRun(t, runID)
		if err := server.CompleteActivityByID(ctx, runID, asyncFailureStepID, map[string]any{"paid": true}, nil); err != nil {
			t.Fatalf("CompleteActivityByID: %v", err)
		}
		history, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if got := countEvents(history, historypkg.EventActivityCompleted); got != 1 {
			t.Fatalf("nil failure must complete, completed events = %d", got)
		}
	})
}
