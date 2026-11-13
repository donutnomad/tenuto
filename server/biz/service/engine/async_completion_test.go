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

func TestServerCompleteActivityByIDClosesOpenActivityAndSchedulesWorkflowTask(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(8)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: memory.NewActivityQueueRouter(8)})

	run := &execution.Run{
		ID:                    "run-async-complete",
		WorkflowID:            "order-flow",
		WorkflowType:          "order-flow",
		WorkflowConfigVersion: "prog_v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "wait-payment"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：complete 权威读 state，需 seed 一个 started 状态供其读取。
	seedStartedActivityState(t, mgr, run.ID, scheduledID, "wait-payment")

	if err := server.CompleteActivityByID(ctx, run.ID, "wait-payment", map[string]any{"paid": true}, nil); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("expected one completed event, got %+v", history)
	}
	lease, err := workflowTasks.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.Type != protocol.TaskTypeWorkflow || lease.Task.RunID != run.ID {
		t.Fatalf("expected workflow task after completion, got %+v", lease.Task)
	}
}

// Error Case → Happy Path（严格 stamp 回归）：activity 经 reset/retry 后 state.Stamp 已非 1（这里=5）。
// CompleteActivityByID 必须从 state 读真实 stamp(5) 透传，否则严格校验下会因 st.Stamp(5)!=0 把
// 自己误判为 stale 丢弃、completion 不落库。本测试证明 CompleteActivityByID 不再被自己误丢弃。
func TestServerCompleteActivityByIDPassesRealStampUnderStrictValidation(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(8)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: memory.NewActivityQueueRouter(8)})

	run := &execution.Run{
		ID:                    "run-async-bumped-stamp",
		WorkflowID:            "order-flow",
		WorkflowType:          "order-flow",
		WorkflowConfigVersion: "prog_v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "wait-payment"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// state.Stamp=5（模拟多次 reset/retry 后的 generation），非 1 才能证明读的是真实 stamp 而非占位。
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID:            run.ID,
		ScheduledEventID: scheduledID,
		ActivityID:       "wait-payment",
		Attempt:          1,
		Stamp:            5,
		StartedEventID:   scheduledID,
		Status:           execution.ActivityStatusEnums.Started,
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.CompleteActivityByID(ctx, run.ID, "wait-payment", map[string]any{"paid": true}, nil); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("CompleteActivityByID must pass real state.Stamp(5) and complete; got %+v", history)
	}
	st, ok, err := mgr.GetActivityState(run.ID, scheduledID)
	if err != nil || !ok {
		t.Fatalf("state lookup failed: ok=%v err=%v", ok, err)
	}
	if st.Status != execution.ActivityStatusEnums.Completed {
		t.Fatalf("expected state completed under strict stamp, got %s", st.Status)
	}
}

func TestServerCompleteActivityByIDIsIdempotentForClosedActivity(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})

	run := &execution.Run{
		ID:                    "run-async-closed",
		WorkflowID:            "order-flow",
		WorkflowType:          "order-flow",
		WorkflowConfigVersion: "prog_v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "wait-payment"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{
		ScheduledEventID: scheduledID,
		Output:           map[string]any{"paid": true},
	})); err != nil {
		t.Fatal(err)
	}

	if err := server.CompleteActivityByID(ctx, run.ID, "wait-payment", map[string]any{"paid": true}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestServerCompleteActivityAcceptsTaskToken(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(8)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: memory.NewActivityQueueRouter(8)})

	run := &execution.Run{
		ID:                    "run-async-token",
		WorkflowID:            "order-flow",
		WorkflowType:          "order-flow",
		WorkflowConfigVersion: "prog_v1",
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "wait-payment"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	seedStartedActivityState(t, mgr, run.ID, scheduledID, "wait-payment")
	// 严格 stamp：token 携带与 seed state 一致的真实 stamp(1)，否则 complete 因 stamp 不符被丢弃。
	token, err := server.TaskTokens().Encode(protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "wait-payment",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := server.CompleteActivity(ctx, engine.ActivityTaskToken(token), map[string]any{"paid": true}, nil); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("expected one completed event from task token, got %+v", history)
	}
}

// seedStartedActivityState 写入一条 Started 状态的 ActivityState（Attempt=1/Stamp=1/真实 StartedEventID），
// 供 mutable-state 架构下"手动注入 ActivityStarted 事件 + 直接 complete/fail"的旧测试读取权威 state。
func seedStartedActivityState(t *testing.T, mgr execution.ExecutionManager, runID string, scheduledEventID int64, stepID string) {
	t.Helper()
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID:            runID,
		ScheduledEventID: scheduledEventID,
		ActivityID:       stepID,
		Attempt:          1,
		Stamp:            1,
		StartedEventID:   scheduledEventID, // 已写 history 的真实 started（非 transient）
		Status:           execution.ActivityStatusEnums.Started,
	}); err != nil {
		t.Fatal(err)
	}
}

func countEvents(history []historypkg.Event, typ historypkg.EventType) int {
	n := 0
	for i := range history {
		if history[i].EventType() == typ {
			n++
		}
	}
	return n
}
