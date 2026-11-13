package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestServerClosesWorkflowWithCompactFailureWhenCommandBatchExceedsHistoryLimit(t *testing.T) {
	mgr, err := memory.NewExecutionManagerWithHistoryLimits(history.HistoryLimits{
		WarningEvents: 2, WarningBytes: 1 << 20,
		HardEvents: 5, HardBytes: 2 << 20,
		ReservedCloseEvents: 2, ReservedCloseBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	engine.RegisterTestActivityWorker(t, server, "activity.first", "activity.second", "activity.any")
	run := &execution.Run{ID: "history-limit-close", WorkflowID: "flow", WorkflowType: "flow", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(engine.WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), run.ID, scheduledID), run.ID, scheduledID, []history.Command{
		{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "first", ActivityType: "activity.first"},
		{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: "second", ActivityType: "activity.second"},
	}, "worker")
	if err != nil {
		t.Fatal(err)
	}
	status, err := server.GetRunStatus(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != history.StatusFailed {
		t.Fatalf("status = %s", status)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var completed, failed, activities int
	for _, event := range history {
		if event.WorkflowTaskCompleted != nil {
			completed++
		}
		if event.WorkflowFailed != nil {
			failed++
		}
		if event.ActivityScheduled != nil {
			activities++
		}
	}
	if completed != 1 || failed != 1 || activities != 0 {
		t.Fatalf("compact close history = %+v", history)
	}
}

func openWorkflowTaskScheduledID(t *testing.T, mgr *memory.ExecutionManager, runID string) int64 {
	t.Helper()
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	var open int64
	for _, event := range events {
		switch {
		case event.WorkflowTaskScheduled != nil:
			open = event.ID
		case event.WorkflowTaskCompleted != nil:
			open = 0
		}
	}
	if open == 0 {
		t.Fatalf("run %s has no open workflow task", runID)
	}
	return open
}

// 子 Run 因 history 上限关闭时，父 Run 收到的失败带 HistoryLimitExceeded 类型。
func TestHistoryLimitCloseReportsFailureTypeToParent(t *testing.T) {
	ctx := context.Background()
	mgr, err := memory.NewExecutionManagerWithHistoryLimits(history.HistoryLimits{
		WarningEvents: 20, WarningBytes: 1 << 20,
		HardEvents: 30, HardBytes: 2 << 20,
		ReservedCloseEvents: 2, ReservedCloseBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
	engine.RegisterTestActivityWorker(t, server, "activity.first", "activity.second", "activity.any")
	t.Cleanup(server.Stop)
	childRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "history-limit-child", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parentRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: "history-limit-parent", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parentRunID, err := startedRunID(server.StartWorkflow(ctx, "", parentRef.WorkflowID, parentRef.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, parentRunID, openWorkflowTaskScheduledID(t, mgr, parentRunID)), parentRunID, openWorkflowTaskScheduledID(t, mgr, parentRunID), []history.Command{
		{Type: history.CmdStartChildWorkflow, ChildID: "child_1", WorkflowID: childRef.WorkflowID},
	}, "worker"); err != nil {
		t.Fatal(err)
	}
	states, err := mgr.ListChildWorkflowStates(parentRunID)
	if err != nil || len(states) != 1 || states[0].ChildRunID == "" {
		t.Fatalf("child states = %+v err=%v", states, err)
	}
	childRunID := states[0].ChildRunID

	commands := make([]history.Command, 0, 40)
	for i := range 40 {
		commands = append(commands, history.Command{Type: history.CmdScheduleActivity, StartToCloseTimeout: time.Minute, ActivityID: fmt.Sprintf("step_%d", i), ActivityType: "activity.any"})
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, childRunID, openWorkflowTaskScheduledID(t, mgr, childRunID)), childRunID, openWorkflowTaskScheduledID(t, mgr, childRunID), commands, "worker"); err != nil {
		t.Fatal(err)
	}
	if status, _ := server.GetRunStatus(childRunID); status != history.StatusFailed {
		t.Fatalf("child status = %s", status)
	}
	parentEvents, err := mgr.GetEvents(parentRunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range parentEvents {
		if failed := event.ChildWorkflowFailed; failed != nil {
			if failed.Failure == nil || failed.Failure.Type != "HistoryLimitExceeded" {
				t.Fatalf("parent must see failure type HistoryLimitExceeded, got %+v", failed.Failure)
			}
			return
		}
	}
	t.Fatalf("parent has no ChildWorkflowFailed event: %+v", parentEvents)
}
