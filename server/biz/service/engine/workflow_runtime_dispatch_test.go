package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestWorkflowRuntimeDispatchPendingTasksRecoversHistoryOutbox(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := &switchableTaskQueue{err: errors.New("workflow queue down")}
	activityTasks := &switchableTaskQueue{err: errors.New("activity queue down")}
	server := assembleServer(mgr, workflowTasks, activityTasks)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "wf-dispatch",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture", TaskQueue: "custodian"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowID, ref.WorkflowID, "", map[string]any{"order_id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	workflowTaskID := currentWorkflowTaskID(t, mgr, runID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, workflowTaskID), runID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "capture",
		ActivityType:        "custodian.Capture",
		TaskQueue:           "custodian",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 0 || len(activityTasks.tasks) != 0 {
		t.Fatalf("expected queues to stay empty while down, workflow=%d activity=%d", len(workflowTasks.tasks), len(activityTasks.tasks))
	}

	workflowTasks.err = nil
	activityTasks.err = nil
	result, err := server.DispatchPendingTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTasks != 0 || result.ActivityTasks != 1 {
		t.Fatalf("unexpected dispatch result: %+v", result)
	}
	if len(activityTasks.tasks) != 1 || activityTasks.tasks[0].Task.ActivityID != "capture" {
		t.Fatalf("expected recovered activity task, got %+v", activityTasks.tasks)
	}

	result, err = server.DispatchPendingTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTasks != 0 || result.ActivityTasks != 0 {
		t.Fatalf("expected dispatch state to skip duplicate delivery, got %+v", result)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one recovered activity task after duplicate scan, got %d", len(activityTasks.tasks))
	}
}

func TestWorkflowRuntimeDispatchPendingTasks(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := &switchableTaskQueue{err: errors.New("workflow queue down")}
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "wf-dispatch-http", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowID, ref.WorkflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 0 {
		t.Fatalf("expected workflow queue to stay empty while down, got %d", len(workflowTasks.tasks))
	}

	workflowTasks.err = nil
	result, err := server.DispatchPendingTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTasks != 1 || result.ActivityTasks != 0 {
		t.Fatalf("unexpected dispatch result: %+v", result)
	}
	if len(workflowTasks.tasks) != 1 || workflowTasks.tasks[0].Task.RunID != runID {
		t.Fatalf("expected recovered workflow task, got %+v", workflowTasks.tasks)
	}
}

func TestWorkflowRuntimeRegistersActivityWorkerVersion(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)

	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		BuildID:       "custodian@v1",
		Identity:      "custodian-worker-1",
		TaskQueues:    []string{"custodian"},
		ActivityTypes: []string{"custodian.Capture"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.HeartbeatActivityWorkerVersionInNamespace(ctx, protocol.DefaultNamespace, "custodian-worker-1", "custodian@v1"); err != nil {
		t.Fatal(err)
	}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "wf-http-worker-version",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-http-worker-version",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "capture",
		ActivityType:        "custodian.Capture",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	if task.TaskQueue != "custodian" {
		t.Fatalf("unexpected routed task: %+v", task)
	}
}

func currentWorkflowTaskID(t *testing.T, events execution.EventStore, runID string) int64 {
	t.Helper()
	history, err := events.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].EventType() == historypkg.EventWorkflowTaskScheduled {
			return history[i].ID
		}
	}
	t.Fatal("workflow task scheduled event missing")
	return 0
}

func TestWorkflowRuntimeDispatchPendingTasksSkipsTerminalRuns(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-terminal-dispatch", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusCompleted, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0)); err != nil {
		t.Fatal(err)
	}

	result, err := server.DispatchPendingTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkflowTasks != 0 || len(workflowTasks.tasks) != 0 {
		t.Fatalf("expected terminal run to skip dispatch, got result=%+v queue=%d", result, len(workflowTasks.tasks))
	}
}
