package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestServerRecoversActivityQueueEnqueueFailureFromHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := &switchableTaskQueue{err: errors.New("queue down")}
	server := assembleServer(mgr, workflowTasks, activityTasks)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "wf-queue-recover",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"step-1": {Symbol: "step-1", Type: "work"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-queue-recover",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, run)
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "step-1",
		ActivityType:        "work",
	}}, "")
	if err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 1 {
		t.Fatalf("expected one ActivityScheduled event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatalf("expected queue failure to leave activity open, got history %+v", history)
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("expected failed enqueue to leave activity queue empty, got %d", len(activityTasks.tasks))
	}

	activityTasks.err = nil
	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected recovered activity task, got %d", len(activityTasks.tasks))
	}
	history, err = mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	scheduledEventID := activityTasks.tasks[0].Task.ScheduledEventID
	if scheduledEventID == 0 {
		t.Fatalf("expected recovered task to carry scheduled event id: %+v", activityTasks.tasks[0].Task)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 1 {
		t.Fatalf("expected recovery to reuse ActivityScheduled event, got history %+v", history)
	}
}

func TestServerRecoversWorkflowQueueEnqueueFailureFromHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := &switchableTaskQueue{err: errors.New("workflow queue down")}
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	wf := &programpkg.WorkflowConfig{ID: "wf-workflow-queue-recover", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(ctx, "", wf.ID, wf.ID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 0 {
		t.Fatalf("expected failed workflow enqueue to leave queue empty, got %d", len(workflowTasks.tasks))
	}

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected one WorkflowTaskScheduled event, got history %+v", history)
	}

	workflowTasks.err = nil
	if err := server.RecoverRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected recovered workflow task, got %d", len(workflowTasks.tasks))
	}
	task := workflowTasks.tasks[0].Task
	if task.Type != protocol.TaskTypeWorkflow || task.RunID != runID || task.ScheduledEventID == 0 {
		t.Fatalf("unexpected recovered workflow task: %+v", task)
	}
}

func TestServerDoesNotCloseWorkflowTaskWhenCommandEventAppendFails(t *testing.T) {
	ctx := context.Background()
	mgr := newFailingEventManager()
	mgr.failOn(historypkg.EventActivityScheduled)
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-command-append-fails", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "will-fail",
		ActivityType:        "work",
	}}, "")
	if err == nil {
		t.Fatal("expected command append failure")
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskCompleted) != 0 {
		t.Fatalf("expected failed command append to keep workflow task open, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 0 {
		t.Fatalf("expected failed command append to skip activity schedule, got history %+v", history)
	}
}

func TestServerRejectsWorkflowTaskCompletedWithoutScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-missing-wft-complete", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), []historypkg.Command{{Type: historypkg.CmdCompleteWorkflow}}, "")
	if err == nil {
		t.Fatal("expected missing workflow task scheduled event error")
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskCompleted) != 0 || countEngineEvents(history, historypkg.EventWorkflowCompleted) != 0 {
		t.Fatalf("expected no workflow task close or terminal event, got history %+v", history)
	}
}

func TestServerRejectsWorkflowTaskTimedOutWithoutScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-missing-wft-timeout", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), "timeout")
	if err == nil {
		t.Fatal("expected missing workflow task scheduled event error")
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskTimedOut) != 0 {
		t.Fatalf("expected no workflow task timeout event, got history %+v", history)
	}
}

func TestServerCompletesWorkflowUpdateFromWorkflowTaskCommand(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-update-command", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowUpdateAcceptedEvent("set_priority", "update-1", updateArgs(map[string]any{"priority": "high"}))); err != nil {
		t.Fatal(err)
	}
	workflowTaskID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:          historypkg.CmdCompleteWorkflowUpdate,
		Name:          "set_priority",
		UpdateID:      "update-1",
		Output:        map[string]any{"accepted": true},
		UpdateOutcome: updateSuccessOutcome(t, map[string]any{"accepted": true}),
	}}, "")
	if err != nil {
		t.Fatal(err)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowUpdateCompleted) != 1 {
		t.Fatalf("expected one update completion, got %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 || len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected next workflow task after update completion, history=%+v queue=%d", history, len(workflowTasks.tasks))
	}
}

func TestServerPersistsWorkflowUpdateOutcome(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "run-update-outcome", WorkflowID: "wf-update-outcome", WorkflowType: "wf-update-outcome", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID,
		historypkg.NewWorkflowUpdateAcceptedEvent("create", "update-1", nil)); err != nil {
		t.Fatal(err)
	}
	workflowTaskID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.NewJSONPayload([]string{"created", "pending_created"})
	if err != nil {
		t.Fatal(err)
	}
	outcome := protocol.UpdateOutcome{Success: &payload}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:          historypkg.CmdCompleteWorkflowUpdate,
		Name:          "create",
		UpdateID:      "update-1",
		UpdateOutcome: &outcome,
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range history {
		if completed := event.WorkflowUpdateCompleted; completed != nil {
			if completed.Outcome == nil || completed.Outcome.Success == nil || completed.Outcome.Failure != nil {
				t.Fatalf("unexpected v2 completion: %+v", completed)
			}
			return
		}
	}
	t.Fatalf("v2 completion missing: %+v", history)
}

func TestServerDeduplicatesWorkflowTaskLifecycleByScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-workflow-task-lifecycle", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks,
		protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}},
		protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}},
	)

	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), nil, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatalf("expected one WorkflowTaskStarted event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskCompleted) != 1 {
		t.Fatalf("expected one WorkflowTaskCompleted event, got history %+v", history)
	}
	for _, event := range history {
		if event.WorkflowTaskCompleted != nil && event.WorkflowTaskCompleted.ScheduledEventID != scheduledEventID {
			t.Fatalf("expected WorkflowTaskCompleted to link scheduled event %d, got %+v", scheduledEventID, event)
		}
	}
}

func TestServerSkipsWorkflowTaskStartedAfterTerminalHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-terminal-workflow-task-poll", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTerminatedEvent("manual")); err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})

	// 陈旧投递由 Server 确认丢弃，不交给 Worker：PollWorkflowTask 继续取下一份，队列已空。
	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); !errors.Is(err, errTestTaskQueueEmpty) {
		t.Fatalf("poll err = %v, want the empty queue", err)
	}
	if acked := workflowTasks.ackedLeases(); len(acked) != 1 || acked[0].Task.ScheduledEventID != scheduledEventID {
		t.Fatalf("acked = %+v, want the stale delivery acknowledged once", acked)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 0 {
		t.Fatalf("expected terminal history to skip WorkflowTaskStarted, got history %+v", history)
	}
}

func TestServerSkipsWorkflowTaskStartedForClosedWorkflowTask(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-closed-workflow-task-poll", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mgr.AppendWorkflowTaskClosed(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskCompletedEvent(scheduledEventID, "")); err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})

	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); !errors.Is(err, errTestTaskQueueEmpty) {
		t.Fatalf("poll err = %v, want the empty queue", err)
	}
	if acked := workflowTasks.ackedLeases(); len(acked) != 1 || acked[0].Task.ScheduledEventID != scheduledEventID {
		t.Fatalf("acked = %+v, want the stale delivery acknowledged once", acked)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 0 {
		t.Fatalf("expected closed workflow task to skip WorkflowTaskStarted, got history %+v", history)
	}
}

func TestServerRecordsWorkflowTaskStartedWorkerInfo(t *testing.T) {
	ctx := protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{Identity: "workflow-app-1", BuildID: "workflow-build-a", WorkflowCapabilities: programpkg.CurrentWLangCapabilities()})
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-wft-worker-info", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})

	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started historypkg.Event
	for _, event := range history {
		if event.EventType() == historypkg.EventWorkflowTaskStarted {
			started = event
		}
	}
	if started.WorkflowTaskStarted == nil || started.WorkflowTaskStarted.WorkerIdentity != "workflow-app-1" || started.WorkflowTaskStarted.WorkerBuildID != "workflow-build-a" {
		t.Fatalf("expected worker metadata on WorkflowTaskStarted, got %+v", started)
	}
}

func TestServerRequeuesV2WorkflowUntilCapableWorkerPolls(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := &switchableTaskQueue{}
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	program := programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		Program:          []any{map[string]any{"return": nil}},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:         "v2-workflow",
		Program:    program,
		Activities: map[string]programpkg.ActivityRef{},
	}); err != nil {
		t.Fatal(err)
	}
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", "v2-workflow", "v2-workflow", "", nil))
	if err != nil {
		t.Fatal(err)
	}

	_, err = server.PollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "old-worker", BuildID: "old-build", WorkflowCapabilities: []string{},
	}))
	var unsupported *programpkg.UnsupportedWLangCapabilityError
	if !errors.As(err, &unsupported) {
		t.Fatalf("old worker error = %v", err)
	}
	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 0 {
		t.Fatalf("old worker started v2 workflow: %+v", history)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("requeued tasks = %d", len(workflowTasks.tasks))
	}

	lease, err := server.PollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "new-worker", BuildID: "new-build", WorkflowCapabilities: programpkg.CurrentWLangCapabilities(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.RunID != runID {
		t.Fatalf("new worker lease = %+v", lease)
	}
	history, err = mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatalf("new worker history = %+v", history)
	}
}

func TestServerPollWorkflowTaskReturnsStartedEventWriteError(t *testing.T) {
	ctx := context.Background()
	mgr := newFailingEventManager()
	mgr.failOn(historypkg.EventWorkflowTaskStarted)
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-wft-start-error", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})

	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); !errors.Is(err, errAppendEventFailed) {
		t.Fatalf("PollWorkflowTask() err = %v, want the workflow task started append error", err)
	}
}

func TestServerDispatchesActivityTaskWithTaskToken(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)

	run := &execution.Run{ID: "run-activity-task-token", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)

	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "capture",
		ActivityType:        "custodian.Capture",
		TaskQueue:           "custodian",
	}}, ""); err != nil {
		t.Fatal(err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	if task.TaskToken == "" {
		t.Fatalf("expected activity task token, got %+v", task)
	}
	decoded, err := testTaskTokens().Decode(task.TaskToken)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != protocol.TaskTypeActivity || decoded.RunID != run.ID || decoded.ScheduledEventID != task.ScheduledEventID || decoded.Attempt != 1 {
		t.Fatalf("unexpected decoded task token: %+v", decoded)
	}
}

func TestServerReusesOpenWorkflowTaskWhenSchedulingAgain(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-open-workflow-task", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	openScheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.scheduleWorkflowTask(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected schedule to reuse open workflow task, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected one workflow task enqueue, got %d", len(workflowTasks.tasks))
	}
	task := workflowTasks.tasks[0].Task
	if task.ScheduledEventID != openScheduledEventID {
		t.Fatalf("expected task to reuse scheduled event %d, got %+v", openScheduledEventID, task)
	}
}

func TestServerDeduplicatesConcurrentWorkflowTaskScheduleByEventStore(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-concurrent-workflow-task-schedule", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- server.scheduleWorkflowTask(ctx, run.ID)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected one WorkflowTaskScheduled event, got history %+v", history)
	}
	for _, lease := range workflowTasks.tasks {
		if lease.Task.ScheduledEventID != history[0].ID {
			t.Fatalf("expected queued task to use deduped event %d, got %+v", history[0].ID, lease.Task)
		}
	}
}

func TestServerIgnoresWorkflowTaskCompletionAfterTimeout(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-late-workflow-task", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})
	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, "timeout"); err != nil {
		t.Fatal(err)
	}

	err = server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "late-step",
		ActivityType:        "work",
	}}, "")
	if err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 0 {
		t.Fatalf("expected timed-out workflow task completion to be ignored, got history %+v", history)
	}
	if len(activityTasks.tasks) != 0 {
		t.Fatalf("expected no activity task after timed-out workflow task completion, got %d", len(activityTasks.tasks))
	}
}

func TestServerSchedulesNewWorkflowTaskAfterWorkflowTaskTimeout(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-workflow-task-timeout-retry", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})
	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, "timeout"); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskTimedOut) != 1 {
		t.Fatalf("expected one WorkflowTaskTimedOut event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected a new WorkflowTaskScheduled event after timeout, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected one new workflow task, got %d", len(workflowTasks.tasks))
	}
	newTask := workflowTasks.tasks[0].Task
	if newTask.ScheduledEventID == 0 || newTask.ScheduledEventID == scheduledEventID {
		t.Fatalf("expected new scheduled event id, got %+v", newTask)
	}
}

func TestServerDeduplicatesDuplicateWorkflowTaskTimeoutByScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-duplicate-workflow-task-timeout", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	registerRunWorkflowForTest(t, server, run)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks.tasks = append(workflowTasks.tasks, protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: run.ID, ScheduledEventID: scheduledEventID}})
	if _, err := server.PollWorkflowTask(capableWorkerContext(ctx)); err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, "timeout"); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskTimedOut) != 1 {
		t.Fatalf("expected duplicate timeout to close only original task, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected one replacement workflow task, got history %+v", history)
	}
}

func TestServerIgnoresWorkflowTaskTimeoutAfterTerminalHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-terminal-workflow-task-timeout", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCancelledEvent(nil)); err != nil {
		t.Fatal(err)
	}

	if err := server.HandleWorkflowTaskTimedOut(server.WorkflowTaskContext(ctx, run.ID, scheduledEventID), run.ID, scheduledEventID, "timeout"); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskTimedOut) != 0 {
		t.Fatalf("expected terminal history to ignore workflow task timeout, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 0 {
		t.Fatalf("expected terminal history to skip replacement workflow task, got %d", len(workflowTasks.tasks))
	}
}

func TestServerSkipsDuplicateScheduleActivityCommand(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-duplicate-command", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	cmd := historypkg.Command{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "step-1",
		ActivityType:        "work",
	}
	firstWorkflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, firstWorkflowTaskID), run.ID, firstWorkflowTaskID, []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	secondWorkflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, secondWorkflowTaskID), run.ID, secondWorkflowTaskID, []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityScheduled) != 1 {
		t.Fatalf("expected one ActivityScheduled event, got history %+v", history)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one activity task, got %d", len(activityTasks.tasks))
	}
	if activityTasks.tasks[0].Task.Attempt != 1 {
		t.Fatalf("expected first activity task attempt 1, got %+v", activityTasks.tasks[0].Task)
	}
}

func TestServerSkipsDuplicateTerminalWorkflowCommand(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-terminal-command", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	cmd := historypkg.Command{Type: historypkg.CmdCompleteWorkflow, Output: map[string]any{"ok": true}}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowCompleted) != 1 {
		t.Fatalf("expected one WorkflowCompleted event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskCompleted) != 1 {
		t.Fatalf("expected one WorkflowTaskCompleted event, got history %+v", history)
	}
}

func TestServerScheduleTimerCommandFiresAndSchedulesWorkflowTask(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-sleep-timer", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:    historypkg.CmdScheduleTimer,
		TimerID: "timer_1",
		Delay:   5 * time.Millisecond,
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventTimerScheduled) != 1 {
		t.Fatalf("expected one TimerScheduled event, got history %+v", history)
	}

	waitForEngineEventCount(t, mgr, run.ID, historypkg.EventTimerFired, 1, 100*time.Millisecond)
	waitForEngineEventCount(t, mgr, run.ID, historypkg.EventWorkflowTaskScheduled, 2, 100*time.Millisecond)
}

func TestServerRecordMarkerCommandSchedulesNextWorkflowTask(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	run := &execution.Run{ID: "run-marker", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:       historypkg.CmdRecordMarker,
		MarkerName: "GetVersion",
		MarkerID:   "change-a",
		Input:      map[string]any{"version": int64(3)},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventMarkerRecorded) != 1 {
		t.Fatalf("expected one MarkerRecorded event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected marker to schedule next workflow task, got history %+v", history)
	}
}

func TestServerStartChildWorkflowCommandCreatesChildRunAndPropagatesCompletion(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	parentRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "parent-flow", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	childRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "child-flow", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parent := &execution.Run{
		ID:                    "run-parent-child",
		WorkflowID:            parentRef.WorkflowID,
		WorkflowType:          parentRef.WorkflowID,
		WorkflowConfigVersion: parentRef.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(parent)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, parent.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, parent.ID, workflowTaskID), parent.ID, workflowTaskID, []historypkg.Command{{
		Type:       historypkg.CmdStartChildWorkflow,
		ChildID:    "child_1",
		WorkflowID: childRef.WorkflowID,
		Input:      map[string]any{"order_id": "o-1"},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	parentHistory, err := mgr.GetEvents(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(parentHistory, historypkg.EventChildWorkflowStarted) != 1 {
		t.Fatalf("expected child start event in parent history, got %+v", parentHistory)
	}
	childRunID := ""
	childScheduledEventID := int64(0)
	for _, event := range parentHistory {
		if event.ChildWorkflowStarted != nil {
			childRunID = event.ChildWorkflowStarted.ChildRunID
			childScheduledEventID = event.ID
			if event.ChildWorkflowStarted.WorkflowConfigVersion != childRef.Version {
				t.Fatalf("child start event should pin config version %s, got %+v", childRef.Version, event)
			}
		}
	}
	if childRunID == "" || childScheduledEventID == 0 {
		t.Fatalf("expected child run metadata in parent history %+v", parentHistory)
	}
	child, err := mgr.Get(childRunID)
	if err != nil {
		t.Fatal(err)
	}
	if child.WorkflowID != execution.ChildWorkflowID(parent.ID, "child_1") || child.WorkflowType != childRef.WorkflowID || child.ParentRunID != parent.ID || child.ParentChildID != "child_1" || child.ParentScheduledEventID != childScheduledEventID {
		t.Fatalf("unexpected child run: %+v", child)
	}
	if child.WorkflowConfigVersion != childRef.Version {
		t.Fatalf("child run should use pinned config version %s, got %+v", childRef.Version, child)
	}
	childHistory, err := mgr.GetEvents(childRunID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(childHistory, historypkg.EventWorkflowStarted) != 1 || countEngineEvents(childHistory, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected child run start and WFT schedule, got %+v", childHistory)
	}

	if err := server.completeWorkflow(childRunID, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	parentHistory, err = mgr.GetEvents(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(parentHistory, historypkg.EventChildWorkflowCompleted) != 1 {
		t.Fatalf("expected child completion event in parent history, got %+v", parentHistory)
	}
	if countEngineEvents(parentHistory, historypkg.EventWorkflowTaskScheduled) != 2 {
		t.Fatalf("expected child completion to schedule parent WFT, got %+v", parentHistory)
	}
}

func TestServerContinueAsNewCommandCreatesNewRunAndClosesCurrent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "rolling-flow", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-continue-as-new",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-continue-as-new",
		Status:                historypkg.StatusRunning,
		IsCurrent:             true, // 旧 run 是 current（CAN 切代 expectedOld=旧run 需其为 current）
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:       historypkg.CmdContinueAsNew,
		WorkflowID: ref.WorkflowID,
		Input:      map[string]any{"page": int64(2)},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	oldRun, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if oldRun.Status != historypkg.StatusContinuedAsNew {
		t.Fatalf("expected old run continued-as-new, got %+v", oldRun)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowContinuedAsNew) != 1 {
		t.Fatalf("expected continue-as-new event, got %+v", history)
	}
	newRunID := ""
	for _, event := range history {
		if event.WorkflowContinuedAsNew != nil {
			newRunID = event.WorkflowContinuedAsNew.NextRunID
			if event.WorkflowContinuedAsNew.WorkflowConfigVersion != ref.Version {
				t.Fatalf("continue-as-new event should pin config version %s, got %+v", ref.Version, event)
			}
		}
	}
	if newRunID == "" {
		t.Fatalf("expected new run id in continue event %+v", history)
	}
	newRun, err := mgr.Get(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if newRun.Status != historypkg.StatusRunning || newRun.Input["page"] != int64(2) {
		t.Fatalf("unexpected continued run: %+v", newRun)
	}
	if newRun.WorkflowConfigVersion != ref.Version {
		t.Fatalf("continued run should use pinned config version %s, got %+v", ref.Version, newRun)
	}
	if newRun.FirstRunID != run.ID {
		t.Fatalf("continued run FirstRunID should be %s (chain head), got %s", run.ID, newRun.FirstRunID)
	}
	if newRun.ContinuedRunID != run.ID {
		t.Fatalf("continued run ContinuedRunID should point back to %s, got %s", run.ID, newRun.ContinuedRunID)
	}
	// 主动 CAN 的 attempt 重置为 1（对标 Temporal：仅 RETRY initiator 累加，WORKFLOW 重置）
	if newRun.Attempt != 1 {
		t.Fatalf("continued run Attempt should reset to 1 (active CAN, not retry), got %d", newRun.Attempt)
	}
	newHistory, err := mgr.GetEvents(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(newHistory, historypkg.EventWorkflowStarted) != 1 || countEngineEvents(newHistory, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected new run start and WFT schedule, got %+v", newHistory)
	}
	for _, event := range newHistory {
		if event.WorkflowStarted != nil {
			if event.WorkflowStarted.ContinuedRunID != run.ID {
				t.Fatalf("WorkflowStarted event ContinuedRunID should be %s, got %s", run.ID, event.WorkflowStarted.ContinuedRunID)
			}
			if event.WorkflowStarted.FirstRunID != run.ID {
				t.Fatalf("WorkflowStarted event FirstRunID should be %s, got %s", run.ID, event.WorkflowStarted.FirstRunID)
			}
			if event.WorkflowStarted.Attempt != 1 {
				t.Fatalf("WorkflowStarted event Attempt should be 1 (active CAN), got %d", event.WorkflowStarted.Attempt)
			}
		}
	}
}

func TestServerContinueAsNewDefaultsToWorkflowTypeAndPreservesExecutionID(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "approval-workflow", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID: "run-custom-execution", Namespace: protocol.DefaultNamespace,
		WorkflowID: "create-rec-1", WorkflowType: ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-custom-execution", Status: historypkg.StatusRunning,
		IsCurrent: true, CreatedAt: time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type: historypkg.CmdContinueAsNew, Input: map[string]any{"page": int64(2)},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var nextRunID string
	for _, event := range history {
		if event.WorkflowContinuedAsNew != nil {
			nextRunID = event.WorkflowContinuedAsNew.NextRunID
		}
	}
	if nextRunID == "" {
		t.Fatalf("continue-as-new event missing: %+v", history)
	}
	next, err := mgr.Get(nextRunID)
	if err != nil {
		t.Fatal(err)
	}
	if next.WorkflowID != "create-rec-1" || next.WorkflowType != "approval-workflow" {
		t.Fatalf("continued identity = %+v", next)
	}
}

func TestServerContinueAsNewInheritsSearchAttributes(t *testing.T) {
	// Happy Path: 主动 CAN 的新 run 应继承旧 run 的 SearchAttributes（对标 Temporal 默认继承）
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "can-sa-flow", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-can-sa",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-can-sa",
		Status:                historypkg.StatusRunning,
		SearchAttributes:      map[string]string{"env": "prod", "tier": "gold"},
		IsCurrent:             true, // 旧 run 是 current（CAN 切代 expectedOld=旧run 需其为 current）
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:       historypkg.CmdContinueAsNew,
		WorkflowID: ref.WorkflowID,
		Input:      map[string]any{"page": int64(2)},
	}}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	newRunID := ""
	for _, event := range history {
		if event.WorkflowContinuedAsNew != nil {
			newRunID = event.WorkflowContinuedAsNew.NextRunID
		}
	}
	if newRunID == "" {
		t.Fatalf("expected new run id in continue event %+v", history)
	}
	newRun, err := mgr.Get(newRunID)
	if err != nil {
		t.Fatal(err)
	}
	if newRun.SearchAttributes["env"] != "prod" || newRun.SearchAttributes["tier"] != "gold" {
		t.Fatalf("continued run should inherit SearchAttributes, got %+v", newRun.SearchAttributes)
	}
}

func TestServerRecoverySchedulesWorkflowTaskAfterActivityCloseEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "wf-recover-after-activity-close",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"capture": {Symbol: "capture", Type: "custodian.Capture"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-after-activity-close",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "capture", ActivityType: "custodian.Capture"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{ScheduledEventID: scheduledID, Output: map[string]any{"ok": true}})); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected recovered workflow task, got %d", len(workflowTasks.tasks))
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("expected one recovered WorkflowTaskScheduled event, got %+v", history)
	}
}

func TestServerRecoverySchedulesWorkflowTaskAfterMarkerEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "wf-recover-after-marker", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-after-marker",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	wftID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskCompletedEvent(wftID, "")); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewMarkerRecordedEvent("SideEffect", "load", map[string]any{"ok": true})); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected recovered workflow task, got %d", len(workflowTasks.tasks))
	}
}

func TestServerRecoveryCreatesMissingChildRunFromHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	parentRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "parent-recover-child", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	childRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "child-recover-child",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"old-step": {Symbol: "old-step", Type: "old.Activity"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	parent := &execution.Run{
		ID:                    "run-parent-recover-child",
		WorkflowID:            parentRef.WorkflowID,
		WorkflowType:          parentRef.WorkflowID,
		WorkflowConfigVersion: parentRef.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(parent)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(parent.ID, parent.ID, parent.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	childStartedEventID, err := mgr.Append(parent.ID, parent.ID, parent.ID, historypkg.NewChildWorkflowStartedEvent("child-recover-child", "child_1", "run-child-recovered", childRef.Version, map[string]any{"request": "child"}))
	if err != nil {
		t.Fatal(err)
	}
	// 补写 ChildWorkflowState（新实现从 state 恢复，不再扫父 history）。
	if err := mgr.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID:                 parent.ID,
		ScheduledEventID:      childStartedEventID,
		ChildID:               "child_1",
		ChildRunID:            "run-child-recovered",
		WorkflowID:            childRef.WorkflowID,
		WorkflowConfigVersion: childRef.Version,
		Status:                execution.ChildWorkflowStatusEnums.Initiated,
		Input:                 map[string]any{"request": "child"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "child-recover-child",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"new-step": {Symbol: "new-step", Type: "new.Activity"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	child, err := mgr.Get("run-child-recovered")
	if err != nil || child.ParentRunID != parent.ID || child.ParentChildID != "child_1" {
		t.Fatalf("expected child run to be recovered, got %+v", child)
	}
	if child.WorkflowConfigVersion != childRef.Version {
		t.Fatalf("recovered child should use history config version %s, got %+v", childRef.Version, child)
	}
	childHistory, err := mgr.GetEvents("run-child-recovered")
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(childHistory, historypkg.EventWorkflowStarted) != 1 || countEngineEvents(childHistory, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("unexpected child history: %+v", childHistory)
	}
}

func TestServerRecoveryPropagatesClosedChildWorkflowToParent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	parentRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "parent-recover-child-close", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	childRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "child-recover-child-close", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	parent := &execution.Run{
		ID:                    "run-parent-recover-child-close",
		WorkflowID:            parentRef.WorkflowID,
		WorkflowType:          parentRef.WorkflowID,
		WorkflowConfigVersion: parentRef.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(parent)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(parent.ID, parent.ID, parent.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	childStartedID, err := mgr.Append(parent.ID, parent.ID, parent.ID, historypkg.NewChildWorkflowStartedEvent(childRef.WorkflowID, "child_1", "run-child-closed-before-parent-event", "", map[string]any{"request": "child"}))
	if err != nil {
		t.Fatal(err)
	}
	child := &execution.Run{
		ID:                     "run-child-closed-before-parent-event",
		WorkflowID:             childRef.WorkflowID,
		WorkflowType:           childRef.WorkflowID,
		WorkflowConfigVersion:  childRef.Version,
		ParentRunID:            parent.ID,
		ParentChildID:          "child_1",
		ParentScheduledEventID: childStartedID,
		Status:                 historypkg.StatusCompleted,
		Output:                 map[string]any{"ok": true},
		CreatedAt:              time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(child)) }); err != nil {
		t.Fatal(err)
	}
	// 父侧 ChildWorkflowState 与 ChildWorkflowStarted 同事务落库；启动恢复只扫 Running 的 run，
	// 已终态的子 run 由父 run 的 open child state 找到并补传播。
	if err := mgr.UpsertChildWorkflowState(&execution.ChildWorkflowState{
		RunID: parent.ID, ScheduledEventID: childStartedID,
		ChildID: "child_1", ChildRunID: child.ID,
		WorkflowID: childRef.WorkflowID, WorkflowConfigVersion: childRef.Version,
		Status: execution.ChildWorkflowStatusEnums.Started, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(child.ID, child.ID, child.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(child.ID, child.ID, child.ID, historypkg.NewWorkflowCompletedEvent(child.Output)); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRunningWorkflows(ctx); err != nil {
		t.Fatal(err)
	}

	parentHistory, err := mgr.GetEvents(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(parentHistory, historypkg.EventChildWorkflowCompleted) != 1 {
		t.Fatalf("expected child completion to be propagated, got %+v", parentHistory)
	}
	if len(workflowTasks.tasks) == 0 {
		t.Fatal("expected parent workflow task after recovered child completion")
	}
}

func TestServerRecoveryCreatesMissingContinueAsNewRunFromHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "recover-continue",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"old-step": {Symbol: "old-step", Type: "old.Activity"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-continue",
		WorkflowID:            "recover-execution-1",
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		FirstRunID:            "run-recover-continue",
		Status:                historypkg.StatusRunning,
		IsCurrent:             true, // 旧 run 是 current（recovery 重建 CAN 切代 expectedOld=旧run 需其为 current）
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowContinuedAsNewEvent(ref.WorkflowID, "run-recovered-continue-next", ref.Version, map[string]any{"generation": int64(2)})); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "recover-continue",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"new-step": {Symbol: "new-step", Type: "new.Activity"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	next, err := mgr.Get("run-recovered-continue-next")
	if err != nil || next.WorkflowID != "recover-execution-1" || next.WorkflowType != "recover-continue" {
		t.Fatalf("expected continue-as-new run to be recovered, got %+v", next)
	}
	if next.WorkflowConfigVersion != ref.Version {
		t.Fatalf("recovered continue-as-new run should use history config version %s, got %+v", ref.Version, next)
	}
	if next.FirstRunID != run.ID {
		t.Fatalf("recovered run FirstRunID should be %s, got %s", run.ID, next.FirstRunID)
	}
	if next.ContinuedRunID != run.ID {
		t.Fatalf("recovered run ContinuedRunID should be %s, got %s", run.ID, next.ContinuedRunID)
	}
	nextHistory, err := mgr.GetEvents("run-recovered-continue-next")
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(nextHistory, historypkg.EventWorkflowStarted) != 1 || countEngineEvents(nextHistory, historypkg.EventWorkflowTaskScheduled) != 1 {
		t.Fatalf("unexpected next history: %+v", nextHistory)
	}
}

func TestServerRecoverRunFiresExpiredTimerFromHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	firstServer := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	wf := &programpkg.WorkflowConfig{ID: "wf", Program: programtest.Noop()}
	ref, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}

	run := &execution.Run{ID: "run-recover-sleep-timer", WorkflowID: "wf", WorkflowType: "wf", WorkflowConfigVersion: ref.Version, Status: historypkg.StatusRunning}
	createStartedRunForTest(t, mgr, run)
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := firstServer.HandleWorkflowTaskCompleted(firstServer.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:    historypkg.CmdScheduleTimer,
		TimerID: "timer_1",
		Delay:   30 * time.Millisecond,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	firstServer.Stop()
	time.Sleep(40 * time.Millisecond)

	recoveredServer := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	if _, err := recoveredServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	defer recoveredServer.Stop()
	if err := recoveredServer.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	waitForEngineEventCount(t, mgr, run.ID, historypkg.EventTimerFired, 1, 100*time.Millisecond)
	waitForEngineEventCount(t, mgr, run.ID, historypkg.EventWorkflowTaskScheduled, 2, 100*time.Millisecond)
}

func TestServerFailWorkflowCancelsOpenActivityTimers(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-fail-cancels-activity", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:                   historypkg.CmdScheduleActivity,
		ActivityID:             "open-step",
		ActivityType:           "work",
		ScheduleToCloseTimeout: time.Hour,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var scheduledEventID int64
	for _, event := range history {
		if event.ActivityScheduled != nil {
			scheduledEventID = event.ID
		}
	}
	if scheduledEventID == 0 {
		t.Fatalf("expected open activity in history %+v", history)
	}
	if !activityTimersArmed(server, run.ID, scheduledEventID) {
		t.Fatal("expected activity timer before workflow failure")
	}

	if err := server.failWorkflow(run.ID, protocol.Failure{Message: "workflow failed"}, ""); err != nil {
		t.Fatal(err)
	}

	if activityTimersArmed(server, run.ID, scheduledEventID) {
		t.Fatal("expected workflow failure to clear open activity timers")
	}
}

func TestServerDeduplicatesTerminalWorkflowCommandByEventStore(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-stale-terminal", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}

	cmd := historypkg.Command{Type: historypkg.CmdCompleteWorkflow, Output: map[string]any{"ok": true}}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, server.OpenWorkflowTaskID(run.ID)), run.ID, server.OpenWorkflowTaskID(run.ID), []historypkg.Command{cmd}, ""); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowCompleted) != 1 {
		t.Fatalf("expected one WorkflowCompleted event, got history %+v", history)
	}
}

func TestServerGetRunStatusUsesTerminalEventHistory(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "run-terminal-history-status", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowFailedEvent("boom", "")); err != nil {
		t.Fatal(err)
	}

	status, err := server.GetRunStatus(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != historypkg.StatusFailed {
		t.Fatalf("expected status from terminal history, got %s", status)
	}
}

func TestServerGetRunStatusKeepsFirstTerminalEvent(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	run := &execution.Run{ID: "run-terminal-precedence", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCompletedEvent(map[string]any{"ok": true})); err != nil {
		t.Fatal(err)
	}

	status, err := server.GetRunStatus(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != historypkg.StatusCompleted {
		t.Fatalf("expected first terminal event to decide status, got %s", status)
	}
}

func TestServerRecoverRunReconcilesTerminalEventStatus(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "wf-reconcile-terminal", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-reconcile-terminal",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	output := map[string]any{"ok": true}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCompletedEvent(output)); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	recovered, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != historypkg.StatusCompleted {
		t.Fatalf("expected recovered status from terminal history, got %s", recovered.Status)
	}
	if recovered.Output["ok"] != true {
		t.Fatalf("expected recovered output, got %+v", recovered.Output)
	}
}

func TestServerRecoverRunAppliesExpiredWorkflowTimeoutBeforeQueueRecovery(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	defer server.Stop()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "wf-recover-expired-timeout", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(-time.Millisecond)
	run := &execution.Run{
		ID:                    "run-recover-expired-timeout",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now().Add(-time.Second),
		DeadlineAt:            &deadline,
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0)); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowFailed) != 1 {
		t.Fatalf("expected expired deadline to fail workflow during recovery, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 0 {
		t.Fatalf("expected expired deadline to skip workflow task recovery, got %d", len(workflowTasks.tasks))
	}
	recovered, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != historypkg.StatusFailed {
		t.Fatalf("expected run table to become failed, got %s", recovered.Status)
	}
}

// Run 行还是 Running、History 已终态：取消读 execution state 拒绝，Run 行由 reconcile 写回终态。
func TestServerCancelWorkflowRejectsTerminalExecutionState(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-cancel-terminal-history", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCompletedEvent(map[string]any{"ok": true})); err != nil {
		t.Fatal(err)
	}

	if err := server.cancelRun(run.ID, ""); err == nil {
		t.Fatal("expected cancel to reject terminal history status")
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowCancelled) != 0 {
		t.Fatalf("expected terminal history to block cancel event, got history %+v", history)
	}
	if err := server.RecoverRun(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := mgr.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != historypkg.StatusCompleted {
		t.Fatalf("expected run table to reconcile to completed, got %s", recovered.Status)
	}
}

func TestServerDeduplicatesActivityLifecycleByScheduledEvent(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-activity-lifecycle", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "step"}))
	if err != nil {
		t.Fatal(err)
	}
	// 无 retry：start 写真实 ActivityStarted 进 history（mutable-state 架构下 start 读 state）。
	seedScheduledActivityState(t, mgr, run.ID, scheduledEventID, "step", nil)

	server.HandleActivityStarted(run.ID, scheduledEventID, 1, 1, 0, 0)
	server.HandleActivityStarted(run.ID, scheduledEventID, 1, 1, 0, 0)
	// 严格 stamp：透传 seed state 的真实 stamp(1)。late failure 也带 stamp=1，
	// 验证它被"已终态"判定忽略（而非靠 stamp 不符），断言不变。
	if err := server.HandleActivityCompleted(ctx, run.ID, scheduledEventID, 1, 1, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(ctx, run.ID, scheduledEventID, 1, 1, protocol.Failure{Message: "late failure"}); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityStarted) != 1 {
		t.Fatalf("expected one ActivityStarted event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityCompleted) != 1 {
		t.Fatalf("expected one ActivityCompleted event, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatalf("expected completed activity to ignore later failure, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected one workflow task after activity close, got %d", len(workflowTasks.tasks))
	}
}

func TestServerRecordsActivityStartedPerAttempt(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	runID := "run-activity-start-attempts"
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(WithRootBranch(&execution.Run{ID: runID, WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}))
	}); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(runID, runID, runID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "retry-step"}))
	if err != nil {
		t.Fatal(err)
	}
	// 带重试的 attempt 先记临时 start，关闭时回填 ActivityStarted（dedupe key 含 attempt，逐 attempt 唯一）。
	// attempt 1 失败后 state 推进到 attempt 2（stamp 2），attempt 2 的重投 start 幂等。
	seedScheduledActivityState(t, mgr, runID, scheduledEventID, "retry-step", &historypkg.RetryPolicy{MaxAttempts: 3, InitialIntervalMs: 1, BackoffCoefficient: 1})

	if err := server.HandleActivityStarted(runID, scheduledEventID, 1, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(context.Background(), runID, scheduledEventID, 1, 1, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := mgr.GetActivityState(runID, scheduledEventID); st.Attempt != 2 || st.Stamp != 2 {
		t.Fatalf("retry must advance to attempt 2 stamp 2, got %+v", st)
	}

	server.HandleActivityStarted(runID, scheduledEventID, 2, 2, 0, 0)
	server.HandleActivityStarted(runID, scheduledEventID, 2, 2, 0, 0) // redelivery: 幂等
	if err := server.HandleActivityCompleted(context.Background(), runID, scheduledEventID, 2, 2, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityStarted) != 2 {
		t.Fatalf("expected ActivityStarted per attempt, got history %+v", history)
	}
}

func TestServerIgnoresActivityStartedAfterActivityClosed(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-activity-start-after-close", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "step"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(ctx, run.ID, scheduledEventID, 1, 0, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	server.HandleActivityStarted(run.ID, scheduledEventID, 1, 0, 0, 0)

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityStarted) != 0 {
		t.Fatalf("expected closed activity to ignore late start, got history %+v", history)
	}
}

func TestServerIgnoresActivityStartedAfterAttemptClosed(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())

	run := &execution.Run{ID: "run-activity-start-after-attempt-close", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "retry-step"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityAttemptFailedEvent(historypkg.ActivityAttemptFailedEvent{ScheduledEventID: scheduledEventID, Attempt: 1, Failure: &protocol.Failure{Message: "temporary"}})); err != nil {
		t.Fatal(err)
	}

	server.HandleActivityStarted(run.ID, scheduledEventID, 1, 0, 0, 0)

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityStarted) != 0 {
		t.Fatalf("expected failed attempt to ignore late start, got history %+v", history)
	}
}

func TestServerIgnoresDuplicateActivityFailureForSameAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)
	defer server.Stop()

	run := &execution.Run{ID: "run-duplicate-activity-failure", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "retry-step",
		ActivityType:        "retry-work",
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    time.Hour,
			BackoffCoefficient: 1,
		},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected first activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	server.HandleActivityStarted(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, 0, 0)

	// 严格 stamp：透传 dispatch task 的真实 stamp。第二次失败带相同 attempt+stamp，
	// 重试后 state.Attempt 已 ++，被"旧 attempt"判定丢弃，断言只记 1 次 AttemptFailed。
	if err := server.HandleActivityFailed(ctx, run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(ctx, run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityAttemptFailed) != 1 {
		t.Fatalf("expected duplicate failure to record one attempt failure, got history %+v", history)
	}
}

func TestServerIgnoresLateActivityCompletionForFailedAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)
	defer server.Stop()

	run := &execution.Run{ID: "run-late-activity-completion", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "retry-step",
		ActivityType:        "retry-work",
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    time.Hour,
			BackoffCoefficient: 1,
		},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	task := activityTasks.tasks[0].Task
	server.HandleActivityStarted(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, 0, 0)
	// 严格 stamp：fail 带真实 stamp，attempt 真推进；随后 late completion 带旧 attempt+旧 stamp
	// 被丢弃（验证"失败 attempt 的迟到完成被忽略"，而非靠 stamp=0 让所有操作空转）。
	if err := server.HandleActivityFailed(ctx, run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}

	if err := server.HandleActivityCompleted(ctx, run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityCompleted) != 0 {
		t.Fatalf("expected late completion for failed attempt to be ignored, got history %+v", history)
	}
}

func TestServerDeduplicatesActivityTimeoutForSameAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)
	defer server.Stop()

	run := &execution.Run{ID: "run-duplicate-activity-timeout", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "retry-step",
		ActivityType:        "retry-work",
		StartToCloseTimeout: time.Millisecond,
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    time.Hour,
			BackoffCoefficient: 1,
		},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	task := activityTasks.tasks[0].Task
	server.HandleActivityStarted(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, 0, 0)
	time.Sleep(5 * time.Millisecond) // 持久化的 StartToClose 截止时间已过

	server.handleActivityTimeoutAttempt(run.ID, task.ScheduledEventID, task.Attempt, protocol.TimeoutTypeStartToClose)
	server.handleActivityTimeoutAttempt(run.ID, task.ScheduledEventID, task.Attempt, protocol.TimeoutTypeStartToClose)

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityTimedOut) != 1 {
		t.Fatalf("expected duplicate timeout to record one timeout, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityAttemptFailed) != 1 {
		t.Fatalf("expected duplicate timeout to record one failed attempt, got history %+v", history)
	}
}

func TestServerIgnoresStaleActivityHeartbeatForOldAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)
	defer server.Stop()

	run := &execution.Run{ID: "run-stale-heartbeat", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
		StartToCloseTimeout: time.Minute,
		Type:                historypkg.CmdScheduleActivity,
		ActivityID:          "retry-step",
		ActivityType:        "retry-work",
		RetryPolicy: &protocol.ParsedRetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    time.Hour,
			BackoffCoefficient: 1,
		},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	task := activityTasks.tasks[0].Task
	// 严格 stamp：透传 attempt-1 的真实 stamp。fail 后 state.Attempt 推进到 2，
	// 随后 attempt-1 的"stale"心跳因旧 attempt 被丢弃——验证只记 1 次（active attempt）心跳。
	server.HandleActivityStarted(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, 0, time.Hour)
	if err := server.HandleActivityHeartbeat(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"cursor": "old"}); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityFailed(ctx, run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, protocol.Failure{Message: "temporary"}); err != nil {
		t.Fatal(err)
	}

	if err := server.HandleActivityHeartbeat(run.ID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{"cursor": "stale"}); err != nil {
		t.Fatal(err)
	}

	state, ok, err := mgr.GetActivityState(run.ID, task.ScheduledEventID)
	if err != nil || !ok || state.LastHeartbeat["cursor"] != "old" {
		t.Fatalf("expected stale heartbeat to be ignored, got %+v ok=%v err=%v", state, ok, err)
	}
}

func TestServerIgnoresActivityEventsAfterWorkflowTerminal(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		typ  historypkg.EventType
		act  func(*Server, string, int64) error
	}{
		{
			name: "completed",
			typ:  historypkg.EventActivityCompleted,
			act: func(server *Server, runID string, scheduledEventID int64) error {
				return server.HandleActivityCompleted(ctx, runID, scheduledEventID, 1, 0, map[string]any{"ok": true})
			},
		},
		{
			name: "failed",
			typ:  historypkg.EventActivityFailed,
			act: func(server *Server, runID string, scheduledEventID int64) error {
				return server.HandleActivityFailed(ctx, runID, scheduledEventID, 1, 0, protocol.Failure{Message: "late failure"})
			},
		},
		{
			name: "timeout",
			typ:  historypkg.EventActivityTimedOut,
			act: func(server *Server, runID string, scheduledEventID int64) error {
				server.handleActivityTimeoutAttempt(runID, scheduledEventID, 1, "late_timeout")
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			workflowTasks := newTestTaskQueue()
			server := assembleServer(mgr, workflowTasks, newTestTaskQueue())

			run := &execution.Run{ID: "run-terminal-late-" + tt.name, WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
			if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
				t.Fatal(err)
			}
			scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "late-step"}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowCancelledEvent(nil)); err != nil {
				t.Fatal(err)
			}

			if err := tt.act(server, run.ID, scheduledEventID); err != nil {
				t.Fatal(err)
			}

			history, err := mgr.GetEvents(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if countEngineEvents(history, tt.typ) != 0 {
				t.Fatalf("expected terminal workflow to ignore late %s, got history %+v", tt.typ, history)
			}
			if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
				t.Fatalf("expected terminal workflow to ignore late activity failure, got history %+v", history)
			}
			if len(workflowTasks.tasks) != 0 {
				t.Fatalf("expected no workflow task after terminal workflow, got %d", len(workflowTasks.tasks))
			}
		})
	}
}

func TestServerSkipsActivityTimeoutAfterActivityClosed(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	run := &execution.Run{ID: "run-activity-timeout-after-close", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "step"}))
	if err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：complete/timeout 权威读 state，seed 一个 started 状态供 complete 读取并终态化。
	seedScheduledActivityState(t, mgr, run.ID, scheduledEventID, "step", nil)
	server.HandleActivityStarted(run.ID, scheduledEventID, 1, 1, 0, 0)

	// 严格 stamp：透传 seed state 的真实 stamp(1)，complete 终态化后 late timeout 被忽略。
	if err := server.HandleActivityCompleted(ctx, run.ID, scheduledEventID, 1, 1, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	server.handleActivityTimeout(run.ID, scheduledEventID, "late_timeout")

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityTimedOut) != 0 {
		t.Fatalf("expected closed activity to ignore late timeout, got history %+v", history)
	}
	if countEngineEvents(history, historypkg.EventActivityFailed) != 0 {
		t.Fatalf("expected closed activity to ignore late timeout failure, got history %+v", history)
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected one workflow task after activity close, got %d", len(workflowTasks.tasks))
	}
}

func TestServerRecoversOpenWorkflowTaskFromEventHistory(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{ID: "wf-recover-open-task", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-open-task",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowStartedEvent(nil, "", "", "", "", 0)); err != nil {
		t.Fatal(err)
	}
	scheduledEventID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("expected one recovered workflow task, got %d", len(workflowTasks.tasks))
	}
	task := workflowTasks.tasks[0].Task
	if task.Type != protocol.TaskTypeWorkflow || task.RunID != run.ID || task.ScheduledEventID != scheduledEventID {
		t.Fatalf("unexpected recovered workflow task: %+v", task)
	}
}

// Integration（崩溃恢复）: RecoverRun 直读 ActivityState 重派一个 Scheduled 的 activity，
// 携带 state 上的 attempt/stamp/超时——不扫事件历史（mutable-state 架构）。
func TestServerRecoversOpenActivityTaskFromState(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	workflowTasks := newTestTaskQueue()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, workflowTasks, activityTasks)

	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "wf-recover-open-activity",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"charge": {Symbol: "charge", Type: "charge_card"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{
		ID:                    "run-recover-open-activity",
		WorkflowID:            ref.WorkflowID,
		WorkflowType:          ref.WorkflowID,
		WorkflowConfigVersion: ref.Version,
		Status:                historypkg.StatusRunning,
		CreatedAt:             time.Now(),
	}
	createStartedRunForTest(t, mgr, run)
	const scheduledEventID int64 = 100
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID:                    run.ID,
		ScheduledEventID:         scheduledEventID,
		ActivityID:               "charge",
		ActivityType:             "charge_card",
		Attempt:                  1,
		Stamp:                    1,
		StartedEventID:           execution.EmptyEventID,
		Status:                   execution.ActivityStatusEnums.Scheduled,
		ScheduledTime:            time.Now(),
		Input:                    map[string]any{"amount": 42},
		ScheduleToStartTimeoutMs: 250,
		StartToCloseTimeoutMs:    500,
		ScheduleToCloseTimeoutMs: 750,
		HeartbeatTimeoutMs:       100,
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.RecoverRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	if len(activityTasks.tasks) != 1 {
		t.Fatalf("expected one recovered activity task, got %d", len(activityTasks.tasks))
	}
	task := activityTasks.tasks[0].Task
	if task.Type != protocol.TaskTypeActivity || task.RunID != run.ID || task.ScheduledEventID != scheduledEventID || task.ActivityID != "charge" || task.ActivityType != "charge_card" {
		t.Fatalf("unexpected recovered activity task: %+v", task)
	}
	if task.ScheduleToStartTimeout != 250*time.Millisecond || task.StartToCloseTimeout != 500*time.Millisecond || task.ScheduleToCloseTimeout != 750*time.Millisecond || task.HeartbeatTimeout != 100*time.Millisecond {
		t.Fatalf("unexpected recovered activity timeouts: %+v", task)
	}
}

type switchableTaskQueue struct {
	err   error
	tasks []protocol.TaskLease
}

func (q *switchableTaskQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	if q.err != nil {
		return q.err
	}
	q.tasks = append(q.tasks, protocol.TaskLease{Task: task})
	return nil
}

// sticky 任务在这个测试队列里与普通任务走同一通道，任何 Worker 都能领取。
func (q *switchableTaskQueue) EnqueueSticky(ctx context.Context, _ string, task protocol.Task) error {
	return q.Enqueue(ctx, task)
}

func (q *switchableTaskQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (*switchableTaskQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*switchableTaskQueue) Durable() bool                      { return false }

func (q *switchableTaskQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	if len(q.tasks) == 0 {
		return protocol.TaskLease{}, errors.New("empty queue")
	}
	lease := q.tasks[0]
	q.tasks = q.tasks[1:]
	return lease, nil
}

func (q *switchableTaskQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

func (q *switchableTaskQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return q.Enqueue(ctx, lease.Task)
}

func countEngineEvents(history []historypkg.Event, typ historypkg.EventType) int {
	count := 0
	for _, event := range history {
		if event.EventType() == typ {
			count++
		}
	}
	return count
}

func waitForEngineEventCount(t *testing.T, events execution.EventStore, runID string, typ historypkg.EventType, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		history, err := events.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if countEngineEvents(history, typ) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at least %d %s events, got history %+v", want, typ, history)
		}
		time.Sleep(time.Millisecond)
	}
}

func appendOpenWorkflowTaskForTest(t testing.TB, events execution.EventStore, runID string) int64 {
	t.Helper()
	eventID, err := events.Append(runID, runID, runID, historypkg.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

// openWorkflowTaskScheduledIDInEvents 返回当前未被 close 的 WFT scheduled 事件 ID（无则 0）：
// WFT close 后旧 scheduled 被释放，新 WFT 可重新调度。
func openWorkflowTaskScheduledIDInEvents(events []historypkg.Event) int64 {
	closed := make(map[int64]bool)
	for _, existing := range events {
		if historypkg.IsWorkflowTaskCloseEventType(existing.EventType()) && existing.FlatScheduledEventID() > 0 {
			closed[existing.FlatScheduledEventID()] = true
		}
	}
	var latest int64
	for _, existing := range events {
		if existing.EventType() == historypkg.EventWorkflowTaskScheduled && existing.ID > 0 && !closed[existing.ID] {
			latest = existing.ID
		}
	}
	return latest
}

type testTaskQueue struct {
	mu    sync.Mutex // 生产 queue（MQ/DB）本身并发安全；测试桩须自带锁，否则不同 workflowID 的 WFT 并发 Enqueue 触发 slice race
	tasks []protocol.TaskLease
	acked []protocol.TaskLease
}

// errTestTaskQueueEmpty 是 testTaskQueue 没有任务时 Dequeue 返回的错误。
var errTestTaskQueueEmpty = errors.New("test task queue is empty")

func newTestTaskQueue() *testTaskQueue {
	return &testTaskQueue{}
}

func (q *testTaskQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, protocol.TaskLease{Task: task})
	return nil
}

// sticky 任务在这个测试队列里与普通任务走同一通道，任何 Worker 都能领取。
func (q *testTaskQueue) EnqueueSticky(ctx context.Context, _ string, task protocol.Task) error {
	return q.Enqueue(ctx, task)
}

func (q *testTaskQueue) DequeueForWorker(ctx context.Context, _ string) (protocol.TaskLease, error) {
	return q.Dequeue(ctx)
}

func (*testTaskQueue) HandlesStickyRoutingDeadline() bool { return true }
func (*testTaskQueue) Durable() bool                      { return false }

func (q *testTaskQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return protocol.TaskLease{}, errTestTaskQueueEmpty
	}
	lease := q.tasks[0]
	q.tasks = q.tasks[1:]
	return lease, nil
}

// snapshot 返回队列中当前 lease 的副本。测试断言入队内容必须走它而非直读
// q.tasks —— 后者在 -race 下会与并发 Enqueue 撞出 data race。
func (q *testTaskQueue) snapshot() []protocol.TaskLease {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]protocol.TaskLease(nil), q.tasks...)
}

func (q *testTaskQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, lease)
	return nil
}

func (q *testTaskQueue) ackedLeases() []protocol.TaskLease {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]protocol.TaskLease(nil), q.acked...)
}

func (q *testTaskQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	return nil
}

// [高熵密钥] 固定 AutoUpgrade 候选的能力校验语义：
// 校验必须针对本次 lease 选中的 ProgramVersion，而不是 Run 的 current 内容。
// current 是不要求 capability 的旧内容、latest 是 v2 内容时，无能力 worker 必须被 Nack。
func TestWorkflowTaskCapabilityUsesSelectedProgramVersion(t *testing.T) {
	mgr := memory.NewExecutionManager()
	workflowTasks := &switchableTaskQueue{}
	server := assembleServer(mgr, workflowTasks, newTestTaskQueue())
	defer server.Stop()

	register := func(id string, program programpkg.WLangProgram) definition.WorkflowConfigRef {
		t.Helper()
		ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
			ID:         id,
			Program:    program,
			Activities: map[string]programpkg.ActivityRef{},
		})

		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	refA, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &programpkg.WorkflowConfig{
		ID:      "capability-upgrade",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"plain": {Symbol: "plain", Type: "noop"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	refB := register("capability-upgrade", programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		Program:          []any{map[string]any{"return": nil}},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	})
	if refB.Version == refA.Version {
		t.Fatal("v2 content must differ from the current content")
	}

	run := &execution.Run{
		ID: "capability-upgrade-run", Namespace: protocol.DefaultNamespace, WorkflowID: refA.WorkflowID, WorkflowType: refA.WorkflowID,
		WorkflowConfigVersion: refA.Version, OriginalWorkflowConfigVersion: refA.Version,
		Policy: execution.RunPolicyAutoUpgrade, Status: historypkg.StatusRunning, CreatedAt: time.Now(),
	}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	if err := server.scheduleWorkflowTask(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}

	_, err = server.PollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "old-worker", BuildID: "old-build", WorkflowCapabilities: []string{},
	}))
	var unsupported *programpkg.UnsupportedWLangCapabilityError
	if !errors.As(err, &unsupported) {
		t.Fatalf("worker without v2 capability must be nacked, got %v", err)
	}
	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 0 {
		t.Fatal("candidate content must not start on a worker lacking its capability")
	}
	if len(workflowTasks.tasks) != 1 {
		t.Fatalf("nacked task must be requeued, got %d", len(workflowTasks.tasks))
	}

	lease, err := server.PollWorkflowTask(protocol.WithWorkerInfo(context.Background(), protocol.WorkerInfo{
		Identity: "new-worker", BuildID: "new-build", WorkflowCapabilities: programpkg.CurrentWLangCapabilities(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ProgramVersion != refB.Version {
		t.Fatalf("started program version = %q, want latest %q", lease.Task.ProgramVersion, refB.Version)
	}
	history, err = mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventWorkflowTaskStarted) != 1 {
		t.Fatal("capable worker must start exactly one workflow task")
	}
}
