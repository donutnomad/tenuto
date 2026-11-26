package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	memory "github.com/donutnomad/tenuto/server/infra/memory"
)

func TestResetReplaysSelectedTaskPreservingPriorActivity(t *testing.T) {
	for _, boundary := range []string{"started", "completed"} {
		t.Run(boundary, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
			defer server.Stop()
			if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Identity: "B", BuildID: "v1", TaskQueues: []string{"q"}, ActivityTypes: []string{"probe"}}); err != nil {
				t.Fatal(err)
			}
			_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "reset-" + boundary, TargetWorkerIdentity: "B"},
				sdkgentest.Step{Type: "probe", Options: `StartToCloseTimeout: time.Minute`},
				sdkgentest.Step{Type: "probe", Options: `StartToCloseTimeout: time.Minute`},
			))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			worker := worker.NewWorkflowWorker(server, worker.WorkerOptions{Identity: "workflow"})
			worker.Start(ctx)
			defer worker.Stop()
			ready, cancelReady := context.WithTimeout(ctx, time.Second)
			defer cancelReady()
			if err := worker.WaitReady(ready); err != nil {
				t.Fatal(err)
			}
			base, err := startedRunID(server.StartWorkflow(ctx, "", "reset-"+boundary, "reset-"+boundary, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			poll := func() protocol.Task {
				t.Helper()
				workerCtx := protocol.WithWorkerInfo(protocol.WithNamespace(ctx, "default"), protocol.WorkerInfo{Namespace: "default", Identity: "B", BuildID: "v1"})
				pollCtx, cancelPoll := context.WithTimeout(workerCtx, 3*time.Second)
				defer cancelPoll()
				lease, err := server.PollActivityTask(pollCtx, "q")
				if err != nil {
					t.Fatal(err)
				}
				return lease.Task
			}
			finish := func(task protocol.Task) {
				t.Helper()
				if task.TargetWorkerIdentity != "B" {
					t.Fatalf("target=%q", task.TargetWorkerIdentity)
				}
				task.WorkerIdentity = "B"
				task.WorkerBuildID = "v1"
				completeActivityTask(t, server, task, map[string]any{"value": task.ActivityID})
			}
			finish(poll())
			finish(poll())
			waitForRunStatus(t, server, base, historypkg.StatusCompleted)
			history, err := mgr.GetEvents(base)
			if err != nil {
				t.Fatal(err)
			}
			var selected, scheduled int64
			for _, event := range history {
				if event.WorkflowTaskStarted != nil && event.ID > 1 {
					if scheduled == 0 {
						scheduled = event.WorkflowTaskStarted.ScheduledEventID
						continue
					}
					selected = event.ID
					if boundary == "completed" {
						selected = 0
					}
					break
				}
			}
			if boundary == "completed" {
				var n int
				for _, event := range history {
					if event.WorkflowTaskCompleted != nil {
						n++
						if n == 2 {
							selected = event.ID
							break
						}
					}
				}
			}
			if selected == 0 {
				t.Fatal("missing second task boundary")
			}
			reset, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, ResetEventID: selected, Reason: "regression"})
			if err != nil {
				t.Fatal(err)
			}
			task := poll()
			if task.RunID != reset || task.ActivityID != "#a2" {
				t.Fatalf("selected task replay=%+v", task)
			}
			finish(task)
			waitForRunStatus(t, server, reset, historypkg.StatusCompleted)
			events, err := mgr.GetEvents(reset)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for i, event := range events {
				if event.ID != int64(i+1) {
					t.Fatal(fmt.Sprintf("history discontinuity at %d: %d", i, event.ID))
				}
				if event.ActivityScheduled != nil {
					counts[event.ActivityScheduled.ActivityID]++
				}
				if event.ActivityStarted != nil && event.ActivityStarted.WorkerIdentity != "B" {
					t.Fatalf("executor=%q", event.ActivityStarted.WorkerIdentity)
				}
			}
			if counts["#a1"] != 1 || counts["#a2"] != 1 {
				t.Fatalf("activity history=%v", counts)
			}
		})
	}
}

// Happy Path: 分叉点上未结束的 Activity / Timer 不阻止 reset：已关闭的 Activity 不带入新 Run，
// 未开始的 Activity 与未触发的 Timer 以新 Run 的 state 行保留，原 Run 被终止。
func TestResetCarriesPendingWorkIntoNewRun(t *testing.T) {
	for _, kind := range []string{"activity", "timer"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewExecutionManager()
			server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
			defer server.Stop()
			if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "pending-" + kind, Program: programtest.Noop()}); err != nil {
				t.Fatal(err)
			}
			base, err := startedRunID(server.StartWorkflow(ctx, "", "pending-"+kind, "pending-"+kind, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			appendEvent := func(e historypkg.Event) int64 {
				t.Helper()
				id, err := mgr.Append(base, base, base, e)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
			now := time.Now()
			var pendingID int64
			if kind == "activity" {
				a := appendEvent(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "A", ActivityType: "probe"}))
				pendingID = appendEvent(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "B", ActivityType: "probe"}))
				appendEvent(historypkg.Event{ActivityCompleted: &historypkg.ActivityCompletedEvent{ScheduledEventID: a}})
				for id, activityID := range map[int64]string{a: "A", pendingID: "B"} {
					if err := mgr.UpsertActivityState(&execution.ActivityState{RunID: base, ScheduledEventID: id, ActivityID: activityID, ActivityType: "probe",
						Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now, CreatedAt: now, UpdatedAt: now}); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				pendingID = appendEvent(historypkg.NewTimerScheduledEvent("waiting", nil, int64(time.Hour/time.Millisecond)))
				if err := mgr.UpsertTimerState(&execution.TimerState{RunID: base, ScheduledEventID: pendingID, TimerID: "waiting",
					Kind: execution.TimerKindEnums.UserTimer, Deadline: now.Add(time.Hour), Status: execution.TimerStatusEnums.Scheduled, CreatedAt: now, UpdatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			scheduled := appendEvent(historypkg.NewWorkflowTaskScheduledEvent(0))
			boundary := appendEvent(historypkg.NewWorkflowTaskStartedEvent(scheduled, "workflow", "", 0))
			newRunID, err := server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, ResetEventID: boundary})
			if err != nil {
				t.Fatalf("reset error=%v", err)
			}
			if status, err := server.GetRunStatus(base); err != nil || status != historypkg.StatusTerminated {
				t.Fatalf("base status=%v err=%v, want terminated", status, err)
			}
			if kind == "activity" {
				states, err := mgr.ListActivityStates(newRunID)
				if err != nil || len(states) != 1 || states[0].ScheduledEventID != pendingID || states[0].ActivityID != "B" || !states[0].IsScheduled() {
					t.Fatalf("new run activity states=%+v err=%v, want only B scheduled", states, err)
				}
				return
			}
			timer, found, err := mgr.GetTimerState(newRunID, pendingID)
			if err != nil || !found || !timer.IsScheduled() || !timer.Deadline.Equal(now.Add(time.Hour)) {
				t.Fatalf("new run timer=%+v found=%v err=%v, want the original deadline", timer, found, err)
			}
		})
	}
}
