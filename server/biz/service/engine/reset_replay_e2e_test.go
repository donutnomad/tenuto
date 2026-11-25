package engine_test

import (
	"context"
	"fmt"
	"strings"
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

func TestResetPendingWorkPreservesCurrentRun(t *testing.T) {
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
			if kind == "activity" {
				a := appendEvent(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "A", ActivityType: "probe"}))
				appendEvent(historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "B", ActivityType: "probe"}))
				appendEvent(historypkg.Event{ActivityCompleted: &historypkg.ActivityCompletedEvent{ScheduledEventID: a}})
			} else {
				appendEvent(historypkg.Event{TimerScheduled: &historypkg.TimerScheduledEvent{TimerID: "waiting"}})
			}
			scheduled := appendEvent(historypkg.NewWorkflowTaskScheduledEvent(0))
			boundary := appendEvent(historypkg.NewWorkflowTaskStartedEvent(scheduled, "workflow", "", 0))
			_, err = server.ResetWorkflow(ctx, execution.ResetWorkflowRequest{RunID: base, ResetEventID: boundary})
			if err == nil || !strings.Contains(err.Error(), "pending "+kind) {
				t.Fatalf("reset error=%v", err)
			}
			status, err := server.GetRunStatus(base)
			if err != nil || status != historypkg.StatusRunning {
				t.Fatalf("status=%v err=%v", status, err)
			}
			original, err := mgr.Get(base)
			if err != nil || original.ResetRunID != "" {
				t.Fatalf("reset link=%+v err=%v", original, err)
			}
		})
	}
}
