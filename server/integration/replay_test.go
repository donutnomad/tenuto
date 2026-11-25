package integration

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestDuplicateWorkflowTaskDoesNotDoubleScheduleActivity(t *testing.T) {
	registry := wfsdk.NewRegistry()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	eng := assembly.NewEngine(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, registry, worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
	})

	release := make(chan struct{})
	registry.Register("blocked", func(ctx context.Context, input m) (m, error) {
		<-release
		return m{"ok": true}, nil
	})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "duplicate-replay"},
		sdkgentest.Step{Type: "blocked", Options: `StartToCloseTimeout: time.Minute`},
	)

	ctx := context.Background()
	eng.Start(ctx)
	defer eng.Stop()

	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.Type, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForEventCount(t, eng, ctx, runID, historypkg.EventActivityScheduled, 1, 3*time.Second)

	scheduled, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := workflowTasks.Enqueue(ctx, protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: runID, ScheduledEventID: firstWorkflowTaskScheduledID(t, scheduled)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	history, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEvent(history, historypkg.EventActivityScheduled); got != 1 {
		t.Fatalf("expected 1 scheduled activity after duplicate workflow task, got %d: %+v", got, history)
	}

	close(release)
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 3*time.Second)
}

func waitForEventCount(t *testing.T, eng *assembly.Engine, ctx context.Context, runID string, typ historypkg.EventType, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		history, err := eng.GetEvents(ctx, runID)
		if err == nil && countEvent(history, typ) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	history, _ := eng.GetEvents(ctx, runID)
	t.Fatalf("expected at least %d %s events, got %d: %+v", want, typ, countEvent(history, typ), history)
}

func countEvent(history []historypkg.Event, typ historypkg.EventType) int {
	count := 0
	for _, event := range history {
		if event.EventType() == typ {
			count++
		}
	}
	return count
}
