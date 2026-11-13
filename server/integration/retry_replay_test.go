package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestRetryAttemptFailureDoesNotStopReplay(t *testing.T) {
	registry := activity.NewRegistry()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	eng := assembly.NewEngine(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, registry, worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
	})

	var mu sync.Mutex
	attempts := 0
	var contextAttempts []int
	registry.Register("sometimes_fails", func(ctx context.Context, input m) (m, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		contextAttempts = append(contextAttempts, activity.GetInfo(ctx).Attempt)
		if attempts == 1 {
			return nil, fmt.Errorf("temporary")
		}
		return m{"ok": true}, nil
	})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{ID: "retry-replay"},
		sdkgentest.Step{Type: "sometimes_fails", Options: `StartToCloseTimeout: time.Minute, RetryPolicy: workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: 10 * time.Millisecond, BackoffCoefficient: 1}`},
	)

	ctx := context.Background()
	eng.Start(ctx)
	defer eng.Stop()

	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 3*time.Second)

	mu.Lock()
	gotAttempts := attempts
	gotContextAttempts := append([]int(nil), contextAttempts...)
	mu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", gotAttempts)
	}
	if len(gotContextAttempts) != 2 || gotContextAttempts[0] != 1 || gotContextAttempts[1] != 2 {
		t.Fatalf("expected step context attempts [1 2], got %v", gotContextAttempts)
	}

	history, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	seenAttemptFailed := false
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityAttemptFailed {
			seenAttemptFailed = true
		}
		if event.EventType() == historypkg.EventActivityFailed {
			t.Fatalf("successful retry history contains terminal ActivityFailed: %+v", history)
		}
	}
	if !seenAttemptFailed {
		t.Fatalf("missing ActivityAttemptFailed in history: %+v", history)
	}
	if got := countEvent(history, historypkg.EventActivityStarted); got != 2 {
		t.Fatalf("expected ActivityStarted for each retry attempt, got %d: %+v", got, history)
	}
}
