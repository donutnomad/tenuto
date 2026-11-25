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
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

type m = map[string]any

func TestEngineRunsLinearWorkflowWithScheduledEventIDs(t *testing.T) {
	registry := activity.NewRegistry()
	mgr := memory.NewExecutionManager()
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)

	registry.Register("first", func(ctx context.Context, input m) (m, error) {
		return m{"value": input["value"]}, nil
	})
	registry.Register("second", func(ctx context.Context, input m) (m, error) {
		return m{"ok": input["value"] == "from-first"}, nil
	})

	eng := assembly.NewEngine(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: workflowTasks, ActivityRouter: activityTasks}, registry, worker.WorkerOptions{
		WorkflowConcurrency: 1,
		ActivityConcurrency: 1,
	})

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "linear"},
		sdkgentest.Step{Type: "first", Input: `map[string]any{"value": "from-first"}`, Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "second", Input: `map[string]any{"value": out0["value"]}`, Options: `StartToCloseTimeout: time.Minute`},
	)

	ctx := context.Background()
	eng.Start(ctx)
	defer eng.Stop()

	if _, err := eng.RegisterWorkflow(wf); err != nil {
		t.Fatal(err)
	}
	runID, err := eng.Run(ctx, wf.Type, m{"request_id": "req-1"})
	if err != nil {
		t.Fatal(err)
	}

	waitForRunStatus(t, eng, ctx, runID, historypkg.StatusCompleted, 3*time.Second)

	history, err := eng.GetEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	scheduled := map[int64]string{}
	completed := map[int64]bool{}
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled {
			if event.ID == 0 {
				t.Fatalf("scheduled event %q has empty event id", event.ActivityScheduled.ActivityID)
			}
			scheduled[event.ID] = event.ActivityScheduled.ActivityID
		}
		if event.EventType() == historypkg.EventActivityCompleted {
			completed[event.FlatScheduledEventID()] = true
		}
	}

	if len(scheduled) != 2 {
		t.Fatalf("expected 2 scheduled events, got %d", len(scheduled))
	}
	for eventID, stepID := range scheduled {
		if !completed[eventID] {
			t.Fatalf("missing completion linked to scheduled event %d for %s", eventID, stepID)
		}
	}
}

func waitForRunStatus(t *testing.T, eng *assembly.Engine, ctx context.Context, runID string, want historypkg.Status, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := eng.GetRun(ctx, runID)
		if err == nil && run.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	run, err := eng.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	t.Fatalf("expected run status %s, got %s", want, run.Status)
}

// registerActivityRoutes 以测试 Worker 的身份在 default namespace、default task queue 登记 activityTypes。直接提交调度命令、
// 或在 Worker 启动之后才注册 Activity 实现的测试经它让调度的 Activity 有路由。
func registerActivityRoutes(t *testing.T, server *engine.Server, activityTypes ...string) {
	t.Helper()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "integration-test-activity-worker", TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: activityTypes,
	}); err != nil {
		t.Fatal(err)
	}
}
