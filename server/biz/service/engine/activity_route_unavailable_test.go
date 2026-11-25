package engine_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

type nackCountingServer struct {
	*engine.Server
	nacks atomic.Int64
}

func (s *nackCountingServer) NackWorkflowTask(ctx context.Context, lease protocol.TaskLease) error {
	s.nacks.Add(1)
	return s.Server.NackWorkflowTask(ctx, lease)
}

func registerRouteWorker(t *testing.T, server *engine.Server, identity string, activityTypes ...string) {
	t.Helper()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity:      identity,
		BuildID:       identity + "-build",
		TaskQueues:    []string{protocol.DefaultActivityTaskQueue},
		ActivityTypes: activityTypes,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowPublishedBeforeActivityWorker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sticky bool
	}{{name: "full replay"}, {name: "sticky session", sticky: true}} {
		t.Run(tc.name, func(t *testing.T) {
			activities := memory.NewTaskQueue(16)
			server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: engine.QueueActivityRouter{Queue: activities}})
			t.Cleanup(server.Stop)
			registerRouteWorker(t, server, "other", "other.Type")
			if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "published-first"},
				sdkgentest.Step{Type: "activity.late", Options: `StartToCloseTimeout: time.Minute`},
			)); err != nil {
				t.Fatalf("publishing before the activity worker must succeed: %v", err)
			}

			options := worker.WorkerOptions{Identity: "route-wait-worker", BuildID: "build-1"}
			if tc.sticky {
				cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				options.ExecutionCache = cache
			}
			api := &nackCountingServer{Server: server}
			worker := worker.NewWorkflowWorker(api, options)
			ctx, cancel := context.WithCancel(context.Background())
			worker.Start(ctx)
			t.Cleanup(func() {
				cancel()
				worker.Stop()
			})
			readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
			defer readyCancel()
			if err := worker.WaitReady(readyCtx); err != nil {
				t.Fatal(err)
			}

			runID, err := startedRunID(server.StartWorkflow(ctx, "", "published-first", "published-first", "", nil))
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(2500 * time.Millisecond)

			state, err := server.GetWorkflowExecutionState(runID)
			if err != nil {
				t.Fatal(err)
			}
			blocked := state.BlockedInfo()
			if !blocked.Blocked || blocked.Cause != engine.WorkflowTaskFailedCauseActivityRouteUnavailable {
				t.Fatalf("blocked info = %+v", blocked)
			}
			if blocked.ConsecutiveFailures < 1 || blocked.ConsecutiveFailures > 4 {
				t.Fatalf("consecutive failures = %d, want bounded by backoff", blocked.ConsecutiveFailures)
			}
			if nacks := api.nacks.Load(); nacks > 8 {
				t.Fatalf("workflow task nacks = %d, want bounded by backoff", nacks)
			}
			status, err := server.GetRunStatus(runID)
			if err != nil || status != history.StatusRunning {
				t.Fatalf("run status = %s, err=%v", status, err)
			}

			registerRouteWorker(t, server, "late", "activity.late")
			task := dequeueActivityTask(t, activities)
			if task.RunID != runID || task.ActivityID != "#a1" {
				t.Fatalf("activity task = %+v", task)
			}
			state, err = server.GetWorkflowExecutionState(runID)
			if err != nil {
				t.Fatal(err)
			}
			if blocked := state.BlockedInfo(); blocked.Blocked {
				t.Fatalf("run still blocked after worker came online: %+v", blocked)
			}
			completeActivityTask(t, server, task, map[string]any{"ok": true})
			waitForRunStatus(t, server, runID, history.StatusCompleted)
		})
	}
}
