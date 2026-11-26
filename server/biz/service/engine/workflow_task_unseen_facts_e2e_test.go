package engine_test

// Workflow Task 进行期间持续到达的 Signal 不让非终态的完成作废（对标 Temporal 的 HasBufferedEvents /
// FlushBufferedEvents，审查 A3 第 4 条与派发审查的同一问题）：Signal 每 10ms 一条、一次 Workflow Task 往返 30ms 时，第一个 Activity 完成之后第二个
// Activity 仍被调度，Run 能走到结束。全量路径与 sticky 增量路径各跑一遍；每个 Workflow Task 都重放（全量路径冷
// 重放、sticky 路径增量），重放分批与提交时 Worker 看到的事件不一致会以 nondeterministic 失败。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// slowReadServer 给 Worker 读 History 与读增量加上固定延迟，模拟跨进程 Worker 一次 Workflow Task 的往返时间。
type slowReadServer struct {
	*engine.Server
	delay time.Duration
}

func (s slowReadServer) GetEvents(runID string) ([]history.Event, error) {
	time.Sleep(s.delay)
	return s.Server.GetEvents(runID)
}

func (s slowReadServer) GetWorkflowActivation(ctx context.Context, request history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	time.Sleep(s.delay)
	return s.Server.GetWorkflowActivation(ctx, request)
}

func TestSteadySignalsDoNotStallWorkflowTasks(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		name := "full-history"
		if sticky {
			name = "sticky"
		}
		t.Run(name, func(t *testing.T) { runSteadySignalsScenario(t, sticky) })
	}
}

func runSteadySignalsScenario(t *testing.T, sticky bool) {
	mgr := memory.NewExecutionManager()
	activityTasks := memory.NewTaskQueue(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: engine.QueueActivityRouter{Queue: activityTasks}})
	defer server.Stop()
	registerE2EActivities(t, server)
	const wfType = "steady-signals"
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, sdkgentest.Sequential(t, &program.WorkflowConfig{Type: wfType},
		sdkgentest.Step{Type: "activity.first", Options: `StartToCloseTimeout: time.Minute`},
		sdkgentest.Step{Type: "activity.second", Options: `StartToCloseTimeout: time.Minute`},
	)); err != nil {
		t.Fatal(err)
	}
	options := worker.WorkerOptions{Identity: "wf-worker", BuildID: "b1"}
	if sticky {
		cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 10, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		options.ExecutionCache = cache
	}
	w := worker.NewWorkflowWorker(slowReadServer{Server: server, delay: 30 * time.Millisecond}, options)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	defer func() { cancel(); w.Stop() }()

	runID, err := startedRunID(server.StartWorkflow(ctx, "", wfType, wfType, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	first := dequeueActivityTask(t, activityTasks)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = server.SignalWorkflow(ctx, "", "", runID, "tick", "", map[string]any{"n": int64(i)})
			}
		}
	}()
	completeActivityTask(t, server, first, map[string]any{"first": "done"})
	second := dequeueActivityTask(t, activityTasks)
	if second.ActivityType != "activity.second" {
		t.Fatalf("second activity task = %s", second.ActivityType)
	}
	time.Sleep(300 * time.Millisecond)
	// 结束 Workflow 的完成遇到没看到的 Signal 会作废重做（unhandled_events，与 Temporal 的 UNHANDLED_COMMAND 相同），
	// 所以先停止 Signal 再等 Run 结束。
	close(stop)
	wg.Wait()
	completeActivityTask(t, server, second, map[string]any{"second": "done"})
	waitForRunStatus(t, server, runID, history.StatusCompleted)

	events, err := server.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	signals, completed := 0, 0
	for _, event := range events {
		switch {
		case event.WorkflowSignaled != nil:
			signals++
		case event.WorkflowTaskCompleted != nil:
			completed++
		case event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause != execution.WorkflowTaskFailedCauseUnhandledEvents:
			t.Fatalf("workflow task failed (%s): %s", event.WorkflowTaskFailed.Cause, event.WorkflowTaskFailed.Reason)
		}
	}
	if signals == 0 || completed < 3 {
		t.Fatalf("history has %d signals and %d completed workflow tasks", signals, completed)
	}
}
