package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	activity "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Worker 停止时注销自己的 Activity 路由：路由立刻退出选路，Worker 的登记记录保留。
func TestWorkerStopDeregistersActivityRoutes(t *testing.T) {
	server := assembleServerWithoutTestWorker(memory.NewExecutionManager())

	registry := activity.NewRegistry()
	registry.Register("payments.Capture", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		return nil, nil
	})
	w := assembleWorker(blockingWorkerAPI{Server: server}, registry, worker.WorkerOptions{
		Role:                          worker.WorkerRoleActivity,
		ActivityTaskQueues:            []string{"payments"},
		Identity:                      "payments-1",
		BuildID:                       "payments-v1",
		ActivityWorkerVersionRegistry: server,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	if err := w.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	ref := dispatch.ActivityRouteRef{Namespace: protocol.DefaultNamespace, Type: "payments.Capture"}
	if _, err := server.resolveActivityRoute(ref); err != nil {
		t.Fatalf("route before stop: %v", err)
	}

	w.Stop()

	active, err := server.workerVersions.ListActiveActivityWorkerVersions(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("routes still active after Worker.Stop: %+v", active)
	}
	if route, err := server.resolveActivityRoute(ref); err == nil {
		t.Fatalf("stopped worker still selected: %+v", route)
	}
	known, err := server.workerVersions.ListActiveActivityWorkerVersions(time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(known) != 1 {
		t.Fatalf("registration records after stop = %d, want 1 kept", len(known))
	}
}

// 注销后的路由保留一天供 Console 查看，到期超过一天后由 PurgeExpiredActivityWorkerVersions 删除。
func TestPurgeExpiredActivityWorkerVersionsKeepsOneDay(t *testing.T) {
	server, clk := newFakeClockServer(memory.NewExecutionManager(), newTestTaskQueue(), newTestTaskQueue())
	worker := protocol.ActivityWorkerVersion{Identity: "pod-1", BuildID: "v1", TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.Capture"}}
	if err := server.RegisterActivityWorkerVersion(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if err := server.DeregisterActivityWorkerVersionInNamespace(context.Background(), protocol.DefaultNamespace, "pod-1", "v1"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(expiredWorkerVersionRetention - time.Second)
	if purged, err := server.PurgeExpiredActivityWorkerVersions(); err != nil || purged != 0 {
		t.Fatalf("purge within retention = %d, %v", purged, err)
	}
	clk.Advance(2 * time.Second)
	if purged, err := server.PurgeExpiredActivityWorkerVersions(); err != nil || purged != 1 {
		t.Fatalf("purge after retention = %d, %v; want 1", purged, err)
	}
	known, err := server.workerVersions.ListActiveActivityWorkerVersions(time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range known {
		if route.Identity == "pod-1" {
			t.Fatalf("purged route still listed: %+v", route)
		}
	}
}

// Running Run 指定的 Worker 下线超过一天也不删除它的登记：Run 的 Activity 仍按它最后登记的队列排队等它回来。
func TestPurgeExpiredActivityWorkerVersionsKeepsWorkersTargetedByRunningRuns(t *testing.T) {
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	for _, identity := range []string{"pinned-pod", "gone-pod"} {
		worker := protocol.ActivityWorkerVersion{Identity: identity, BuildID: "v1", TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.Capture"}}
		if err := server.RegisterActivityWorkerVersion(context.Background(), worker); err != nil {
			t.Fatal(err)
		}
		if err := server.DeregisterActivityWorkerVersionInNamespace(context.Background(), protocol.DefaultNamespace, identity, "v1"); err != nil {
			t.Fatal(err)
		}
	}
	run := execution.NewRun(execution.NewRunOptions{ID: "run-pinned", WorkflowID: "wf-pinned", WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: clk.Now()})
	if err := run.ApplyStartPolicy("", "pinned-pod"); err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowStartedEvent(nil, run.ID, "", run.WorkflowID, "Workflow", 0)
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return execution.CommitStartedRun(tx, run, "", run.ID, run.ID, started)
	}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(expiredWorkerVersionRetention + time.Second)
	if purged, err := server.PurgeExpiredActivityWorkerVersions(); err != nil || purged != 1 {
		t.Fatalf("purge = %d, %v; want only the route of gone-pod", purged, err)
	}
	routes, err := server.workerVersions.ListActivityWorkerVersionsOfWorker(protocol.DefaultNamespace, "pinned-pod")
	if err != nil || len(routes) != 1 {
		t.Fatalf("routes of the targeted worker = %+v, %v; want kept", routes, err)
	}
	if _, err := server.resolveTargetedActivityRoute(dispatch.ActivityRouteRef{Namespace: protocol.DefaultNamespace, Type: "payments.Capture", TargetWorkerIdentity: "pinned-pod"}); err != nil {
		t.Fatalf("targeted route after purge: %v", err)
	}
}
