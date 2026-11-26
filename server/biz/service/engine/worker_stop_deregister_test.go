package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	activity "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
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
