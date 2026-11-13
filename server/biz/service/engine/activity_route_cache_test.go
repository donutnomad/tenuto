package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// countingWorkerVersions 统计目录重建读取活跃登记的次数。
type countingWorkerVersions struct {
	*memory.WorkerVersionRepository
	listActive atomic.Int64
}

func (r *countingWorkerVersions) ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error) {
	r.listActive.Add(1)
	return r.WorkerVersionRepository.ListActiveActivityWorkerVersions(now)
}

func TestActivityRouteCatalogIsCachedBetweenCommands(t *testing.T) {
	clk := clocktest.NewFake(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	versions := &countingWorkerVersions{WorkerVersionRepository: memory.NewWorkerVersionRepository()}
	deps := assembleServerDeps(memory.NewExecutionManager(), newTestTaskQueue(), QueueActivityRouter{Queue: newTestTaskQueue()})
	deps.Clock = clk
	deps.WorkerVersions = versions
	server := NewServer(deps)
	defer server.Stop()

	// Happy Path：本副本上的登记立即重建目录，随后的大量选路沿用同一份快照。
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "pay-1", BuildID: "pay@v1", TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	reads := versions.listActive.Load()
	ref := dispatch.ActivityRouteRef{Namespace: protocol.DefaultNamespace, ActivityID: "charge", Type: "payments.charge"}
	for range 200 {
		if route, err := server.resolveActivityRoute(ref); err != nil || route.BuildID != "pay@v1" {
			t.Fatalf("route = %+v err=%v", route, err)
		}
	}
	if got := versions.listActive.Load() - reads; got != 0 {
		t.Fatalf("200 commands rebuilt the catalog %d times, want 0 within the cache TTL", got)
	}

	// 另一个副本写入的登记：快照有效期内不可见，过期后的下一条命令重建目录看到它。
	if _, err := versions.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "ship-1", BuildID: "ship@v1", TaskQueues: []string{"shipping"}, ActivityTypes: []string{"shipping.send"},
	}, clk.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	shipRef := dispatch.ActivityRouteRef{Namespace: protocol.DefaultNamespace, ActivityID: "send", Type: "shipping.send"}
	if _, err := server.resolveActivityRoute(shipRef); err == nil {
		t.Fatal("a registration on another replica was visible before the cache expired")
	}
	clk.Advance(activityRouteCacheTTL)
	if route, err := server.resolveActivityRoute(shipRef); err != nil || route.TaskQueue != "shipping" {
		t.Fatalf("route after cache expiry = %+v err=%v", route, err)
	}
	if got := versions.listActive.Load() - reads; got != 1 {
		t.Fatalf("catalog rebuilds after expiry = %d, want 1", got)
	}
}
