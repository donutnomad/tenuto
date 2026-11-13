package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

// [高熵密钥] 生产路径回归：worker 契约经 gorm store 持久化、进程重启后由
// refreshActivityWorkerVersions 重建路由，route 仍带 Contracts。
// 重建后丢了契约，声明了契约的 Activity 会全部被判为不匹配。
func TestGormStoreRouteRebuildKeepsContracts(t *testing.T) {
	store, err := persistence.New(mysqltest.NewDB(t), persistence.Config{})
	if err != nil {
		t.Fatal(err)
	}
	contracts := []activity.Contract{{Type: "payments.charge.v1", Version: "v1", Input: "wire:in", Output: "wire:out"}}
	first := assemblePersistentServer(store, memory.NewTaskQueue(8), memory.NewActivityQueueRouter(8))
	if err := first.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Namespace: "default", Identity: "pay-worker", BuildID: "build-1",
		TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.charge.v1"},
		Contracts: contracts,
	}); err != nil {
		t.Fatal(err)
	}

	// 模拟服务重启：同一 store 上新建 Server，从 DB 重建 worker 路由。
	restarted := assemblePersistentServer(store, memory.NewTaskQueue(8), memory.NewActivityQueueRouter(8))
	if err := restarted.refreshActivityWorkerVersions(time.Now()); err != nil {
		t.Fatal(err)
	}
	route, err := restarted.resolveActivityRoute(dispatch.ActivityRouteRef{
		Namespace: "default", Type: "payments.charge.v1", TaskQueue: "payments",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(route.Contracts) != 1 || route.Contracts[0].Input != "wire:in" {
		t.Fatalf("route contracts after restart = %+v", route.Contracts)
	}
}
