package memory_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch/dispatchtest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestDispatchRepositoryContract(t *testing.T) {
	dispatchtest.RunRepositoryContract(t, func(*testing.T) dispatch.Repository { return memory.NewDispatchRepository() })
}

func TestWorkerVersionRepositoryContract(t *testing.T) {
	dispatchtest.RunWorkerVersionRepositoryContract(t, func(*testing.T) dispatch.WorkerVersionRepository {
		return memory.NewWorkerVersionRepository()
	})
}

// DispatchRepository.ReleaseTaskDispatch 的守卫必须与 gormstore 侧逐条一致，
// 否则两个后端在"同一 activity 会不会被并发执行"这个 P0 语义上漂移
//（此前已多次出现两侧漂移）。对照用例见
// server/infra/persistence/mysql_dispatch_lease_guard_test.go。

func TestLocalReleaseTaskDispatchDoesNotClearActiveLease(t *testing.T) {
	store := memory.NewDispatchRepository()
	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeActivity,
		RunID: "run-local-guard", ScheduledEventID: 7, Attempt: 1,
		ActivityType: "charge", TaskQueue: "default",
	}

	lease, ok, err := store.AcquireTaskDispatch(task, time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireTaskDispatch ok=%v err=%v", ok, err)
	}

	// 兜底清理（不持 leaseID）不得清掉活跃租约。
	if err := store.ReleaseTaskDispatch(task, "recovery"); err != nil {
		t.Fatalf("ReleaseTaskDispatch: %v", err)
	}

	state, found, err := store.GetTaskDispatch(task)
	if err != nil || !found {
		t.Fatalf("GetTaskDispatch found=%v err=%v", found, err)
	}
	if state.LeaseID != lease.LeaseID || state.Status != dispatch.StatusLeased {
		t.Fatalf("活跃租约被 Release 清掉了: status=%q lease_id=%q want status=%q lease_id=%q",
			state.Status, state.LeaseID, dispatch.StatusLeased, lease.LeaseID)
	}

	// 关键：清不掉 ⇒ 抢不到 ⇒ 同一 activity 不会被两个 worker 并发执行。
	if _, ok, err := store.AcquireTaskDispatch(task, time.Minute); err != nil || ok {
		t.Fatalf("持有活跃租约期间不应抢到: ok=%v err=%v", ok, err)
	}
}

func TestLocalReleaseTaskDispatchClearsExpiredLease(t *testing.T) {
	store := memory.NewDispatchRepository()
	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-local-expired", ScheduledEventID: 11,
	}
	if _, ok, err := store.AcquireTaskDispatch(task, -time.Minute); err != nil || !ok {
		t.Fatalf("AcquireTaskDispatch ok=%v err=%v", ok, err)
	}

	if err := store.ReleaseTaskDispatch(task, "sticky schedule-to-start timeout"); err != nil {
		t.Fatalf("ReleaseTaskDispatch: %v", err)
	}
	state, found, err := store.GetTaskDispatch(task)
	if err != nil || !found {
		t.Fatalf("GetTaskDispatch found=%v err=%v", found, err)
	}
	if state.LeaseID != "" || state.Status != dispatch.StatusFailed {
		t.Fatalf("过期租约未被清理: status=%q lease_id=%q", state.Status, state.LeaseID)
	}
	if _, ok, err := store.AcquireTaskDispatch(task, time.Minute); err != nil || !ok {
		t.Fatalf("清理后应可重新 Acquire: ok=%v err=%v", ok, err)
	}
}

// TestLocalReleaseTaskDispatchClearsDispatched 覆盖 status=dispatched 分支：
// 已交付队列不算活跃租约，redispatchActivityTask 的 reset 重派依赖它可被清。
func TestLocalReleaseTaskDispatchClearsDispatched(t *testing.T) {
	store := memory.NewDispatchRepository()
	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeActivity,
		RunID: "run-local-dispatched", ScheduledEventID: 5, Attempt: 1,
		ActivityType: "notify", TaskQueue: "default",
	}
	lease, ok, err := store.AcquireTaskDispatch(task, time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireTaskDispatch ok=%v err=%v", ok, err)
	}
	if err := store.MarkTaskDispatchDispatched(task, lease.LeaseID); err != nil {
		t.Fatalf("MarkTaskDispatchDispatched: %v", err)
	}
	if err := store.ReleaseTaskDispatch(task, "redispatch"); err != nil {
		t.Fatalf("ReleaseTaskDispatch: %v", err)
	}
	if _, ok, err := store.AcquireTaskDispatch(task, time.Minute); err != nil || !ok {
		t.Fatalf("dispatched 行清理后应可重新 Acquire: ok=%v err=%v", ok, err)
	}
}
