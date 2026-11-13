package schedules_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// StartLocalDriver 集成测试
// ============================================
// 测试策略:
// - Integration: memory store + LocalDriver → 到期 schedule 自动触发（P0-A+P0-B 验证）
// - Happy Path: Stop() 后 tick 停止，不再触发
// - 编译期验证: LocalDriver 实现 interface{ Stop() }
// ============================================

// TestStartLocalDriver_DueScheduleAutoTriggered 验证 StartLocalDriver
// 启动后，到期 schedule 会被自动触发。
// Integration: memory store + LocalDriver → 到期 schedule 自动 fire（P0-A+P0-B）
func TestStartLocalDriver_DueScheduleAutoTriggered(t *testing.T) {
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	// 已到期的 schedule
	past := time.Now().Add(-1 * time.Minute)
	sch := newTestSchedule("sch-local", "wf-local", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	// 用 StartLocalDriver 启动驱动（P0-A：真实启动路径）
	driver := schedules.StartLocalDriver(schedules.NewDriverTask(newTestUseCase(firer)))
	defer driver.Stop()

	// 最多等 3 秒（tick interval=1s），schedule 应被自动触发
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if firer.startCallCount() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if firer.startCallCount() == 0 {
		t.Fatal("StartLocalDriver: due schedule was not auto-triggered within 3 seconds")
	}
	t.Logf("StartLocalDriver: triggered after LocalDriver started, startCalls=%d", firer.startCallCount())
}

// TestStartLocalDriver_StopStopsTick 验证 Stop() 后 tick 不再触发。
// Happy Path: driver.Stop() 取消 ctx，DriverTask.Start() 退出，不再触发到期 schedule。
func TestStartLocalDriver_StopStopsTick(t *testing.T) {
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	driver := schedules.StartLocalDriver(schedules.NewDriverTask(newTestUseCase(firer)))
	driver.Stop()

	// 给 goroutine 退出时间
	time.Sleep(50 * time.Millisecond)

	// 停止后添加到期 schedule，不应被触发
	past := time.Now().Add(-1 * time.Minute)
	sch := newTestSchedule("sch-after-stop", "wf-after-stop", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	// 等超过一个 tick interval（1s），不应触发
	time.Sleep(1200 * time.Millisecond)

	if firer.startCallCount() > 0 {
		t.Fatalf("expected no triggers after Stop(), got %d", firer.startCallCount())
	}
}

// TestLocalDriver_ImplementsStopInterface 编译期+运行时验证接口满足。
// LocalDriver 必须实现 interface{ Stop() }，才能传给 Server.SetScheduleDriver()。
func TestLocalDriver_ImplementsStopInterface(t *testing.T) {
	// 编译期类型检查
	var _ interface{ Stop() } = (*schedules.LocalDriver)(nil)

	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	driver := schedules.StartLocalDriver(schedules.NewDriverTask(newTestUseCase(firer)))
	driver.Stop() // 不应 panic
}
