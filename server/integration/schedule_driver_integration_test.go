package integration

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestScheduleDriverAutoTriggersDueSchedule 覆盖"启动 driver 后 schedule 自动触发"路径。
//
// Integration: 持久化仓储（默认内存实现，TENUTO_TEST_MYSQL=1 时 MySQL）+ 真实 Server，验证 ScheduleDriverTask.Start 的 tick 循环
// 自动扫到到期 schedule 并触发其 workflow（schedule.Firer→fireSchedule→StartWorkflowInNamespace→CAS 推进）。
//
// 说明：本测试不经过 leader.Manager（dbleader 选主），直接验证 driver task 的触发逻辑端到端。
func TestScheduleDriverAutoTriggersDueSchedule(t *testing.T) {
	ctx := context.Background()
	stores := newWorkflowStores(t)

	workflowTasks := memory.NewTaskQueue(16)
	activityQueues := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: stores, WorkflowQueue: workflowTasks, ActivityRouter: activityQueues})
	defer server.Stop()

	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "cron-wf"},
		sdkgentest.Step{Type: "complete", Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}

	// 到期的 schedule：NextRunAt 在过去（但在 catchup window 内），driver tick 应自动触发
	past := time.Now().Add(-2 * time.Second)
	sch := &schedule.Schedule{
		ID:             "sch-auto",
		Namespace:      protocol.DefaultNamespace,
		WorkflowType:   wf.Type,
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		OverlapPolicy:  string(schedule.OverlapPolicyEnums.AllowAll),
		NextRunAt:      &past,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := stores.Schedules.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	// 启动 driver task（schedule.Firer 由 Server 实现），用真实 tick 循环
	task := schedules.NewDriverTask(server.ScheduleUseCase())
	driverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = task.Start(driverCtx) }()

	// 轮询等待 schedule 被自动触发：tick interval 为 1s，给足 5s
	deadline := time.Now().Add(5 * time.Second)
	var triggered *schedule.Schedule
	for time.Now().Before(deadline) {
		got, err := stores.Schedules.GetSchedule(protocol.DefaultNamespace, "sch-auto")
		if err != nil {
			t.Fatal(err)
		}
		if got.LastRunID != "" {
			triggered = got
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()

	if triggered == nil {
		t.Fatal("schedule was not auto-triggered by driver within 5s")
	}
	if triggered.RunCount != 1 {
		t.Errorf("RunCount: want 1, got %d", triggered.RunCount)
	}
	// NextRunAt 应已被推进到未来
	if triggered.NextRunAt == nil || !triggered.NextRunAt.After(time.Now()) {
		t.Errorf("NextRunAt should be advanced to future, got %v", triggered.NextRunAt)
	}
	// 触发的 run 确实存在于 store，且标记 Initiator=Cron
	firedRun, err := stores.Executions.Get(triggered.LastRunID)
	if err != nil {
		t.Errorf("triggered run %s not found in store: %v", triggered.LastRunID, err)
	} else if firedRun.Initiator != execution.InitiatorEnums.Cron {
		t.Errorf("cron-triggered run Initiator should be Cron, got %q", firedRun.Initiator)
	}
}
