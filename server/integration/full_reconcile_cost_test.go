package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// 全量补偿（DispatchPendingTasks）与启动恢复（RecoverRunningWorkflows）对没有待处理事项的 Running Run 不逐个读取：
// 每页 Running Run 是固定的几条批量查询，加上到期索引的候选查询。用例放 idleRuns 个只在等 30 天后 timer 的 Run，
// 断言两者的语句数与 Run 数无关（每页预算 pageStatements 条），并检查空闲 Run 的远期 timer 没有挂到本进程。
//
//	TENUTO_TEST_MYSQL=1 GOWORK=off go test ./server/integration -run TestFullReconcileCostIsPerPage -count=1 -v
func TestFullReconcileCostIsPerPage(t *testing.T) {
	mysqltest.Require(t)
	const (
		idleRuns = 450 // 3 页（每页 200）
		pages    = 3
		// 每页：Run 列表、页内 execution state、页内 open 子关联；另加最后一次空页与 ListRecoveryRunIDs 的 8 条查询。
		pageStatements      = 3
		candidateStatements = 8
	)
	db := mysqltest.NewDB(t)
	var statements atomic.Int64
	count := func(*gorm.DB) { statements.Add(1) }
	for name, err := range map[string]error{
		"query": db.Callback().Query().After("gorm:query").Register("cost:query", count),
		"row":   db.Callback().Row().After("gorm:row").Register("cost:row", count),
		"raw":   db.Callback().Raw().After("gorm:raw").Register("cost:raw", count),
	} {
		if err != nil {
			t.Fatalf("register %s callback: %v", name, err)
		}
	}
	store := openWorkflowStore(t, db, persistence.Config{})
	deadline := time.Now().Add(30 * 24 * time.Hour)
	for i := range idleRuns {
		runID := fmt.Sprintf("run-idle-%04d", i)
		run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: time.Now()})
		if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error {
			if err := tx.CreateRun(run); err != nil {
				return err
			}
			// 已处理完首个 Workflow Task、正在等一个 30 天后到期的 timer 的 Run。
			events := []historypkg.Event{
				historypkg.NewWorkflowStartedEvent(nil, runID, "", run.WorkflowID, "Workflow", 0),
				historypkg.NewWorkflowTaskScheduledEvent(0),
				historypkg.NewWorkflowTaskStartedEvent(2, "worker-1", "build-1", 0),
				historypkg.NewWorkflowTaskCompletedEvent(2, "worker-1"),
				{TimerScheduled: &historypkg.TimerScheduledEvent{TimerID: "t1"}},
			}
			var timerEventID int64
			for _, event := range events {
				id, err := tx.Append(runID, runID, runID, event)
				if err != nil {
					return err
				}
				timerEventID = id
			}
			return tx.UpsertTimerState(&execution.TimerState{
				RunID: runID, ScheduledEventID: timerEventID, TimerID: "t1", Kind: execution.TimerKindEnums.UserTimer,
				Deadline: deadline, Status: execution.TimerStatusEnums.Scheduled, CreatedAt: time.Now(), UpdatedAt: time.Now(),
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
	server := assembly.NewServer(assembly.Options{
		Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
	})
	defer server.Stop()

	if candidates, err := store.Executions.ListRecoveryRunIDs(time.Now().Add(time.Minute)); err != nil || len(candidates) != 0 {
		t.Fatalf("recovery candidates = %d, %v; want none", len(candidates), err)
	}
	budget := int64(candidateStatements + (pages+1)*pageStatements)
	for name, run := range map[string]func() error{
		"full reconciliation": func() error { _, err := server.DispatchPendingTasks(context.Background()); return err },
		"startup recovery": func() error {
			return server.RecoverRunningWorkflows(context.Background(), engine.DefaultReconcileLookahead)
		},
	} {
		statements.Store(0)
		started := time.Now()
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Logf("%s: %d 个空闲 Running Run，%d 条查询，耗时 %s", name, idleRuns, statements.Load(), time.Since(started))
		if got := statements.Load(); got > budget {
			t.Errorf("%s ran %d statements for %d idle runs, budget %d (per page, not per run)", name, got, idleRuns, budget)
		}
	}
}
