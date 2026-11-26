package integration

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/worker"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 内嵌的 Engine 重启后，启动恢复只挂 lookahead 内到期的定时器；更晚到期的 timer 由 Engine 运行的周期补偿
// （engine.Reconciler）在进入 lookahead 时挂上，并在截止时间准点触发。
func TestEngineRestartFiresTimerBeyondStartupLookahead(t *testing.T) {
	ctx := context.Background()
	stores := newWorkflowStores(t)
	clk := clocktest.NewFake(time.Now().UTC().Truncate(time.Millisecond))
	const interval = 50 * time.Millisecond // lookahead 100ms
	deadline := clk.Now().Add(10 * time.Minute)

	// 重启前的进程留下一个已处理完首个 WFT、正在等 10 分钟后 timer 的 Run。
	const runID = "run-engine-restart-timer"
	run := execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + runID, WorkflowType: "wf", WorkflowConfigVersion: "v1", CreatedAt: clk.Now()})
	var timerEventID int64
	if err := stores.Executions.RunInTx(func(tx execution.ExecutionTx) error {
		if err := tx.CreateRun(run); err != nil {
			return err
		}
		for _, event := range []historypkg.Event{
			historypkg.NewWorkflowStartedEvent(nil, runID, "", run.WorkflowID, "Workflow", 0),
			historypkg.NewWorkflowTaskScheduledEvent(0),
			historypkg.NewWorkflowTaskStartedEvent(2, "worker-1", "build-1", 0),
			historypkg.NewWorkflowTaskCompletedEvent(2, "worker-1"),
			{TimerScheduled: &historypkg.TimerScheduledEvent{TimerID: "t1"}},
		} {
			id, err := tx.Append(runID, runID, runID, event)
			if err != nil {
				return err
			}
			timerEventID = id
		}
		return tx.UpsertTimerState(&execution.TimerState{
			RunID: runID, ScheduledEventID: timerEventID, TimerID: "t1", Kind: execution.TimerKindEnums.UserTimer,
			Deadline: deadline, Status: execution.TimerStatusEnums.Scheduled, CreatedAt: clk.Now(), UpdatedAt: clk.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}

	eng := assembly.NewEngine(assembly.Options{
		Stores: stores, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16),
		Clock: clk, ReconcileInterval: interval, ReconcileFullScanInterval: time.Hour,
	}, activity.NewRegistry(), worker.WorkerOptions{WorkflowConcurrency: 1, ActivityConcurrency: 1})
	eng.Start(ctx)
	defer eng.Stop()

	timerFired := func() bool {
		events, err := stores.Executions.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.TimerFired != nil {
				return true
			}
		}
		return false
	}
	// 截止时间进入 lookahead：等几轮周期补偿把 timer 挂上，此时还没到期。
	clk.Advance(deadline.Sub(clk.Now()) - interval/2)
	time.Sleep(6 * interval)
	if timerFired() {
		t.Fatal("timer fired before its deadline")
	}
	clk.Advance(interval / 2)
	waitUntil := time.Now().Add(3 * time.Second)
	for !timerFired() {
		if time.Now().After(waitUntil) {
			t.Fatal("timer beyond the startup lookahead never fired after an Engine restart")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
