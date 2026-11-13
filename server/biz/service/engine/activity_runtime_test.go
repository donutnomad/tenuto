package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// seedCancelRequestedActivityState 写入一个已开始、Workflow 已对它发出取消命令的 Activity。
func seedCancelRequestedActivityState(t *testing.T, mgr execution.ExecutionManager, runID string, scheduledEventID int64, stepID string) {
	t.Helper()
	if err := mgr.UpsertActivityState(&execution.ActivityState{
		RunID: runID, ScheduledEventID: scheduledEventID, ActivityID: stepID,
		Attempt: 1, Stamp: 1, StartedEventID: scheduledEventID,
		Status: execution.ActivityStatusEnums.Started, CancelRequested: true,
	}); err != nil {
		t.Fatal(err)
	}
}

// Workflow 的取消请求交给 Workflow 代码，本身不取消 Activity；Workflow 对 Activity 发出取消命令后，
// 心跳返回 activity_cancel_requested。
func TestActivityRuntimeHeartbeatReportsActivityCancellationOnly(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-heartbeat-cancel", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "slow-step"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ActivityID:       "slow-step",
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}
	// mutable-state 架构：heartbeat 权威读 state，seed 一个 started 状态供其写入心跳。
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledID, "slow-step", nil)
	if err := server.cancelRun(run.ID, ""); err != nil {
		t.Fatal(err)
	}

	runtime := NewActivityRuntimeService(server)
	task := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "slow-step",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1, // 严格 stamp：与 seed state 一致
	}
	result, err := runtime.RecordActivityHeartbeat(ctx, task, map[string]any{"offset": int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	if result.CancelRequested {
		t.Fatalf("workflow cancellation request must not cancel the activity, got %+v", result)
	}

	cancelledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "cancelled-step"}))
	if err != nil {
		t.Fatal(err)
	}
	seedCancelRequestedActivityState(t, mgr, run.ID, cancelledID, "cancelled-step")
	task.ActivityID, task.ScheduledEventID = "cancelled-step", cancelledID
	result, err = runtime.RecordActivityHeartbeat(ctx, task, map[string]any{"offset": int64(11)})
	if err != nil {
		t.Fatal(err)
	}
	if !result.CancelRequested || result.Reason != protocol.ActivityCancelRequestedReason {
		t.Fatalf("expected activity cancel requested result, got %+v", result)
	}

	// 两次心跳都记在 Activity state 上：取消不丢掉进度。
	for id, want := range map[int64]int64{scheduledID: 10, cancelledID: 11} {
		state, ok, err := mgr.GetActivityState(run.ID, id)
		if err != nil || !ok || state.LastHeartbeat["offset"] != want || state.LastHeartbeatAt == nil {
			t.Fatalf("activity %d heartbeat state = %+v, ok=%v err=%v; want offset %d", id, state, ok, err, want)
		}
	}
}

func TestActivityRuntimeRecordsActivityStartedWorkerInfo(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-activity-worker-info", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "capture", ActivityType: "custodian.Capture", TaskQueue: "custodian"}))
	if err != nil {
		t.Fatal(err)
	}
	// 只执行一次的 Activity 开始即写 ActivityStarted。
	seedScheduledActivityState(t, mgr, run.ID, scheduledID, "capture", &historypkg.RetryPolicy{MaxAttempts: 1})

	runtime := NewActivityRuntimeService(server)
	result, err := runtime.BeginActivityTask(ctx, protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "capture",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
		WorkerIdentity:   "custodian-worker-1",
		WorkerBuildID:    "custodian@2026-06-08",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Execute {
		t.Fatalf("expected activity execution, got %+v", result)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started *historypkg.ActivityStartedEvent
	for _, event := range history {
		if event.ActivityStarted != nil {
			started = event.ActivityStarted
		}
	}
	if started == nil || started.WorkerIdentity != "custodian-worker-1" || started.WorkerBuildID != "custodian@2026-06-08" {
		t.Fatalf("expected worker metadata on ActivityStarted, got %+v", started)
	}
}

func TestActivityRuntimeSkipsStartedAttemptDuplicate(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-activity-started-duplicate", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "capture", ActivityType: "custodian.Capture", TaskQueue: "custodian"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ActivityID:       "capture",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
	})); err != nil {
		t.Fatal(err)
	}
	// Begin 读 ActivityState：attempt 已 started 时队列重投的副本不执行，也不再写 ActivityStarted。
	seedStartedActivityStateWithRetry(t, mgr, run.ID, scheduledID, "capture", nil)

	runtime := NewActivityRuntimeService(server)
	result, err := runtime.BeginActivityTask(ctx, protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "capture",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Execute || result.Reason != "attempt_already_started" {
		t.Fatalf("expected duplicate started attempt to be skipped, got %+v", result)
	}

	history, err := mgr.GetEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(history, historypkg.EventActivityStarted) != 1 {
		t.Fatalf("expected the duplicate to leave the start event alone, got %+v", history)
	}
}

func TestActivityRuntimeBeginReturnsStartEventWriteError(t *testing.T) {
	ctx := context.Background()
	mgr := newFailingEventManager()
	mgr.failOn(historypkg.EventActivityStarted)
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-activity-start-error", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "capture", ActivityType: "custodian.Capture", TaskQueue: "custodian"}))
	if err != nil {
		t.Fatal(err)
	}
	// 只执行一次的 activity（非 transient）start 会写 ActivityStarted 进 history，
	// 失败注入命中。seed 一个 scheduled 状态供 start 路径读取。
	seedScheduledActivityState(t, mgr, run.ID, scheduledID, "capture", &historypkg.RetryPolicy{MaxAttempts: 1})

	runtime := NewActivityRuntimeService(server)
	result, err := runtime.BeginActivityTask(ctx, protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "capture",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1,
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
	})
	if err == nil {
		t.Fatalf("expected begin to return start event write error, got result %+v", result)
	}
}

// ============================================
// RecordActivityHeartbeat 先记录再检查取消测试
// ============================================
// 测试策略:
// - Happy Path: activity 已取消时，心跳 details 仍被持久化，
//   同时返回 CancelRequested=true
// ============================================

func TestActivityRuntimeHeartbeatSavesDetailsBeforeCancelCheck(t *testing.T) {
	// Happy Path: Workflow 已请求取消 Activity 时，heartbeat details 先被持久化，
	// 然后返回 CancelRequested=true。
	// 这确保重试时可通过 LastHeartbeatDetails 恢复进度。
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()

	run := &execution.Run{ID: "run-heartbeat-save-then-cancel", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduledID, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "slow-step"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append(run.ID, run.ID, run.ID, historypkg.NewActivityStartedEvent(historypkg.ActivityStartedEvent{
		ActivityID:       "slow-step",
		ScheduledEventID: scheduledID,
		Attempt:          1,
	})); err != nil {
		t.Fatal(err)
	}

	// Workflow 已请求取消这个 Activity：HandleActivityHeartbeat 放行写入，
	// CheckActivityTaskCancellation 读 ActivityState 的 CancelRequested 返回取消。
	seedCancelRequestedActivityState(t, mgr, run.ID, scheduledID, "slow-step")

	runtime := NewActivityRuntimeService(server)
	result, err := runtime.RecordActivityHeartbeat(ctx, protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            run.ID,
		ActivityID:       "slow-step",
		ScheduledEventID: scheduledID,
		Attempt:          1,
		Stamp:            1, // 严格 stamp：与 seed state 一致
	}, map[string]any{"progress": int64(42)})
	if err != nil {
		t.Fatal(err)
	}
	if !result.CancelRequested {
		t.Fatalf("expected CancelRequested=true, got %+v", result)
	}

	state, ok, err := mgr.GetActivityState(run.ID, scheduledID)
	if err != nil || !ok || state.LastHeartbeat["progress"] != int64(42) {
		t.Fatalf("expected heartbeat to be recorded before cancel check, got %+v ok=%v err=%v", state, ok, err)
	}
}
