package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Schedule 触发经真实 Server 启动 Run 的测试。
// CRUD、分页、过滤与触发编排的单元测试在 server/biz/usecase/schedules。

func TestTriggerSchedule(t *testing.T) {
	// Happy Path: 每次触发启动一个 Workflow ID 为 schedule-<Schedule ID>-<触发时刻> 的 Run，并记录 lastRunAt/lastRunID/runCount
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-trigger")

	created, err := server.ScheduleUseCase().Create(schedules.CreateRequest{
		WorkflowType:   "wf-trigger",
		CronExpression: "0 0 * * *",
		Input:          map[string]any{"source": "schedule"},
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}
	if created.RunCount != 0 || created.LastRunAt != nil {
		t.Fatalf("expected no run yet, got run_count=%d last_run_at=%v", created.RunCount, created.LastRunAt)
	}

	runID1, err := server.ScheduleUseCase().Trigger(ctx, "default", created.ID)
	if err != nil {
		t.Fatalf("TriggerSchedule failed: %v", err)
	}
	afterTrigger1 := getSchedule(t, server, created.ID)
	if afterTrigger1.RunCount != 1 || afterTrigger1.LastRunID != runID1 || afterTrigger1.LastRunAt == nil || afterTrigger1.NextRunAt == nil {
		t.Fatalf("after first trigger: %+v", afterTrigger1)
	}
	assertScheduleRun(t, mgr, runID1, created.RunWorkflowID(testClockStart), "wf-trigger", history.StatusRunning)

	if err := server.completeWorkflow(runID1, nil); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	runID2, err := server.ScheduleUseCase().Trigger(ctx, "default", created.ID)
	if err != nil {
		t.Fatalf("second TriggerSchedule failed: %v", err)
	}
	if afterTrigger2 := getSchedule(t, server, created.ID); afterTrigger2.RunCount != 2 || afterTrigger2.LastRunID != runID2 {
		t.Fatalf("after second trigger: %+v", afterTrigger2)
	}
	assertScheduleRun(t, mgr, runID2, created.RunWorkflowID(clk.Now()), "wf-trigger", history.StatusRunning)
	assertScheduleRun(t, mgr, runID1, created.RunWorkflowID(testClockStart), "wf-trigger", history.StatusCompleted)
}

// SKIP 按上一次触发所在 Workflow ID 的 current Run 判定：上一次的 Run continue-as-new 之后，接续的 Run 仍在运行时
// 到期与手动触发都跳过；它结束之后下一个到期点启动新 Run。
func TestScheduleSkipFollowsTheContinuedRun(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	ref := registerScheduleWorkflow(t, server, "wf-skip-chain")
	uc := server.ScheduleUseCase()

	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-skip-chain", CronExpression: "*/5 * * * *", OverlapPolicy: "SKIP"})
	if err != nil {
		t.Fatal(err)
	}
	firstDue := *created.NextRunAt
	clk.Advance(firstDue.Sub(clk.Now()))
	uc.FireDue(ctx)
	first := getSchedule(t, server, created.ID)
	if first.RunCount != 1 {
		t.Fatalf("first due point: run_count = %d, want 1", first.RunCount)
	}
	assertScheduleRun(t, mgr, first.LastRunID, created.RunWorkflowID(firstDue), "wf-skip-chain", history.StatusRunning)

	// 上一次触发的 Run continue-as-new：LastRunID 已结束，接续的 Run 仍在运行。
	continuedID := execution.NewID()
	if err := server.startContinuedRunWithRef(ctx, first.LastRunID, continuedID, nil, ref); err != nil {
		t.Fatal(err)
	}
	if status, _ := server.GetRunStatus(first.LastRunID); status != history.StatusContinuedAsNew {
		t.Fatalf("last run status = %s, want continued-as-new", status)
	}

	clk.Advance(5 * time.Minute)
	uc.FireDue(ctx)
	if got := getSchedule(t, server, created.ID); got.RunCount != 1 {
		t.Fatalf("second due point while the continued run is running: run_count = %d, want 1", got.RunCount)
	}
	if _, err := uc.Trigger(ctx, "default", created.ID); !errors.Is(err, schedule.ErrOverlapSkipped) {
		t.Fatalf("manual trigger while the continued run is running: err = %v, want ErrOverlapSkipped", err)
	}

	if err := server.completeWorkflow(continuedID, nil); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	uc.FireDue(ctx)
	third := getSchedule(t, server, created.ID)
	if third.RunCount != 2 || third.LastRunID == first.LastRunID {
		t.Fatalf("due point after the chain finished: run_count = %d last_run_id = %s", third.RunCount, third.LastRunID)
	}
	assertScheduleRun(t, mgr, third.LastRunID, created.RunWorkflowID(clk.Now().Truncate(5*time.Minute)), "wf-skip-chain", history.StatusRunning)
}

// ALLOW_ALL 的触发与仍在运行的 Run 并行：各有自己的 Workflow ID，先启动的 Run 仍是它那个 Workflow ID 的 current。
func TestScheduleAllowAllRunsInParallel(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-allow-all")
	uc := server.ScheduleUseCase()

	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-allow-all", CronExpression: "*/5 * * * *", OverlapPolicy: "ALLOW_ALL"})
	if err != nil {
		t.Fatal(err)
	}
	firstDue := *created.NextRunAt
	clk.Advance(firstDue.Sub(clk.Now()))
	uc.FireDue(ctx)
	firstRunID := getSchedule(t, server, created.ID).LastRunID
	clk.Advance(5 * time.Minute)
	uc.FireDue(ctx)
	second := getSchedule(t, server, created.ID)
	if second.RunCount != 2 || second.LastRunID == firstRunID {
		t.Fatalf("second due point: run_count = %d last_run_id = %s", second.RunCount, second.LastRunID)
	}
	assertScheduleRun(t, mgr, firstRunID, created.RunWorkflowID(firstDue), "wf-allow-all", history.StatusRunning)
	assertScheduleRun(t, mgr, second.LastRunID, created.RunWorkflowID(firstDue.Add(5*time.Minute)), "wf-allow-all", history.StatusRunning)
}

// 同一个到期点的 Workflow ID 已有运行中的 Run 时，Schedule 的启动不接替它：返回 ErrCurrentRunConflict。
func TestStartWorkflowForCronDoesNotReplaceARunningRun(t *testing.T) {
	ctx := context.Background()
	server := newTestServer()
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-cron-conflict")

	runID, err := server.StartWorkflowForCron(ctx, "", "wf-cron-conflict", "schedule-x-2026-11-10T09:05:00Z", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.StartWorkflowForCron(ctx, "", "wf-cron-conflict", "schedule-x-2026-11-10T09:05:00Z", nil, nil); !errors.Is(err, execution.ErrCurrentRunConflict) {
		t.Fatalf("second start: err = %v, want ErrCurrentRunConflict", err)
	}
	if status, _ := server.GetRunStatus(runID); status != history.StatusRunning {
		t.Fatalf("first run status = %s, want running", status)
	}
}

func registerScheduleWorkflow(t *testing.T, server *Server, workflowType string) definition.WorkflowConfigRef {
	t.Helper()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{ID: workflowType, Name: workflowType, Program: programtest.Noop()})
	if err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}
	return ref
}

func getSchedule(t *testing.T, server *Server, scheduleID string) *schedule.Schedule {
	t.Helper()
	got, err := server.ScheduleUseCase().Get("default", scheduleID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// assertScheduleRun 断言 runID 的 Workflow ID 与类型、它是该 Workflow ID 的 current Run，以及它的状态。
func assertScheduleRun(t *testing.T, mgr execution.ExecutionManager, runID, workflowID, workflowType string, status history.Status) {
	t.Helper()
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowID != workflowID || run.WorkflowType != workflowType {
		t.Fatalf("run %s: workflow id/type = %s/%s, want %s/%s", runID, run.WorkflowID, run.WorkflowType, workflowID, workflowType)
	}
	current, err := mgr.GetCurrentRunID("default", workflowID)
	if err != nil || current != runID {
		t.Fatalf("current run of %s = %s (%v), want %s", workflowID, current, err, runID)
	}
	state, err := mgr.GetWorkflowExecutionState(runID)
	if err != nil || state.Status != status {
		t.Fatalf("run %s status = %v (%v), want %s", runID, state, err, status)
	}
}

func TestTriggerSchedule_Paused(t *testing.T) {
	// Edge Case: 暂停状态的 schedule 仍可手动触发，但 nextRunAt 保持 nil
	ctx := context.Background()
	server := newTestServer()

	wf := &program.WorkflowConfig{
		Program: programtest.Noop(),
		ID:      "wf-trigger-paused",
		Name:    "Trigger Paused Test",
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	created, err := server.ScheduleUseCase().Create(schedules.CreateRequest{
		WorkflowType:   "wf-trigger-paused",
		CronExpression: "0 0 * * *",
		Paused:         true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}
	if created.NextRunAt != nil {
		t.Fatal("expected next_run_at=nil for paused schedule")
	}

	runID, err := server.ScheduleUseCase().Trigger(ctx, "default", created.ID)
	if err != nil {
		t.Fatalf("TriggerSchedule on paused schedule failed: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID from paused schedule trigger")
	}

	afterTrigger, err := server.ScheduleUseCase().Get("default", created.ID)
	if err != nil {
		t.Fatalf("GetSchedule after trigger failed: %v", err)
	}
	if afterTrigger.RunCount != 1 {
		t.Fatalf("expected run_count=1, got %d", afterTrigger.RunCount)
	}
	if afterTrigger.LastRunID != runID {
		t.Fatalf("expected last_run_id=%s, got %s", runID, afterTrigger.LastRunID)
	}
	if afterTrigger.NextRunAt != nil {
		t.Fatalf("expected next_run_at=nil for paused schedule after trigger, got %v", afterTrigger.NextRunAt)
	}
}

// Schedule 启动的 Run 带 TemporalScheduledById 与 TemporalScheduledStartTime；按 Schedule ID 过滤的列表与批量操作
// 只命中这个 Schedule 的 Run，调用端不能写这两个键。
func TestScheduleRunsCarryScheduleSearchAttributes(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server, clk := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	registerScheduleWorkflow(t, server, "wf-sched-attrs")
	uc := server.ScheduleUseCase()

	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-sched-attrs", CronExpression: "0 0 * * *", OverlapPolicy: "ALLOW_ALL"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-sched-attrs", CronExpression: "0 0 * * *"})
	if err != nil {
		t.Fatal(err)
	}
	var runIDs []string
	var fireTimes []string
	for range 2 {
		runID, err := uc.Trigger(ctx, "default", created.ID)
		if err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, runID)
		fireTimes = append(fireTimes, clk.Now().UTC().Truncate(time.Second).Format(time.RFC3339))
		clk.Advance(time.Minute)
	}
	otherRunID, err := uc.Trigger(ctx, "default", other.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.StartWorkflow(ctx, "", "wf-sched-attrs", "not-scheduled", "", nil); err != nil {
		t.Fatal(err)
	}

	filter := execution.RunListFilter{
		Namespace: "default", IncludeTerminal: true,
		SearchAttributes: map[string]string{history.ScheduledByIDSearchAttribute: created.ID},
	}
	listed, err := server.View().ListRuns(filter)
	if err != nil {
		t.Fatal(err)
	}
	if listed.Total != 2 || len(listed.Items) != 2 {
		t.Fatalf("runs of schedule %s = %d (%+v), want 2", created.ID, listed.Total, listed.Items)
	}
	for i, item := range listed.Items {
		if item.RunID != runIDs[i] || item.SearchAttributes[history.ScheduledStartTimeSearchAttribute] != fireTimes[i] {
			t.Errorf("item %d = run %s attrs %v, want run %s scheduled at %s", i, item.RunID, item.SearchAttributes, runIDs[i], fireTimes[i])
		}
	}
	events, err := mgr.GetEvents(runIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if started := events[0].WorkflowStarted; started == nil || started.SearchAttributes[history.ScheduledByIDSearchAttribute] != created.ID {
		t.Fatalf("WorkflowStarted = %+v, want the schedule search attributes recorded at start", events[0])
	}

	for _, key := range []string{history.ScheduledByIDSearchAttribute, history.ScheduledStartTimeSearchAttribute} {
		if err := server.UpsertSearchAttributes(ctx, "", "", runIDs[0], map[string]string{key: "x"}); !errors.Is(err, protocol.ErrInvalidRequest) {
			t.Errorf("upsert %s: err = %v, want ErrInvalidRequest", key, err)
		}
	}

	op, err := driveBatch(server, batches.StartRequest{Namespace: "default", Type: batch.TypeEnums.Terminate, Filter: filter, Reason: "schedule removed"})
	if err != nil {
		t.Fatal(err)
	}
	if op.SucceededCount != 2 {
		t.Fatalf("batch terminate by schedule: succeeded = %d, want 2", op.SucceededCount)
	}
	if status, _ := server.GetRunStatus(otherRunID); status != history.StatusRunning {
		t.Fatalf("other schedule's run status = %s, want running", status)
	}
}
