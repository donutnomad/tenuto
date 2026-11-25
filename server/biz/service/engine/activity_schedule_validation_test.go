package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 调度命令既没有 StartToCloseTimeout 也没有 ScheduleToCloseTimeout：WFT 记为失败并退避重试，
// Run 显示 Blocked，命令不落 ActivityScheduled、不入队。
func TestScheduleActivityWithoutCloseTimeoutBlocksRun(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()

	runID, wftID := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "missing-activity-timeout", Program: programtest.Noop()})
	command := historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: "charge", ActivityType: "payments.charge"}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{command}, ""); err != nil {
		t.Fatal(err)
	}

	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked := state.BlockedInfo(); !blocked.Blocked || blocked.Cause != execution.WorkflowTaskFailedCauseBadScheduleActivityAttributes {
		t.Fatalf("blocked info = %+v", blocked)
	}
	events, err := mgr.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	if countEngineEvents(events, historypkg.EventActivityScheduled) != 0 || len(activityTasks.snapshot()) != 0 {
		t.Fatalf("rejected command must not schedule the activity: events=%+v", events)
	}
}

// ActivityID 与仍未关闭的 Activity 重复：同一批里的两条调度命令、或与上一个 Workflow Task 调度且尚未关闭的
// Activity 重复，WFT 都以 schedule_activity_duplicate_id 失败，Run 显示 Blocked，重复的命令不落库。
func TestScheduleActivityDuplicateIDBlocksRun(t *testing.T) {
	schedule := func(id string) historypkg.Command {
		return historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: id, ActivityType: "work", StartToCloseTimeout: time.Minute}
	}
	assertDuplicateBlocked := func(t *testing.T, server *Server, mgr execution.ExecutionManager, runID string, wantScheduled int) {
		t.Helper()
		state, err := server.GetWorkflowExecutionState(runID)
		if err != nil {
			t.Fatal(err)
		}
		blocked := state.BlockedInfo()
		if !blocked.Blocked || blocked.Cause != execution.WorkflowTaskFailedCauseScheduleActivityDuplicateID ||
			!strings.Contains(blocked.Reason, `activity "charge" (type work)`) {
			t.Fatalf("blocked info = %+v", blocked)
		}
		events, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		if got := countEngineEvents(events, historypkg.EventActivityScheduled); got != wantScheduled {
			t.Fatalf("ActivityScheduled events = %d, want %d", got, wantScheduled)
		}
	}

	t.Run("same batch", func(t *testing.T) {
		ctx := context.Background()
		mgr := memory.NewExecutionManager()
		server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
		defer server.Stop()
		runID, wftID := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "duplicate-activity-id-batch", Program: programtest.Noop()})
		commands := []historypkg.Command{schedule("charge"), schedule("charge")}
		if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, commands, ""); err != nil {
			t.Fatal(err)
		}
		assertDuplicateBlocked(t, server, mgr, runID, 0)
	})

	t.Run("open activity", func(t *testing.T) {
		ctx := context.Background()
		mgr := memory.NewExecutionManager()
		server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
		defer server.Stop()
		runID, wftID := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "duplicate-activity-id-open", Program: programtest.Noop()})
		if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{schedule("charge")}, ""); err != nil {
			t.Fatal(err)
		}
		next := appendOpenWorkflowTaskForTest(t, mgr, runID)
		if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, next), runID, next, []historypkg.Command{schedule("charge")}, ""); err != nil {
			t.Fatal(err)
		}
		assertDuplicateBlocked(t, server, mgr, runID, 1)
	})
}
