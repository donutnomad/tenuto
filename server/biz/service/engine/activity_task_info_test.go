package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Activity Task 带上调度它的 Workflow 身份与首次调度时间，begin 返回 Server 记下的开始时间：
// Worker 据此填 activity.Info。重派的 Task（重试、恢复）带同样的字段。
func TestActivityTaskCarriesWorkflowIdentityAndTimes(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := memory.NewActivityQueueRouter(8)
	server := assembleServerWithActivityRouter(mgr, newTestTaskQueue(), queue)
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{Identity: "A", BuildID: "v1", TaskQueues: []string{"payments"}, ActivityTypes: []string{"charge"}}); err != nil {
		t.Fatal(err)
	}
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "order-flow", Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"charge": {Symbol: "charge", Type: "charge"},
		}})
	if err != nil {
		t.Fatal(err)
	}
	run := &execution.Run{ID: "run-info", Namespace: "default", WorkflowID: "order-7", WorkflowType: ref.WorkflowID, WorkflowConfigVersion: ref.Version, Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	scheduled := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), run.ID, scheduled), run.ID, scheduled, []history.Command{{
		Type: history.CmdScheduleActivity, ActivityID: "#a1", ActivityType: "charge", TaskQueue: "payments", StartToCloseTimeout: time.Minute,
	}}, ""); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(protocol.WithWorkerInfo(protocol.WithNamespace(context.Background(), "default"), protocol.WorkerInfo{Namespace: "default", Identity: "A", BuildID: "v1"}), time.Second)
	defer cancel()
	lease, err := server.PollActivityTask(ctx, "payments")
	if err != nil {
		t.Fatal(err)
	}
	states, err := mgr.ListActivityStates(run.ID)
	if err != nil || len(states) != 1 {
		t.Fatalf("activity states = %+v, err = %v", states, err)
	}
	task := lease.Task
	if task.WorkflowID != "order-7" || task.WorkflowType != "order-flow" || !task.ScheduledTime.Equal(states[0].ScheduledTime) {
		t.Fatalf("dispatched task identity = %q/%q scheduled %v, want order-7/order-flow scheduled %v", task.WorkflowID, task.WorkflowType, task.ScheduledTime, states[0].ScheduledTime)
	}
	task.WorkerIdentity, task.WorkerBuildID = "A", "v1"
	result, err := NewActivityRuntimeService(server).BeginActivityTask(ctx, task)
	if err != nil || !result.Execute {
		t.Fatalf("begin = %+v, %v", result, err)
	}
	started, _, err := mgr.GetActivityState(run.ID, task.ScheduledEventID)
	if err != nil || started.StartedTime == nil || !result.StartedTime.Equal(*started.StartedTime) {
		t.Fatalf("begin StartedTime = %v, state = %+v, err = %v", result.StartedTime, started, err)
	}
	redispatched := server.activityTaskFromState(started)
	if redispatched.WorkflowID != "order-7" || redispatched.WorkflowType != "order-flow" || !redispatched.ScheduledTime.Equal(started.ScheduledTime) {
		t.Fatalf("redispatched task = %+v", redispatched)
	}
}
