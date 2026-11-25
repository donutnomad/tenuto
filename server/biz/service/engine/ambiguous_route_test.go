package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestUndeclaredQueueAmbiguityBlocksScheduling 锁住调度时的歧义处理：Program 与调用处都没有声明队列，
// 两个服务以默认路由在互不相关的队列上提供同一类型与契约时，Workflow Task 记 activity_route_unavailable
// 并说明要声明队列，任务不入队、Run 不暂停；其中一个服务下线后，选路回到剩下的那个队列。
// 两个服务的 Worker 登记本身都成功：调用方声明了队列时它们可以并存。
func TestUndeclaredQueueAmbiguityBlocksScheduling(t *testing.T) {
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	contract := protocol.ActivityContract{Type: "payments.charge", Input: "ChargeInput.v1", Output: "Receipt.v1"}
	for _, w := range []struct{ identity, queue string }{{"payment-1", "payments"}, {"billing-1", "billing"}} {
		if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
			Identity: w.identity, BuildID: w.identity + "@v1", TaskQueues: []string{w.queue},
			ActivityTypes: []string{contract.Type}, Contracts: []protocol.ActivityContract{contract}, Default: true,
		}); err != nil {
			t.Fatalf("registering %s must succeed: %v", w.identity, err)
		}
	}
	wf := contractWorkflowConfig("undeclared-queue", map[string]program.ActivityRef{
		"charge": {Type: contract.Type, Input: contract.Input, Output: contract.Output},
	})
	runID, wftID := startContractRun(t, mgr, server, wf)
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(context.Background(), runID, wftID), runID, wftID, scheduleChargeCommand(), ""); err != nil {
		t.Fatal(err)
	}

	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" {
		t.Fatalf("ambiguity must not pause the run: %q", run.ConfigError)
	}
	state, err := server.GetWorkflowExecutionState(runID)
	if err != nil {
		t.Fatal(err)
	}
	blocked := state.BlockedInfo()
	if !blocked.Blocked || blocked.Cause != WorkflowTaskFailedCauseActivityRouteUnavailable ||
		!strings.Contains(blocked.Reason, "declares no task queue") {
		t.Fatalf("blocked info = %+v", blocked)
	}
	if len(activityTasks.snapshot()) != 0 {
		t.Fatalf("an ambiguous activity must not be queued, got %d tasks", len(activityTasks.snapshot()))
	}

	if err := server.DeregisterActivityWorkerVersionInNamespace(context.Background(), protocol.DefaultNamespace, "billing-1", "billing-1@v1"); err != nil {
		t.Fatal(err)
	}
	route, err := server.resolveActivityRoute(dispatch.ActivityRouteRef{
		Namespace: protocol.DefaultNamespace, ActivityID: "charge", Type: contract.Type,
		Input: contract.Input, Output: contract.Output,
	})
	if err != nil || route.TaskQueue != "payments" {
		t.Fatalf("route = %+v, err = %v; want payments once billing is gone", route, err)
	}
}
