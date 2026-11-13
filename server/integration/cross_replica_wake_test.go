package integration

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/worker"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

const crossReplicaUpdatesSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	limit := int64(0)
	done := false
	if err := workflow.SetUpdateHandler(ctx, "set-limit", func(ctx workflow.Context, next int64) (int64, error) {
		limit = next
		return limit, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, "finish", func(ctx workflow.Context) (int64, error) {
		done = true
		return limit, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.Await(ctx, func() bool { return done }); err != nil {
		return nil, err
	}
	return map[string]any{"limit": limit}, nil
}
`

// 同一个库上的两个 Server：A 只受理调用方请求，B 带 Worker 处理全部 Workflow Task。A 上等待 Update 结果与
// 等待 Run 结束的调用都在 B 提交之后很快返回，不等 Update 的 20s 软超时。TENUTO_TEST_MYSQL=1 时走 GORM 存储
// （两个 Store 实例连同一个库），否则两个 Server 共用内存存储。
func TestCrossReplicaUpdateAndCloseWake(t *testing.T) {
	stores := newReopenableWorkflowStores(t)
	tokens, err := dispatch.NewTaskTokenCodec([]byte("cross-replica-wake-secret-0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	workflowTasks := memory.NewTaskQueue(64)
	activityTasks := memory.NewActivityQueueRouter(64)
	replicaA := assembly.NewServer(assembly.Options{Stores: stores(), WorkflowQueue: workflowTasks, ActivityRouter: activityTasks, TaskTokens: tokens})
	t.Cleanup(replicaA.Stop)
	replicaB := assembly.NewEngine(assembly.Options{Stores: stores(), WorkflowQueue: workflowTasks, ActivityRouter: activityTasks, TaskTokens: tokens},
		activity.NewRegistry(), worker.WorkerOptions{WorkflowConcurrency: 2, ActivityConcurrency: 1})

	generated, err := sdkgen.GenerateFromSource([]byte(crossReplicaUpdatesSrc), sdkgen.Options{WorkflowID: "cross-replica-updates", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicaB.RegisterWorkflow(generated.Workflow); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	replicaB.Start(ctx)
	defer replicaB.Stop()

	run, err := replicaA.StartWorkflow(ctx, "", "cross-replica-updates", "cross-replica-1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	limit, err := protocol.NewJSONPayload(int64(7))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := replicaA.UpdateWorkflow(ctx, "", run.WorkflowID, run.ID, "set-limit", "set-limit-1", protocol.UpdateWaitStageEnums.Completed, protocol.Payloads{limit})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Completed || result.Outcome == nil || result.Outcome.Success == nil {
		t.Fatalf("update result = %+v", result)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("update completed on replica B reached replica A after %v", elapsed)
	}

	type closedReturn struct {
		run   string
		event historypkg.Event
		err   error
		at    time.Time
	}
	closed := make(chan closedReturn, 1)
	go func() {
		closedRun, event, err := replicaA.WaitRunClosed(ctx, "", run.WorkflowID, run.ID)
		var id string
		if closedRun != nil {
			id = closedRun.ID
		}
		closed <- closedReturn{id, event, err, time.Now()}
	}()
	finish, err := replicaA.UpdateWorkflow(ctx, "", run.WorkflowID, run.ID, "finish", "finish-1", protocol.UpdateWaitStageEnums.Completed, nil)
	if err != nil || finish.Stage != protocol.UpdateReachedStageEnums.Completed {
		t.Fatalf("finish = %+v, %v", finish, err)
	}
	finishedAt := time.Now()
	select {
	case got := <-closed:
		if got.err != nil || got.run != run.ID || got.event.WorkflowCompleted == nil {
			t.Fatalf("WaitRunClosed = %+v", got)
		}
		if lag := got.at.Sub(finishedAt); lag > time.Second {
			t.Fatalf("WaitRunClosed returned %v after the update that closed the run", lag)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitRunClosed on replica A did not return after replica B closed the run")
	}
}
