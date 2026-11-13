package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
)

// ackFailingTaskQueue 的 Ack 总是失败，模拟 TaskQ 分区换主期间确认送不到。
type ackFailingTaskQueue struct {
	*testTaskQueue
	err error
}

func (q *ackFailingTaskQueue) Ack(context.Context, protocol.TaskLease) error { return q.err }

// attempt started 落库之后 Worker 确认租约；确认失败时 AckActivityTask 不返回错误，只记 WARN。队列之后重投
// 同一份任务时 BeginActivityTask 按 started 判定不执行，原 attempt 的结果照常落库，Activity 只执行一次。
func TestActivityAckFailureAfterAttemptStartedDropsRedelivery(t *testing.T) {
	for name, ackErr := range map[string]error{
		"retries exhausted": errors.New("frontend: ack: max retries exceeded: rpc error: code = FailedPrecondition desc = not owner"),
		"stale token":       errors.New("frontend: ack: rpc error: code = InvalidArgument desc = stale ack token: token epoch 3 does not match queue epoch 4"),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var logs bytes.Buffer
			providers := tenutotel.NoopProviders()
			providers.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			mgr := memory.NewExecutionManager()
			activityTasks := &ackFailingTaskQueue{testTaskQueue: newTestTaskQueue(), err: ackErr}
			server := assembleServer(mgr, newTestTaskQueue(), activityTasks, WithTelemetry(providers))
			defer server.Stop()

			run := &execution.Run{ID: "run-ack-failure", WorkflowID: "wf", WorkflowType: "wf", Status: historypkg.StatusRunning}
			if err := mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
				t.Fatal(err)
			}
			workflowTaskID := appendOpenWorkflowTaskForTest(t, mgr, run.ID)
			if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, run.ID, workflowTaskID), run.ID, workflowTaskID, []historypkg.Command{{
				Type: historypkg.CmdScheduleActivity, ActivityID: "capture", ActivityType: "custodian.Capture", TaskQueue: "custodian",
				StartToCloseTimeout: time.Minute,
			}}, ""); err != nil {
				t.Fatal(err)
			}
			lease, err := activityTasks.Dequeue(ctx)
			if err != nil {
				t.Fatal(err)
			}
			lease.Task.WorkerIdentity = testActivityWorkerIdentity

			runtime := NewActivityRuntimeService(server)
			begin, err := runtime.BeginActivityTask(ctx, lease.Task)
			if err != nil || !begin.Execute {
				t.Fatalf("begin = %+v, %v; want Execute", begin, err)
			}
			if err := runtime.AckActivityTask(ctx, lease); err != nil {
				t.Fatalf("ack after attempt started = %v, want nil", err)
			}
			if !strings.Contains(logs.String(), "task queue ack failed") {
				t.Fatalf("ack failure was not logged; logs:\n%s", logs.String())
			}

			// TaskQ 在可见性超时后重投同一份任务。
			redelivered, err := runtime.BeginActivityTask(ctx, lease.Task)
			if err != nil {
				t.Fatal(err)
			}
			if redelivered.Execute || redelivered.Reason != "attempt_already_started" {
				t.Fatalf("redelivered begin = %+v, want attempt_already_started without Execute", redelivered)
			}
			if err := runtime.CompleteActivityTask(ctx, lease.Task, map[string]any{"ok": true}, nil); err != nil {
				t.Fatal(err)
			}
			events, err := mgr.GetEvents(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if started, completed := countEngineEvents(events, historypkg.EventActivityStarted), countEngineEvents(events, historypkg.EventActivityCompleted); started != 1 || completed != 1 {
				t.Fatalf("ActivityStarted = %d, ActivityCompleted = %d; want 1 each", started, completed)
			}
		})
	}
}

// WFT 的确认同样发生在结果上报落库之后：确认失败时 AckWorkflowTask 不返回错误。
func TestWorkflowTaskAckFailureIsNotAnError(t *testing.T) {
	workflowTasks := &ackFailingTaskQueue{testTaskQueue: newTestTaskQueue(), err: errors.New("frontend: ack: max retries exceeded")}
	server := assembleServer(memory.NewExecutionManager(), workflowTasks, newTestTaskQueue())
	defer server.Stop()

	lease := protocol.TaskLease{Task: protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-wft-ack", ScheduledEventID: 3}, Token: "wft-lease"}
	if err := server.AckWorkflowTask(context.Background(), lease); err != nil {
		t.Fatalf("ack = %v, want nil", err)
	}
}
