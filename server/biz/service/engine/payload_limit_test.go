package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 命令载荷超过上限：WFT 以 payload_size_limit_exceeded 失败并退避重试，Run 显示 Blocked，命令不落库。
func TestCommandPayloadOverLimitBlocksRun(t *testing.T) {
	blob := map[string]any{"blob": strings.Repeat("x", protocol.DefaultPayloadSizeLimit)}
	for name, command := range map[string]historypkg.Command{
		"schedule activity": {Type: historypkg.CmdScheduleActivity, ActivityID: "charge", ActivityType: "payments.charge", StartToCloseTimeout: time.Minute, Input: blob},
		"complete workflow": {Type: historypkg.CmdCompleteWorkflow, Output: blob},
		"continue as new":   {Type: historypkg.CmdContinueAsNew, Input: blob},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mgr := memory.NewExecutionManager()
			activityTasks := newTestTaskQueue()
			server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
			defer server.Stop()
			runID, wftID := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "payload-limit-" + strings.ReplaceAll(name, " ", "-"), Program: programtest.Noop()})
			if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{command}, ""); err != nil {
				t.Fatal(err)
			}
			state, err := server.GetWorkflowExecutionState(runID)
			if err != nil {
				t.Fatal(err)
			}
			if blocked := state.BlockedInfo(); !blocked.Blocked || blocked.Cause != execution.WorkflowTaskFailedCausePayloadSizeLimitExceeded {
				t.Fatalf("blocked info = %+v", blocked)
			}
			if status, _ := server.GetRunStatus(runID); status != historypkg.StatusRunning {
				t.Fatalf("run status = %s, want running", status)
			}
			if len(activityTasks.snapshot()) != 0 {
				t.Fatal("rejected command must not schedule the activity")
			}
		})
	}
}

// Workflow 输入超过上限时启动请求被拒绝，不创建 Run。
func TestWorkflowInputOverLimitIsRejected(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	wf := &program.WorkflowConfig{Type: "payload-limit-start", Program: programtest.Noop()}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"blob": strings.Repeat("x", protocol.DefaultPayloadSizeLimit)}
	_, err := server.StartWorkflow(ctx, "", wf.Type, "payload-limit-start", "", input)
	if !errors.Is(err, protocol.ErrInvalidRequest) || !strings.Contains(err.Error(), "payload size limit") {
		t.Fatalf("start with an oversized input: err = %v, want an invalid request naming the payload size limit", err)
	}
	if _, err := mgr.GetCurrentRunID(protocol.DefaultNamespace, "payload-limit-start"); !errors.Is(err, execution.ErrNoCurrentRun) {
		t.Fatalf("current run lookup err = %v, want no run", err)
	}
}

// Signal 输入与 Update 参数超过上限时请求被拒绝，History 不变。
func TestSignalAndUpdatePayloadOverLimitAreRejected(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	server := assembleServer(mgr, newTestTaskQueue(), newTestTaskQueue())
	defer server.Stop()
	runID, _ := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "payload-limit-signal", Program: programtest.Noop()})
	before, _ := mgr.GetEvents(runID)
	blob := strings.Repeat("x", protocol.DefaultPayloadSizeLimit)
	if _, err := server.SignalWorkflow(ctx, "", "", runID, "approve", "", map[string]any{"blob": blob}); !errors.Is(err, protocol.ErrInvalidRequest) || !strings.Contains(err.Error(), "signal input") {
		t.Fatalf("oversized signal: err = %v, want an invalid request naming the signal input", err)
	}
	if _, err := server.UpdateWorkflow(ctx, "", "", runID, "set", "update-1", "", protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`"` + blob + `"`)}}); !errors.Is(err, protocol.ErrInvalidRequest) || !strings.Contains(err.Error(), "update arguments") {
		t.Fatalf("oversized update: err = %v, want an invalid request naming the update arguments", err)
	}
	if after, _ := mgr.GetEvents(runID); len(after) != len(before) {
		t.Fatalf("rejected payloads changed history: %d -> %d events", len(before), len(after))
	}
}

// 异步完成（task token 路径）的结果超过上限：照 Temporal 记为不可重试的失败。
func TestAsyncCompletionOverLimitFailsActivity(t *testing.T) {
	ctx := context.Background()
	mgr := memory.NewExecutionManager()
	activityTasks := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), activityTasks)
	defer server.Stop()
	if err := server.RegisterActivityWorkerVersion(ctx, protocol.ActivityWorkerVersion{
		Identity: "act-worker", TaskQueues: []string{protocol.DefaultActivityTaskQueue}, ActivityTypes: []string{"payments.charge"},
	}); err != nil {
		t.Fatal(err)
	}
	runID, wftID := startContractRun(t, mgr, server, &program.WorkflowConfig{Type: "payload-limit-async", Program: programtest.Noop()})
	command := historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: "charge", ActivityType: "payments.charge", StartToCloseTimeout: time.Minute}
	if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, wftID), runID, wftID, []historypkg.Command{command}, ""); err != nil {
		t.Fatal(err)
	}
	queued := activityTasks.snapshot()
	if len(queued) != 1 {
		t.Fatalf("queued activity tasks = %d, want 1", len(queued))
	}
	task, err := server.tokens.Attach(queued[0].Task)
	if err != nil {
		t.Fatal(err)
	}
	output := map[string]any{"blob": strings.Repeat("y", protocol.DefaultPayloadSizeLimit)}
	if err := server.CompleteActivity(ctx, ActivityTaskToken(task.TaskToken), output, nil); err != nil {
		t.Fatal(err)
	}
	st, _, err := mgr.GetActivityState(runID, task.ScheduledEventID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != execution.ActivityStatusEnums.Failed {
		t.Fatalf("activity status = %s, want failed", st.Status)
	}
	events, _ := mgr.GetEvents(runID)
	for _, event := range events {
		if event.ActivityFailed != nil {
			if f := event.ActivityFailed.Failure; f == nil || f.Type != protocol.FailureTypePayloadSizeLimitExceeded || !f.NonRetryable {
				t.Fatalf("ActivityFailed failure = %+v", f)
			}
			return
		}
	}
	t.Fatal("no ActivityFailed event")
}
