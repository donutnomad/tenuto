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

// ============================================
// Run 关闭时未完成的 Update
// ============================================
// 测试策略:
// - Happy Path: Run 以每种方式关闭（完成、失败、终止、取消、超时）时，已接受未完成的 Update 以
//   AcceptedUpdateCompletedWorkflow 失败结束，等待结果的调用方立即返回（假时钟不推进，软超时不会到期）
// - Edge Case: 关闭之后同一 updateID 的重试直接拿到失败结果；新的 Update 被拒绝；查询看到最终状态
// ============================================

func TestUpdateFailsWhenRunCloses(t *testing.T) {
	for _, kind := range []string{"completed", "failed", "terminated", "cancelled", "timed_out"} {
		t.Run(kind, func(t *testing.T) {
			mgr := memory.NewExecutionManager()
			srv, _ := newFakeClockServer(mgr, newTestTaskQueue(), newTestTaskQueue())
			t.Cleanup(srv.Stop)
			ref, err := srv.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "update-close-" + kind, Program: programtest.Noop()})
			if err != nil {
				t.Fatal(err)
			}
			run := &execution.Run{ID: "run-update-close-" + kind, Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowType, WorkflowType: ref.WorkflowType, Status: history.StatusRunning, CreatedAt: time.Now()}
			createStartedRunForTest(t, mgr, run)
			ctx := context.Background()

			type polled struct {
				result protocol.WorkflowUpdateResult
				err    error
			}
			done := make(chan polled, 1)
			go func() {
				result, err := srv.UpdateWorkflow(ctx, "", "", run.ID, "set_priority", "u-1", protocol.UpdateWaitStageEnums.Completed, nil)
				done <- polled{result, err}
			}()
			wftID := waitForAcceptedUpdate(t, mgr, run.ID, "u-1")

			switch kind {
			case "completed", "failed", "cancelled":
				command := map[string]history.Command{
					"completed": {Type: history.CmdCompleteWorkflow},
					"failed":    {Type: history.CmdFailWorkflow, Error: "boom", WorkflowFailure: &protocol.Failure{Message: "boom"}},
					"cancelled": {Type: history.CmdCancelWorkflow},
				}[kind]
				if err := srv.HandleWorkflowTaskCompleted(srv.WorkflowTaskContext(ctx, run.ID, wftID), run.ID, wftID, []history.Command{command}, ""); err != nil {
					t.Fatal(err)
				}
			case "terminated":
				if err := srv.TerminateWorkflow(ctx, "", "", run.ID, "stop"); err != nil {
					t.Fatal(err)
				}
			case "timed_out":
				srv.handleWorkflowTimeout(run.ID)
			}

			var got polled
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("long-polling caller was not woken by the run close")
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			assertClosedUpdateResult(t, got.result)

			// 关闭之后同一 updateID 的重试直接拿到同一个失败结果。
			retried, err := srv.UpdateWorkflow(ctx, "", "", run.ID, "set_priority", "u-1", protocol.UpdateWaitStageEnums.Completed, nil)
			if err != nil {
				t.Fatal(err)
			}
			assertClosedUpdateResult(t, retried)
			// 新的 Update 被拒绝，不写 History。
			if _, err := srv.UpdateWorkflow(ctx, "", "", run.ID, "set_priority", "u-2", protocol.UpdateWaitStageEnums.Accepted, nil); err == nil {
				t.Fatal("update accepted after the run closed")
			}
			// 查询看到最终状态。
			query, err := srv.QueryWorkflow(ctx, "", "", run.ID, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(query.Updates) != 1 || query.Updates[0].CompletedEventID == 0 || query.Updates[0].Outcome == nil ||
				query.Updates[0].Outcome.Failure == nil || query.Updates[0].Outcome.Failure.Type != protocol.UpdateFailureTypeWorkflowClosed {
				t.Fatalf("query updates = %+v, want one update failed by the close", query.Updates)
			}
		})
	}
}

// waitForAcceptedUpdate 等到 updateID 的 WorkflowUpdateAccepted 落库，返回之后调度的 WFT。
func waitForAcceptedUpdate(t *testing.T, mgr execution.ExecutionManager, runID, updateID string) int64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events, err := mgr.GetEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		accepted := false
		var wftID int64
		for _, event := range events {
			if event.WorkflowUpdateAccepted != nil && event.WorkflowUpdateAccepted.UpdateID == updateID {
				accepted = true
			}
			if event.WorkflowTaskScheduled != nil {
				wftID = event.ID
			}
		}
		if accepted && wftID != 0 {
			return wftID
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("update %s was not accepted", updateID)
	return 0
}

func assertClosedUpdateResult(t *testing.T, result protocol.WorkflowUpdateResult) {
	t.Helper()
	if result.Stage != protocol.UpdateReachedStageEnums.Completed || result.Outcome == nil || result.Outcome.Failure == nil {
		t.Fatalf("update result = %+v, want completed with a failure", result)
	}
	failure := result.Outcome.Failure
	if failure.Type != protocol.UpdateFailureTypeWorkflowClosed || !failure.NonRetryable {
		t.Fatalf("update failure = %+v, want non-retryable %s", failure, protocol.UpdateFailureTypeWorkflowClosed)
	}
}
