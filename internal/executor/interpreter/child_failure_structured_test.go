package interpreter

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 子工作流失败的结构化载荷：父侧 future.Get 拿到的错误与 Activity 失败同形，
// 因此 IsApplicationError / details 读取在父 run 里同样可用。
const childFailedBranchSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteChildWorkflow(ctx, "child-wf", input).Get(ctx, &out)
	if err != nil {
		if workflow.IsApplicationError(err, "ChildDeclined") {
			return workflow.NewApplicationErrorWithCause("child declined, parent aborted", "ParentAborted", err)
		}
		return workflow.NewApplicationError("unknown child outcome, manual review", "ManualReview")
	}
	return nil
}
`

// childFailedEvents 构造子工作流失败历史。失败只由 Failure 指针表达，Error 镜像其 message。
func childFailedEvents(failure protocol.Failure) []history.Event {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	child := &history.ChildWorkflowFailedEvent{
		ChildID: "#c1", ChildRunID: "run-child", ScheduledEventID: 2,
		Error: failure.Message, Failure: &failure,
	}
	return []history.Event{
		started,
		{ID: 2, Timestamp: time.Now(), ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{ChildID: "#c1", Name: "child-wf"}},
		{ID: 3, Timestamp: time.Now(), ChildWorkflowFailed: child},
		wftc(4),
	}
}

// typed 子失败：父侧走 typed 分支，终态 cause 保留子流程的 type/message/details。
func TestChildFailureKeepsStructuredPayload(t *testing.T) {
	failure := protocol.Failure{
		Message: "child declined", Type: "ChildDeclined", NonRetryable: true,
		Details: protocol.Payloads{{
			Encoding: protocol.PayloadEncodingJSONPlainV1,
			Data:     []byte(`{"reason":"insufficient_funds"}`),
		}},
	}
	cmds, err := probeExecFix(t, childFailedBranchSrc, childFailedEvents(failure))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "ParentAborted" {
		t.Fatalf("typed 子失败应走 ChildDeclined 分支, got %+v", fail.WorkflowFailure)
	}
	cause := fail.WorkflowFailure.Cause
	if cause == nil || cause.Type != "ChildDeclined" || cause.Message != "child declined" || !cause.NonRetryable {
		t.Fatalf("cause 丢失子流程结构: %+v", cause)
	}
	if len(cause.Details) != 1 || string(cause.Details[0].Data) != `{"reason":"insufficient_funds"}` {
		t.Fatalf("cause 丢失子流程 details: %+v", cause.Details)
	}
}

// 空 message 的 typed 子失败仍然是失败：不得因为 message 为空被判成成功。
func TestChildFailureTypedEmptyMessageStaysFailure(t *testing.T) {
	failure := protocol.Failure{Message: "", Type: "ChildDeclined"}
	cmds, err := probeExecFix(t, childFailedBranchSrc, childFailedEvents(failure))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "ParentAborted" {
		t.Fatalf("空 message 的 typed 子失败必须仍走失败分支, got %+v", fail.WorkflowFailure)
	}
	if fail.WorkflowFailure.Cause == nil || fail.WorkflowFailure.Cause.Type != "ChildDeclined" {
		t.Fatalf("cause 应保留 type: %+v", fail.WorkflowFailure.Cause)
	}
}

// live/replay 用同一份带结构化子失败的历史连跑两次，命令必须逐字段一致。
func TestChildFailureReplayDeterministic(t *testing.T) {
	failure := protocol.Failure{
		Message: "child declined", Type: "ChildDeclined",
		Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`1`)}},
	}
	events := childFailedEvents(failure)
	first, err := probeExecFix(t, childFailedBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute first: %v", err)
	}
	second, err := probeExecFix(t, childFailedBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute second: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("命令数量不一致: %d vs %d", len(first), len(second))
	}
	a, b := failCmdOf(t, first), failCmdOf(t, second)
	if a.WorkflowFailure == nil || b.WorkflowFailure == nil {
		t.Fatalf("两次执行都应产失败命令: %+v / %+v", a, b)
	}
	if a.WorkflowFailure.Type != b.WorkflowFailure.Type || a.Error != b.Error {
		t.Fatalf("replay 失败命令漂移: %+v vs %+v", a.WorkflowFailure, b.WorkflowFailure)
	}
	if a.WorkflowFailure.Cause == nil || b.WorkflowFailure.Cause == nil ||
		a.WorkflowFailure.Cause.Type != b.WorkflowFailure.Cause.Type {
		t.Fatalf("replay cause 漂移: %+v vs %+v", a.WorkflowFailure.Cause, b.WorkflowFailure.Cause)
	}
}
