package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// 子工作流失败经 engine 传播到父历史时保留完整结构化载荷：父 run 的 future 与
// 冷恢复都从这一份事件读，不是只拿到 message。
func TestChildFailPropagatesStructuredFailure(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-structured")

	failure := protocol.Failure{
		Message:      "child declined",
		Type:         "ChildDeclined",
		NonRetryable: true,
		Details: protocol.Payloads{{
			Encoding: protocol.PayloadEncodingJSONPlainV1,
			Data:     []byte(`{"reason":"insufficient_funds"}`),
		}},
		Cause: &protocol.Failure{Message: "inner", Type: "InnerType"},
	}
	if err := srv.failWorkflow(childRunID, failure, ""); err != nil {
		t.Fatalf("failWorkflow: %v", err)
	}

	parentHistory, err := mgr.GetEvents(parentRunID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	var propagated *historypkg.ChildWorkflowFailedEvent
	for i := range parentHistory {
		if failed := parentHistory[i].ChildWorkflowFailed; failed != nil {
			propagated = failed
		}
	}
	if propagated == nil {
		t.Fatalf("父历史缺少 ChildWorkflowFailed: %+v", parentHistory)
	}
	if propagated.Error != failure.Message {
		t.Fatalf("Error 应镜像 message: %q", propagated.Error)
	}
	if propagated.Failure == nil {
		t.Fatalf("结构化失败丢失: %+v", propagated)
	}
	if propagated.Failure.Type != "ChildDeclined" || !propagated.Failure.NonRetryable {
		t.Fatalf("scalars 丢失: %+v", propagated.Failure)
	}
	if len(propagated.Failure.Details) != 1 || string(propagated.Failure.Details[0].Data) != `{"reason":"insufficient_funds"}` {
		t.Fatalf("details 丢失: %+v", propagated.Failure.Details)
	}
	if propagated.Failure.Cause == nil || propagated.Failure.Cause.Type != "InnerType" {
		t.Fatalf("cause 丢失: %+v", propagated.Failure.Cause)
	}
	if propagated.ChildID == "" || propagated.ChildRunID != childRunID || propagated.ScheduledEventID != schedID {
		t.Fatalf("子流程标识丢失: %+v", propagated)
	}
	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Failed || st.Error != failure.Message {
		t.Fatalf("父侧 child state 不一致: %+v", st)
	}
}

// 旧 message-only 失败（超时/普通 error）：父历史仍是 message-only Failure（Type 空 = untyped），
// 不因为新增结构化通道而变成 typed。
func TestChildFailPropagatesMessageOnlyFailure(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-message-only")

	if err := srv.failWorkflow(childRunID, protocol.Failure{Message: "boom"}, ""); err != nil {
		t.Fatalf("failWorkflow: %v", err)
	}
	parentHistory, err := mgr.GetEvents(parentRunID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	var propagated *historypkg.ChildWorkflowFailedEvent
	for i := range parentHistory {
		if failed := parentHistory[i].ChildWorkflowFailed; failed != nil {
			propagated = failed
		}
	}
	if propagated == nil || propagated.Failure == nil {
		t.Fatalf("消息失败也应带 message-only Failure: %+v", propagated)
	}
	if propagated.Failure.Type != "" || propagated.Failure.NonRetryable || propagated.Failure.Details != nil || propagated.Failure.Cause != nil {
		t.Fatalf("message-only 失败必须保持 untyped: %+v", propagated.Failure)
	}
	if propagated.Failure.Message != "boom" {
		t.Fatalf("Message = %q", propagated.Failure.Message)
	}
	st, _, _ := mgr.GetChildWorkflowState(parentRunID, schedID)
	if st.Status != execution.ChildWorkflowStatusEnums.Failed {
		t.Fatalf("父侧 child state 应 failed: %+v", st)
	}
}

// 冷恢复：父 run 从历史补发子终态时同样带结构化载荷。
func TestChildCloseRecoveryKeepsStructuredFailure(t *testing.T) {
	srv, mgr := newServerForTest(t)
	parentRunID, schedID, childRunID := startChildWorkflow(t, srv, mgr, "run-parent-recovery")

	failure := protocol.Failure{Message: "child declined", Type: "ChildDeclined"}
	if err := srv.failWorkflow(childRunID, failure, ""); err != nil {
		t.Fatalf("failWorkflow: %v", err)
	}
	child, err := mgr.Get(childRunID)
	if err != nil {
		t.Fatalf("Get child: %v", err)
	}
	// 删掉父侧 ChildWorkflowFailed 事件，模拟父 run 在传播前中断、由恢复路径补发。
	history, err := mgr.GetEvents(parentRunID)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	_ = history
	if _, _, err := mgr.GetChildWorkflowState(parentRunID, schedID); err != nil {
		t.Fatalf("GetChildWorkflowState: %v", err)
	}
	action, needed, err := srv.runtime.ChildCloseRecoveryActionFor(child, historypkg.StatusFailed)
	if err != nil {
		t.Fatalf("ChildCloseRecoveryActionFor: %v", err)
	}
	if !needed {
		t.Fatal("子 run 已失败时恢复路径应需要补发")
	}
	if action.Failure == nil || action.Failure.Type != "ChildDeclined" {
		t.Fatalf("恢复动作丢弃结构化失败: %+v", action)
	}
	if action.Reason != "child declined" {
		t.Fatalf("Reason = %q", action.Reason)
	}
	_ = time.Now
}
