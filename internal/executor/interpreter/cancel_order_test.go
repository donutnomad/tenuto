package interpreter

// 同一个决策批里取消请求与 Activity 结果按事件顺序生效（对标 Temporal 按序处理事件）：取消请求在前时 Activity 按
// 取消处理，不等待取消的 Future 以取消错误返回，之后的完成不改变它；完成在前时 Future 得到结果。
// Workflow Task 进行期间到达的取消请求不让非终态提交作废，它与之后的 Activity 完成常落在同一批。

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
)

const cancelOrderSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (string, error) {
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out); err != nil {
		return "", err
	}
	return "done", nil
}
`

// cancelOrderHistory 返回第一个 Workflow Task 调度 A 之后、第二个 Workflow Task started 的 History，第二批里同时有
// 取消请求与 A 的完成。cancelFirst 时取消请求在第一个 Workflow Task 进行期间到达（Worker 没有看到，ID 大于
// SeenThroughEventID），排在 A 的完成之前；否则 A 先完成，取消请求随后到达。
func cancelOrderHistory(cancelFirst bool) []history.Event {
	h := &fanOutHistory{}
	h.add(history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0))
	wft := h.startTask()
	seen := h.last()
	if cancelFirst {
		h.add(history.NewWorkflowCancellationRequestedEvent("test"))
	}
	h.commit(wft, seen, []history.Command{{Type: history.CmdScheduleActivity, ActivityID: "#a1", ActivityType: "activities.A"}})
	h.complete("#a1")
	if !cancelFirst {
		h.add(history.NewWorkflowCancellationRequestedEvent("test"))
	}
	h.startTask()
	return h.events
}

func TestCancellationBeforeActivityResultInOneBatchCancelsTheActivity(t *testing.T) {
	nodes := compileSource(t, cancelOrderSrc)
	commands, err := NewWorkflowExecutor(nodes, cancelOrderHistory(true)).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || commands[0].Type != history.CmdRequestCancelActivity || commands[1].Type != history.CmdCancelWorkflow {
		t.Fatalf("commands = %+v, want RequestCancelActivity then CancelWorkflow", commands)
	}
}

func TestActivityResultBeforeCancellationInOneBatchWins(t *testing.T) {
	nodes := compileSource(t, cancelOrderSrc)
	commands, err := NewWorkflowExecutor(nodes, cancelOrderHistory(false)).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].Type != history.CmdCompleteWorkflow {
		t.Fatalf("commands = %+v, want CompleteWorkflow", commands)
	}
}
