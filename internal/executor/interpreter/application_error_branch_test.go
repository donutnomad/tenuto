package interpreter

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// 应用错误类型分支：body 用自然的 `if workflow.IsApplicationError(err, "T")` 分支，
// 失败来自历史里带结构化 Failure 的 ActivityFailed 事件。live 与 replay 用同一份历史，
// 两次执行必须产出一致的命令（同一分支、同一错误）。
const appErrorBranchSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
	if err != nil {
		if workflow.IsApplicationError(err, "PaymentDeclined") {
			return workflow.NewApplicationError("declined, inventory released", "PaymentDeclined")
		} else {
			return workflow.NewApplicationErrorWithCause("unknown outcome, manual review", "ManualReview", err)
		}
	}
	return nil
}
`

// 取反形态：`!workflow.IsApplicationError(...)` 同样必须走对分支。
const appErrorNegatedSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
	if err != nil {
		if !workflow.IsApplicationError(err, "PaymentDeclined") {
			return workflow.NewApplicationError("unknown outcome, manual review", "ManualReview")
		}
		return workflow.NewApplicationError("declined, inventory released", "PaymentDeclined")
	}
	return nil
}
`

// activityFailedWithFailure 构造带结构化失败的历史（调度 + 终态失败 + 批界）。
func activityFailedWithFailure(failure protocol.Failure) []history.Event {
	started := history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)
	sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{
		ActivityID: "#a1", ActivityType: "activities.A",
	}}
	failed := history.Event{ID: 3, Timestamp: time.Now(), ActivityFailed: &history.ActivityFailedEvent{
		ScheduledEventID: 2, Error: failure.Message, Failure: &failure,
	}}
	return []history.Event{started, sched, failed, wftc(4)}
}

func failCmdOf(t *testing.T, cmds []history.Command) *history.Command {
	t.Helper()
	for i := range cmds {
		if cmds[i].Type == history.CmdFailWorkflow {
			return &cmds[i]
		}
	}
	t.Fatalf("缺少 CmdFailWorkflow: %+v", cmds)
	return nil
}

// 命中 typed failure：走 PaymentDeclined 分支，终态结构化失败 type 保持。
func TestApplicationErrorBranchTypedFailure(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{
		Message: "", Type: "PaymentDeclined", NonRetryable: true,
	})
	cmds, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "PaymentDeclined" {
		t.Fatalf("应走 PaymentDeclined 分支, got %+v", fail.WorkflowFailure)
	}
	if fail.Error != "declined, inventory released" {
		t.Fatalf("Error = %q", fail.Error)
	}
	if fail.WorkflowFailure.Cause != nil {
		t.Fatalf("该分支不应带 cause: %+v", fail.WorkflowFailure)
	}
}

// 非命中：走 else 分支，Activity 的失败作为 cause 保留（含 type）。
func TestApplicationErrorBranchUntyped(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{Message: "boom", Type: "PaymentUnavailable"})
	cmds, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "ManualReview" {
		t.Fatalf("应走 ManualReview 分支, got %+v", fail.WorkflowFailure)
	}
	if fail.WorkflowFailure.Cause == nil || fail.WorkflowFailure.Cause.Type != "PaymentUnavailable" {
		t.Fatalf("cause 应保留 Activity 的 type: %+v", fail.WorkflowFailure.Cause)
	}
}

// untyped 失败（旧 worker / 普通 error）：IsApplicationError 恒 false，走 else 分支。
func TestApplicationErrorBranchLegacyUntyped(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{Message: "boom"})
	cmds, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "ManualReview" {
		t.Fatalf("untyped 失败应走 else 分支, got %+v", fail.WorkflowFailure)
	}
	if fail.WorkflowFailure.Cause == nil || fail.WorkflowFailure.Cause.Type != "" {
		t.Fatalf("untyped cause 的 type 应为空: %+v", fail.WorkflowFailure.Cause)
	}
}

// 取反形态：typed 命中时走 return 的 PaymentDeclined 分支，untyped 走 ManualReview。
func TestApplicationErrorNegatedForm(t *testing.T) {
	typed := activityFailedWithFailure(protocol.Failure{Message: "", Type: "PaymentDeclined"})
	cmds, err := probeExecFix(t, appErrorNegatedSrc, typed)
	if err != nil {
		t.Fatalf("Execute typed: %v", err)
	}
	if fail := failCmdOf(t, cmds); fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "PaymentDeclined" {
		t.Fatalf("取反形态 typed 分支错误: %+v", fail.WorkflowFailure)
	}

	untyped := activityFailedWithFailure(protocol.Failure{Message: "boom"})
	cmds, err = probeExecFix(t, appErrorNegatedSrc, untyped)
	if err != nil {
		t.Fatalf("Execute untyped: %v", err)
	}
	if fail := failCmdOf(t, cmds); fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "ManualReview" {
		t.Fatalf("取反形态 untyped 分支错误: %+v", fail.WorkflowFailure)
	}
}

// replay 确定性：同一份历史执行两次，产出的失败命令逐字段一致（分支、type、message、cause）。
func TestApplicationErrorBranchReplayDeterministic(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{
		Message: "declined", Type: "PaymentDeclined",
		Details: protocol.Payloads{mustJSONPayload(t, "card-1")},
	})
	first, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	second, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	a, b := failCmdOf(t, first), failCmdOf(t, second)
	if a.Error != b.Error || a.WorkflowFailure.Type != b.WorkflowFailure.Type {
		t.Fatalf("replay 不一致: %+v vs %+v", a.WorkflowFailure, b.WorkflowFailure)
	}
}

// ApplicationErrorDetail 用 typed out 参数读取 Activity 写入的 struct detail：
// 源码是合法 Go（detail.Reason 可编译），body-local DTO 由 go2wlang 收集 descriptor，
// 宿主按 payload 解码并由 registry 写回 record。
func TestApplicationErrorDetailReadable(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{
		Message: "declined", Type: "PaymentDeclined",
		Details: protocol.Payloads{mustJSONPayload(t, map[string]any{"reason": "insufficient_funds", "card": "card-1"})},
	})
	cmds, err := probeExecFix(t, appErrorDetailSrc, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fail := failCmdOf(t, cmds); fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "DetailsReadable" {
		t.Fatalf("结构化 detail 未正确读取: %+v", fail.WorkflowFailure)
	}
}

// 明确的失败语义：越界/无 details 返回 ErrNoDetails，out 不是指针返回 ErrDetailTarget。
func TestApplicationErrorDetailErrors(t *testing.T) {
	api := &runtimeWorkflowAPI{}
	withDetails := apperror.FromFailure(protocol.Failure{
		Message: "m", Type: "T",
		Details: protocol.Payloads{mustJSONPayload(t, "only")},
	}, nil)
	withoutDetails := apperror.FromFailure(protocol.Failure{Message: "m"}, nil)

	if err := api.ApplicationErrorDetail(withoutDetails, 0, new(map[string]any)); !errors.Is(err, apperror.ErrNoDetails) {
		t.Fatalf("无 details 应返回 ErrNoDetails, got %v", err)
	}
	if err := api.ApplicationErrorDetail(withDetails, 3, new(map[string]any)); !errors.Is(err, apperror.ErrNoDetails) {
		t.Fatalf("越界应返回 ErrNoDetails, got %v", err)
	}
	var nilMap map[string]any
	if err := api.ApplicationErrorDetail(withDetails, 0, nilMap); !errors.Is(err, apperror.ErrDetailTarget) {
		t.Fatalf("nil out 应返回 ErrDetailTarget, got %v", err)
	}
	if err := api.ApplicationErrorDetail(withDetails, 0, "not-pointer"); !errors.Is(err, apperror.ErrDetailTarget) {
		t.Fatalf("非指针 out 应返回 ErrDetailTarget, got %v", err)
	}
	if got := api.ApplicationErrorDetailCount(withoutDetails); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
	if got := api.ApplicationErrorDetailCount(withDetails); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

// 空 message 的 typed failure 必须仍然走失败分支（不以 Message 空串判失败）。
func TestApplicationErrorEmptyMessageTypedFailure(t *testing.T) {
	events := activityFailedWithFailure(protocol.Failure{Message: "", Type: "PaymentDeclined", NonRetryable: true})
	cmds, err := probeExecFix(t, appErrorBranchSrc, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	fail := failCmdOf(t, cmds)
	if fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "PaymentDeclined" {
		t.Fatalf("空 message typed failure 走了错误分支: %+v", fail.WorkflowFailure)
	}
}

// mustJSONPayload 把值编码成 json/plain-v1 载荷，供构造结构化失败用。
func mustJSONPayload(t *testing.T, value any) protocol.Payload {
	t.Helper()
	payload, err := protocol.NewJSONPayload(value)
	if err != nil {
		t.Fatalf("NewJSONPayload(%v): %v", value, err)
	}
	return payload
}

// appErrorDetailSrc 用 typed out 参数读取第一个 detail 的 struct 字段。
const appErrorDetailSrc = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
type ChargeFailure struct {
	Reason string ` + "`json:\"reason\"`" + `
	Card   string ` + "`json:\"card\"`" + `
}
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
	if err != nil {
		count := workflow.ApplicationErrorDetailCount(err)
		if count == 0 {
			return workflow.NewApplicationError("no details", "DetailsMissing")
		}
		var detail ChargeFailure
		detailsErr := workflow.ApplicationErrorDetail(err, 0, &detail)
		if detailsErr != nil {
			return workflow.NewApplicationError("details decode failed", "DetailsDecodeError", detailsErr)
		}
		if detail.Reason != "insufficient_funds" || detail.Card != "card-1" {
			return workflow.NewApplicationError("details mismatch", "DetailsMismatch")
		}
		return workflow.NewApplicationError("details readable", "DetailsReadable")
	}
	return nil
}
`

// 解码失败时 out 必须保持原值（先解码到临时目标，成功后才写回）。
func TestApplicationErrorDetailKeepsOutOnDecodeFailure(t *testing.T) {
	api := &runtimeWorkflowAPI{}
	withBadDetail := apperror.FromFailure(protocol.Failure{
		Message: "m", Type: "T",
		Details: protocol.Payloads{mustJSONPayload(t, map[string]any{"reason": 123})},
	}, nil)

	out := map[string]string{"card": "keep"}
	err := api.ApplicationErrorDetail(withBadDetail, 0, &out)
	if err == nil {
		t.Fatal("类型不匹配应返回解码错误")
	}
	if out["card"] != "keep" || len(out) != 1 {
		t.Fatalf("解码失败改写了 out: %+v", out)
	}
}

// 解码失败必须明确报错，不能被吞成「没有 details」。
func TestApplicationErrorDetailDecodeError(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
	if err != nil {
		var detail map[string]any
		detailsErr := workflow.ApplicationErrorDetail(err, 0, &detail)
		if detailsErr != nil {
			return workflow.NewApplicationError("details decode failed", "DetailsDecodeError", detailsErr)
		}
		return workflow.NewApplicationError("no decode error", "DetailsUnexpectedSuccess")
	}
	return nil
}
`
	events := activityFailedWithFailure(protocol.Failure{
		Message: "declined", Type: "PaymentDeclined",
		Details: protocol.Payloads{mustJSONPayload(t, "not-an-object")},
	})
	cmds, err := probeExecFix(t, src, events)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fail := failCmdOf(t, cmds); fail.WorkflowFailure == nil || fail.WorkflowFailure.Type != "DetailsDecodeError" {
		t.Fatalf("解码失败应明确返回错误: %+v", fail.WorkflowFailure)
	}
}
