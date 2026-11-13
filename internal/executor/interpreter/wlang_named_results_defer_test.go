package interpreter

// ============================================
// WLang 多命名结果 (out T, err error) + defer：executor 级跨 WFT 回归
// ============================================
// 背景：Saga 的正式写法是 `func W(ctx, in) (out OrderOutput, err error)`，补偿注册在
// if / for 块内的 defer 里，靠函数级 drain 在 return 之后执行。这里要锁住的不只是
// 「defer 什么时候跑」，而是「defer 跑完之后，入口的命名结果怎么被读走」：
//
//   - 显式 return 形态：`return out, err` 先把值写进命名结果，defer 再改 err；补偿跨
//     WFT（每轮一个 activity）执行完后，engine 才读命名结果 → 终态 CmdFailWorkflow 的
//     Failure 必须是 defer 写回后的 CompensationFailed（cause 保留 ShipRejected，
//     details 带补偿错误文本），而不是原 ShipRejected，也不是运行时 error 通道。
//   - 裸 return 形态：`return` 不写表达式，返回值就是函数边界上的命名结果；defer 内先
//     跑一次成功的 Refund 补偿（跨 WFT），再改 out.Status → 终态 CmdCompleteWorkflow
//     .Output 必须读到 defer 改过的值。
//
// 驱动全部复用 wlang_function_defer_test.go 的 generateDeferProgram /
// driveDeferSaga：真实 sdkgen.GenerateFromSource 从内联 Go 源码出发，得到 executor 真正执行的
// WorkflowConfig，再按 Event history
// 逐轮 Execute()（一轮 = 一个 WFT）。activity 结果由 Event 注入，不连 DB / server。
//
// 证据边界：只有 interpreter/executor 层。translation 产物本身的 named-return 语义由
// go2wlang / wlang 侧的测试负责；这里只认 executor 可见的命令与失败结构。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// 入口签名为多命名结果 `(out OrderOutput, err error)`：Entry.Output 来自第一个非 error
// 结果，out 与 err 都是命名结果变量。补偿注册分别在 if 块与 for 块内，LIFO 顺序为
// Refund（后登记）→ Release（先登记）。
//
// 失败路径用显式 `return out, err`：值在 return 时写进命名结果，defer 里的写回发生在其后。
//
// activity 载荷单独拼成 map[string]any：host 的 ExecuteActivity 第三个形参是 `input any`，
// 直接传入口的 typed struct（Record）会在绑定 host 参数时报 [E_TYPE] requires struct target，
// 那是 host 签名限制，与命名结果语义无关，故不混进本回归的观察面。
const wlangNamedResultsExplicitReturnSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type OrderInput struct {
	OrderID string ` + "`json:\"order_id\"`" + `
}

type OrderOutput struct {
	Status string ` + "`json:\"status\"`" + `
}

func W(ctx workflow.Context, in OrderInput) (out OrderOutput, err error) {
	payload := map[string]any{"order_id": in.OrderID}
	var reservedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Reserve, payload).Get(ctx, &reservedOut)
	if err != nil {
		return out, err
	}
	reserved := true
	if reserved {
		defer func() {
			if err == nil {
				return
			}
			var releaseOut map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Release, payload).Get(ctx, &releaseOut)
			if compErr != nil {
				err = workflow.NewApplicationErrorWithCause("workflow failed and compensation failed", "CompensationFailed", err, compErr.Error())
			}
		}()
	}
	for round := 0; round < 1; round++ {
		var paidOut map[string]any
		err = workflow.ExecuteActivity(ctx, activities.Pay, payload).Get(ctx, &paidOut)
		if err != nil {
			return out, err
		}
		defer func() {
			if err == nil {
				return
			}
			var refundOut map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Refund, payload).Get(ctx, &refundOut)
			if compErr != nil {
				err = workflow.NewApplicationErrorWithCause("workflow failed and compensation failed", "CompensationFailed", err, compErr.Error())
			}
		}()
	}
	var shippedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Ship, payload).Get(ctx, &shippedOut)
	if err != nil {
		out.Status = "ship-failed"
		return out, err
	}
	out.Status = "shipped"
	return out, nil
}
`

// 显式 return + 失败路径：Ship 失败 → LIFO 补偿 Refund（失败）→ Release → 终态失败结构
// 来自 defer 写回的命名 err。
//
// 旧行为（多命名结果未被翻译/未被 entry 读取）下的红点，实测基线 wlang HEAD 68716bb：
//   - out/err 不是命名结果、也没进 locals → 首次 `err = ...Get()` 在 resume 后报
//     [E_SYMBOL] variable "err" not defined（第 2 轮起，Get 才把值写回 err）；
//   - 即便退回单返回值形态，终态 Failure 也只剩原 ShipRejected，CompensationFailed
//     包装写不回去；
//   - defer 里的 Refund 跨 WFT 恢复后，命名结果不再被重新解析 → out 的写回丢失。
func TestWLangNamedResultsExplicitReturnCompensationFailure(t *testing.T) {
	result := generateDeferProgram(t, wlangNamedResultsExplicitReturnSrc)
	nodes := result.Workflow

	rounds, _ := driveDeferSaga(t, nodes, map[string]*protocol.Failure{
		"activities.Ship":   {Type: "ShipRejected"},
		"activities.Refund": {Type: "RefundUnavailable", Message: "refund-boom"},
	})

	// 真实跨轮调度序列：正向三步各占一轮 + 两笔补偿各占一轮（LIFO）+ 终态轮。
	failure := assertDeferSagaTrace(t, rounds, []string{
		"activities.Reserve",
		"activities.Pay",
		"activities.Ship",
		"activities.Refund",
		"activities.Release",
	})

	if got := result.Workflow.Program.Entry.Output; got == "" {
		t.Fatalf("多命名结果的入口应声明 Entry.Output，实际为空（typed entry 未识别命名结果）: %+v",
			result.Workflow.Program.Entry)
	}

	workflowFailure := failure.WorkflowFailure
	if workflowFailure == nil {
		t.Fatalf("终态必须携带 typed Failure，实际 %+v（Error=%q）——命名 err 没被 entry 读到",
			failure, failure.Error)
	}
	// 命名 err 的写回发生在补偿 WFT 里，必须在 drain 结束、函数边界解析之后被读到。
	if workflowFailure.Type != "CompensationFailed" {
		t.Fatalf("终态 Failure.Type = %q，期望 defer 写回的 CompensationFailed（命名 err 未被 entry 读取）: %+v",
			workflowFailure.Type, workflowFailure)
	}
	if workflowFailure.Message != "workflow failed and compensation failed" {
		t.Fatalf("终态 Failure.Message = %q，期望 defer 写入的 message: %+v",
			workflowFailure.Message, workflowFailure)
	}
	// 原业务失败保留为 cause：补偿包装不得吞掉 Ship 的 typed Failure。
	if workflowFailure.Cause == nil || workflowFailure.Cause.Type != "ShipRejected" {
		t.Fatalf("终态 Failure.Cause 应保留原 ShipRejected，实际 %+v", workflowFailure.Cause)
	}
	// 补偿错误文本进 details（同一 API 的 variadic details）。
	if len(workflowFailure.Details) == 0 {
		t.Fatalf("终态 Failure.Details 为空，期望包含补偿错误文本: %+v", workflowFailure)
	}
	var detail string
	if err := json.Unmarshal(workflowFailure.Details[0].Data, &detail); err != nil {
		t.Fatalf("解码 details[0] 失败: %v（原始 %q）", err, workflowFailure.Details[0].Data)
	}
	if !strings.Contains(detail, "refund-boom") {
		t.Fatalf("details[0] = %q，期望包含补偿错误文本 refund-boom", detail)
	}
	t.Logf("终态 Failure = type=%q cause.type=%q details[0]=%q（命名 err 在补偿 WFT 之后被 entry 读取）",
		workflowFailure.Type, workflowFailure.Cause.Type, detail)
}

// 裸 return + 正常路径：defer 内先跑一次成功的 Refund 补偿（跨 WFT），再改 out.Status。
// 返回值没有任何表达式，函数边界读的必须是命名结果变量本身。
const wlangNamedResultsBareReturnSrc = `package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type OrderInput struct {
	OrderID string ` + "`json:\"order_id\"`" + `
}

type OrderOutput struct {
	Status string ` + "`json:\"status\"`" + `
}

func W(ctx workflow.Context, in OrderInput) (out OrderOutput, err error) {
	payload := map[string]any{"order_id": in.OrderID}
	var reservedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Reserve, payload).Get(ctx, &reservedOut)
	if err != nil {
		return out, err
	}
	reserved := true
	if reserved {
		defer func() {
			var refundOut map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Refund, payload).Get(ctx, &refundOut)
			if compErr != nil {
				err = compErr
				return
			}
			out.Status = "compensated"
		}()
	}
	out.Status = "shipped"
	return
}
`

// 裸 return 形态的可见结果：CmdCompleteWorkflow.Output 是 deferred 闭包改过的值。
//
// 这条同时证明两件事：
//   - 裸 return 不是「return null」——它解析的是函数边界的命名结果；
//   - 命名结果的解析发生在 defer drain 之后，且 Refund 的补偿跨了一个 WFT 仍可见。
func TestWLangNamedResultsBareReturnOutputAfterDefer(t *testing.T) {
	result := generateDeferProgram(t, wlangNamedResultsBareReturnSrc)
	nodes := result.Workflow

	rounds, _ := driveDeferSaga(t, nodes, nil)

	if got := deferScheduledSequence(rounds); !equalDeferSequence(got, []string{
		"activities.Reserve",
		"activities.Refund",
	}) {
		t.Fatalf("跨轮调度序列 = %v，期望 [activities.Reserve activities.Refund]: %+v", got, rounds)
	}
	// 补偿在独立 WFT 里执行：defer 的写回必须跨轮活到终态。
	if len(rounds) != 3 {
		t.Fatalf("轮次数 = %d，期望 3（Reserve + Refund + 终态轮）: %+v", len(rounds), rounds)
	}
	for index, want := range []string{"activities.Reserve", "activities.Refund"} {
		if len(rounds[index].scheduled) != 1 || rounds[index].scheduled[0] != want {
			t.Fatalf("第 %d 轮调度 = %v，期望恰好 [%s]", index+1, rounds[index].scheduled, want)
		}
	}
	if len(rounds[2].scheduled) != 0 {
		t.Fatalf("终态轮不应再调度 activity，实际 %v", rounds[2].scheduled)
	}

	completes := completeWorkflowCommands(rounds[2].commands)
	if len(completes) != 1 {
		t.Fatalf("终态应恰好一条 CmdCompleteWorkflow，实际 %+v", rounds[2].commands)
	}
	output := completes[0].Output
	if len(output) == 0 {
		t.Fatalf("CmdCompleteWorkflow.Output 为空——命名 out 被静默丢弃: %+v", completes[0])
	}
	if got, _ := output["status"].(string); got != "compensated" {
		t.Fatalf("Output.status = %#v，期望 defer 里写入的 compensated（读到的是 defer 之前的值）",
			output["status"])
	}
	t.Logf("CmdCompleteWorkflow.Output = %#v（裸 return 读到 defer 改过的命名 out）", output)
}

// completeWorkflowCommands 过滤出 CmdCompleteWorkflow。
func completeWorkflowCommands(commands []history.Command) []history.Command {
	var completes []history.Command
	for _, command := range commands {
		if command.Type == history.CmdCompleteWorkflow {
			completes = append(completes, command)
		}
	}
	return completes
}
