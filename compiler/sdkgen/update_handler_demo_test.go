package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

// programText 取编译产物的完整 WLang 程序文本，
// 与 control_flow_test.go 取产物的方式一致。
func programText(t *testing.T, result Result) string {
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatalf("marshal program: %v", err)
	}
	return string(raw)
}

// 本文件是「Update Handler 用法演示」——把用户会写的 workflow 源码喂给 sdkgen，
// 验证它们真能编译成 WLang 程序（即"这样写是合法的、能跑的"）。
// 运行：GOWORK=off go test ./sdkgen/ -run UpdateHandlerDemo -v

// 演示 1：update handler + 闭包累加 counter（对标 Temporal SetUpdateHandler）。
//
// 用户写法：注册一个回调，外部每发一次 "add" update，回调把参数 n 累加到闭包
// 捕获的 counter 上并返回当前总和。handler 签名 func(ctx, n int64) (int64, error)。
func TestUpdateHandlerDemo_CounterWithClosureAccumulation(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func CounterWorkflow(ctx workflow.Context, input map[string]any) error {
	counter := int64(0)

	// 注册 handler：闭包按引用捕获 counter，跨多次 update 累加。
	if err := workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, n int64) (int64, error) {
		counter = counter + n
		return counter, nil
	}); err != nil {
		return err
	}

	// 主流程等一个 stop signal 后结束；update 在主流程阻塞期间也会被处理。
	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "counter", FuncName: "CounterWorkflow", Filename: "counter.go"})
	if err != nil {
		t.Fatalf("counter workflow 应能编译，却报错: %v", err)
	}
	program := programText(t, result)
	if !strings.Contains(program, "SetUpdateHandler") {
		t.Errorf("编译产物应含 SetUpdateHandler host 调用，got: %s", program)
	}
	if !strings.Contains(program, `"returns":["int64","error"]`) {
		t.Errorf("handler 的 (int64, error) 返回应保留在产物里，got: %s", program)
	}
	t.Logf("✅ CounterWorkflow 编译通过，含 SetUpdateHandler（handler 由 engine 协程执行）")
}

// 演示 2：handler 内业务级拒绝——余额不足时返回 error，
// 外部经 stage=Completed 拿到 Outcome.Failure（对标 Temporal handler 返 error）。
func TestUpdateHandlerDemo_HandlerBusinessReject(t *testing.T) {
	src := []byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func WithdrawWorkflow(ctx workflow.Context, input map[string]any) error {
	balance := int64(100)

	if err := workflow.SetUpdateHandler(ctx, "withdraw", func(ctx workflow.Context, amount int64) (int64, error) {
		if amount > balance {
			return 0, workflow.NewApplicationError("insufficient balance", "InsufficientBalance")
		}
		balance = balance - amount
		return balance, nil
	}); err != nil {
		return err
	}

	var stop map[string]any
	err := workflow.WaitSignal(ctx, "stop").Get(ctx, &stop)
	if err != nil {
		return err
	}
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "withdraw", FuncName: "WithdrawWorkflow", Filename: "withdraw.go"})
	if err != nil {
		t.Fatalf("withdraw workflow 应能编译，却报错: %v", err)
	}
	if !strings.Contains(programText(t, result), "NewApplicationError") {
		t.Errorf("handler 内的 error 返回应保留在产物里，got: %s", programText(t, result))
	}
	t.Logf("✅ WithdrawWorkflow 编译通过（handler 返回 error 表达业务拒绝）")
}

// 演示 3：非结构化并发 wg.Go + WaitGroup join——并发调多个 activity，
// 用 WaitGroup 显式 join（对标 Temporal workflow.Go = NewCoroutine，起了即返回不 join）。
//
// ⚠️ lane 内忽略 error 要写成 `e := ...Get(...); _ = e`（先赋值再丢弃），
// 不能写 `_ = workflow.ExecuteActivity(...).Get(...)`——那会被 sdkgen 拒绝
// （副作用调用不能用 `_ =` 直接丢弃，防静默丢失）。
func TestUpdateHandlerDemo_StructuredConcurrency(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func FanOutWorkflow(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var out map[string]any
		e := workflow.ExecuteActivity(ctx, activities.FetchProfile, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		_ = e
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var out map[string]any
		e := workflow.ExecuteActivity(ctx, activities.FetchOrders, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
		_ = e
	})
	wg.Wait(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "fanout", FuncName: "FanOutWorkflow", Filename: "fanout.go"})
	if err != nil {
		t.Fatalf("fanout workflow 应能编译，却报错: %v", err)
	}
	if !strings.Contains(programText(t, result), `"Go"`) {
		t.Errorf("编译产物应含 wg.Go 的 host 调用 {\"Go\":[...]}，got: %s", programText(t, result))
	}
	t.Logf("✅ FanOutWorkflow 编译通过，含非结构化并发 host 调用")
}

// 演示：lane/handler 内用 `_ = <副作用调用>` 会被 go2wlang 翻译成执行（{"expr":...}），
// 而非静默丢弃——副作用调用照常执行、只丢弃返回值。所以 `_ = Execute(...).Get(...)`
// 是合法写法（activity 真被调度执行，只是忽略返回的 error）。
func TestUpdateHandlerDemo_BlankAssignCallIsTranslated(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func FireWorkflow(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	// _ = 副作用调用：activity 照常执行，只丢弃返回值。
	_ = workflow.ExecuteActivity(ctx, activities.Do, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "fire", FuncName: "FireWorkflow", Filename: "fire.go"})
	if err != nil {
		t.Fatalf("`_ = <副作用调用>` 应能翻译执行，却报错: %v", err)
	}
	// 翻译产物含 activity 调用（Do 被真正调度），证明调用没被静默丢弃。
	if !strings.Contains(programText(t, result), "activities.Do") {
		t.Errorf("编译产物应含 activities.Do 调用（`_ =` 不静默丢弃副作用调用），got: %s", programText(t, result))
	}
	t.Logf("✅ `_ = <副作用调用>` 被翻译执行，activity 照常调度")
}
