package sdkgen

import (
	"reflect"
	"sort"
	"testing"

	programpkg "github.com/donutnomad/tenuto/program"
)

// ============================================
// 生成器产出的 Program 上的载荷位置识别（program.PayloadTypes）
// ============================================
// 测试策略:
// - Happy Path: Selector 回调里 Get 的 future（AddSignalFuture / AddFuture / AddChildFuture）按加入的 future 追溯到
//   WaitSignal / ExecuteActivity / ExecuteChildWorkflow；AddReceive 回调的 Receive 不是 History 载荷
// - Happy Path: 名字取常量折叠：Go 常量、只赋值一次的局部变量、`+` 拼接、常量实参的 fmt.Sprintf、每个调用处实参都
//   相同的同包函数形参
// - Happy Path: 没有显式声明类型的变量：同包函数返回值（含多返回值）、另一个变量、字段访问、入口参数；future 切片
//   元素与同包函数的 future 形参；workflow.Go 闭包；ApplicationErrorDetail 的 detail
// - Edge Case: 不同闭包里的同名变量互不影响（回调形参 `a any` 不会把 any 带进外层 `a` 的位置）
// - Error Case: 名字来自输入字段、同包函数形参在不同调用处取不同值时，记为未确定的解码处，带种类、类型、路径与原因
// ============================================

const payloadFormsSrc = `package order

import (
	"context"
	"fmt"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Approval struct {
	OrderID string ` + "`json:\"order_id\"`" + `
}

type Out struct {
	ID string ` + "`json:\"id\"`" + `
}

type Detail struct {
	Code string ` + "`json:\"code\"`" + `
}

const approveSignal = "approve"

// @Activity(type="forms.charge",queue="forms",version=v1)
func Charge(ctx context.Context, in Approval) (Out, error) { return Out{}, nil }

func makeApproval() Approval { return Approval{} }

func makePair() (Approval, error) { return Approval{}, nil }

func waitFor(ctx workflow.Context, f workflow.SignalFuture) error {
	var viaHelper Approval
	return f.Get(ctx, &viaHelper)
}

func waitNamed(ctx workflow.Context, name string) error {
	var named Out
	return workflow.WaitSignal(ctx, name).Get(ctx, &named)
}

// @Workflow(type="forms")
func Order(ctx workflow.Context, in Approval) (Out, error) {
	var a Approval
	var o Out
	var kid Out
	sig := workflow.WaitSignal(ctx, approveSignal)
	act := workflow.ExecuteActivity(ctx, Charge, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	child := workflow.ExecuteChildWorkflow(ctx, "kid", in)
	ch := workflow.NewChannel(ctx)
	s := workflow.NewSelector(ctx)
	s.AddSignalFuture(ctx, sig, func(f workflow.SignalFuture) { _ = f.Get(ctx, &a) })
	s.AddFuture(ctx, act, func(f workflow.ActivityFuture) { _ = f.Get(ctx, &o) })
	s.AddChildFuture(ctx, child, func(f workflow.ChildWorkflowFuture) { _ = f.Get(ctx, &kid) })
	s.AddReceive(ctx, ch, func(c workflow.Channel, more bool) { c.Receive(ctx, &a) })
	s.Select(ctx)

	name := "later"
	b := makeApproval()
	_ = workflow.WaitSignal(ctx, name).Get(ctx, &b)
	pair, err := makePair()
	_ = workflow.WaitSignal(ctx, "pre-"+name).Get(ctx, &pair)
	copied := in
	_ = workflow.WaitSignal(ctx, fmt.Sprintf("%s-%d", "copy", 3)).Get(ctx, &copied)
	id := in.OrderID
	_ = workflow.WaitSignal(ctx, "field").Get(ctx, &id)
	futures := []workflow.SignalFuture{workflow.WaitSignal(ctx, "element")}
	var element Approval
	_ = futures[0].Get(ctx, &element)
	_ = waitFor(ctx, workflow.WaitSignal(ctx, "helper"))
	_ = waitNamed(ctx, "one")
	_ = waitNamed(ctx, "two")
	var detail Detail
	_ = workflow.ApplicationErrorDetail(err, 0, &detail)
	var m Approval
	_ = workflow.MutableSideEffect(ctx, approveSignal, func(ctx workflow.Context) any { return Approval{} }, func(a, b any) bool { return false }).Get(ctx, &m)
	workflow.Go(ctx, func(ctx workflow.Context) {
		var later Out
		_ = workflow.ExecuteActivity(ctx, Charge, in).Get(ctx, &later)
	})
	var dynamic Approval
	_ = workflow.WaitSignal(ctx, "dyn-"+in.OrderID).Get(ctx, &dynamic)
	return o, nil
}
`

func TestPayloadTypesOnGeneratedForms(t *testing.T) {
	result, err := GenerateFromSource([]byte(payloadFormsSrc), Options{WorkflowType: "forms", FuncName: "Order", Filename: "order.go"})
	if err != nil {
		t.Fatal(err)
	}
	types, err := programpkg.PayloadTypes(result.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for position, decoded := range types.Positions {
		for _, item := range decoded {
			got[position.String()] = append(got[position.String()], item.Name)
		}
		sort.Strings(got[position.String()])
	}
	want := map[string][]string{
		"workflow input":                           {"order.Approval"},
		`signal "approve"`:                         {"order.Approval"},
		`input of pending activity "order.Charge"`: {"order.Approval"},
		`result of activity "order.Charge"`:        {"order.Out"},
		`result of child workflow "kid"`:           {"order.Out"},
		`signal "later"`:                           {"order.Approval"},
		`signal "pre-later"`:                       {"order.Approval"},
		`signal "copy-3"`:                          {"order.Approval"},
		`signal "field"`:                           {"string"},
		`signal "element"`:                         {"order.Approval"},
		`signal "helper"`:                          {"order.Approval"},
		`mutable side effect "approve"`:            {"order.Approval"},
		"application error detail":                 {"order.Detail"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PayloadTypes() positions = %v\nwant %v", got, want)
	}
	var unresolved []string
	for _, site := range types.Unresolved {
		unresolved = append(unresolved, site.String())
	}
	wantUnresolved := []string{
		"a signal with a non-constant name decoded into order.Approval at program[38].expr.Get (the signal name is not a constant)",
		"a signal with a non-constant name decoded into order.Out at program[3].let.__wlang_func_waitNamed.fn.do[1].return.Get (the signal name is not a constant)",
	}
	if !reflect.DeepEqual(unresolved, wantUnresolved) {
		t.Fatalf("PayloadTypes() unresolved = %q\nwant %q", unresolved, wantUnresolved)
	}
}
