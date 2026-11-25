package workflow_test

import (
	"github.com/donutnomad/tenuto/sdk/workflow"
)

type OrderInput struct {
	OrderID string `json:"order_id"`
}

type OrderOutput struct {
	Status string `json:"status"`
}

// PlaceOrder 是一个 Workflow：等待 approve Signal，按它的参数完成或以应用错误结束。
// 生成器把它翻译成 WLang 程序由 Worker 执行；它不在普通 Go 代码里调用。
//
// @Workflow(id="place-order",version=v1)
func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	var decision map[string]any
	if err := workflow.WaitSignal(ctx, "approve").Get(ctx, &decision); err != nil {
		return OrderOutput{}, err
	}
	if decision["approved"] != true {
		return OrderOutput{}, workflow.NewApplicationError("order "+in.OrderID+" rejected", "OrderRejected")
	}
	return OrderOutput{Status: "approved"}, nil
}

// Workflow 函数由生成器翻译，本例只展示写法，不运行。
func Example() {
	_ = PlaceOrder
}
