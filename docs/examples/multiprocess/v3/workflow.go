// Package v3 是 OrderFlow 的第三代：在 v2 基础上更换扣款实现。
//
// 它是本示例的“不兼容版本”：
//   - 标记点调用 workflow.GetVersion(ctx, "order-flow-changes", 7, 7)：旧执行 history 里的
//     marker（4 或 5）低于 minSupported，replay 时判定非确定性并抛错，Run 不终结、持续重试；
//   - 新执行（无 marker）走全新的 Charge → Finalize（policy=strict-v3），不会再调度 v1/v2 的路径。
package v3

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/docs/examples/multiprocess/contracts"
)

// OrderInput 是 OrderFlow 的入参。
type OrderInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// OrderOutput 是 OrderFlow 的返回值。
type OrderOutput struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	StockID string `json:"stock_id"`
	HoldID  string `json:"hold_id"`
	Receipt string `json:"receipt"`
	Note    string `json:"note"`
}

// @Workflow(type="order-flow",name="Multiprocess Order Flow",policy=auto_upgrade,version=v3)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	workflow.GetVersion(ctx, "order-flow-changes", 7, 7)
	stock, err := contracts.CallCheck(ctx, contracts.CheckStockInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	hold, err := contracts.CallHold(ctx, contracts.HoldStockInput{OrderID: in.OrderID, StockID: stock.StockID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	charged, err := contracts.CallCharge(ctx, contracts.ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	out, err := CallFinalize(ctx, FinalizeInput{OrderID: in.OrderID, HoldID: hold.HoldID, Receipt: charged.ChargeID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	return OrderOutput{OrderID: in.OrderID, Status: out.Status, StockID: stock.StockID, HoldID: hold.HoldID, Receipt: charged.ChargeID, Note: "strict-v3"}, nil
}

// FinalizeInput 是本地收尾 activity 的入参。
type FinalizeInput struct {
	OrderID string `json:"order_id"`
	HoldID  string `json:"hold_id"`
	Receipt string `json:"receipt"`
}

// FinalizeOutput 是本地收尾 activity 的结果。
type FinalizeOutput struct {
	Status string `json:"status"`
}

// Finalize 是 app-1 本地执行在 orders 队列上的 activity。
//
// @Activity(type="orders.finalize",queue="orders")
func Finalize(_ context.Context, in FinalizeInput) (FinalizeOutput, error) {
	return FinalizeOutput{Status: "finalized:" + in.HoldID + ":" + in.Receipt}, nil
}
