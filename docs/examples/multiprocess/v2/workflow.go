// Package v2 是 OrderFlow 的第二代：v3 之前的当前版本，多一次“风控”远程调用和一个分支。
//
// 与 v1 的差异只出现在标记点之后：history 里 marker 为 5 时走风控 → 扣款 → 结算（policy=strict），
// marker 为 4（v1 写入）时走 v1 路径（policy=legacy）。
package v2

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

// @Workflow(id="order-flow",name="Multiprocess Order Flow",queue="orders",policy=auto_upgrade,version=v2)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	version := workflow.GetVersion(ctx, "order-flow-changes", 3, 5)
	stock, err := contracts.CallCheck(ctx, contracts.CheckStockInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	hold, err := contracts.CallHold(ctx, contracts.HoldStockInput{OrderID: in.OrderID, StockID: stock.StockID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	note := "legacy"
	if version >= 5 {
		review, err := contracts.CallReview(ctx, contracts.RiskReviewInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return OrderOutput{}, err
		}
		_ = review
		note = "strict"
	}
	settled, err := contracts.CallSettle(ctx, contracts.SettleInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	out, err := CallFinalize(ctx, FinalizeInput{OrderID: in.OrderID, HoldID: hold.HoldID, Receipt: settled.Receipt}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	return OrderOutput{OrderID: in.OrderID, Status: out.Status, StockID: stock.StockID, HoldID: hold.HoldID, Receipt: settled.Receipt, Note: note}, nil
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
// @Activity(type="orders.finalize",queue="orders",version=v1)
func Finalize(_ context.Context, in FinalizeInput) (FinalizeOutput, error) {
	return FinalizeOutput{Status: "finalized:" + in.HoldID + ":" + in.Receipt}, nil
}
