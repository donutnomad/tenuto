// Package v1 是 multiprocess 示例的第一代 OrderFlow：库存检查 → 库存占用 → 结算 → 占用转正。
//
// 前两步在 app-2 的 inventory 队列执行，结算在 app-2 的 ledger 队列执行；最后一步是 app-1
// 自己的本地 activity。整个编排完全由 app-1 发起，所有远程调用都走 app-2 的 worker。
package v1

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

// @Workflow(type="order-flow",name="Multiprocess Order Flow",policy=auto_upgrade,version=v1)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	// 第一代标记点：v1/v2 写入 4，v3 提高 minSupported 后旧历史 marker 越界即不兼容。
	version := workflow.GetVersion(ctx, "order-flow-changes", 3, 4)
	note := "v1"
	if version >= 4 {
		note = "v1"
	}
	stock, err := contracts.CallCheck(ctx, contracts.CheckStockInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	hold, err := contracts.CallHold(ctx, contracts.HoldStockInput{OrderID: in.OrderID, StockID: stock.StockID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
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
