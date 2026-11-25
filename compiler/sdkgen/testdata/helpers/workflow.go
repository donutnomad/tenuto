// Package helpers 是“Workflow 调用同包普通函数”的 fixture：helper 分布在多个文件里，
// 互相调用，其中一个带 workflow.Context 并经生成的桥调度 Activity。
package helpers

import (
	"context"
	"fmt"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type OrderInput struct {
	OrderID string `json:"order_id"`
	Items   []Item `json:"items"`
}

type Item struct {
	SKU   string `json:"sku"`
	Qty   int    `json:"qty"`
	Cents int    `json:"cents"`
}

type OrderOutput struct {
	Label    string `json:"label"`
	ChargeID string `json:"charge_id"`
}

type ChargeInput struct {
	OrderID string `json:"order_id"`
	Cents   int    `json:"cents"`
}

type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
}

// @Activity(type="payments.charge",queue="payments")
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: fmt.Sprintf("charge-%s-%d", in.OrderID, in.Cents)}, nil
}

// @Workflow(type="helpers-flow",name="Helpers Flow",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	total := orderTotal(in.Items)
	chargeID, err := charge(ctx, in.OrderID, total)
	if err != nil {
		return OrderOutput{}, err
	}
	return OrderOutput{Label: describe(in.OrderID, total), ChargeID: chargeID}, nil
}
