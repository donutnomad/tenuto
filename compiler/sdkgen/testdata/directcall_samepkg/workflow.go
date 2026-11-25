// Package directcallsamepkg 是“同包直调注解函数”的回归 fixture。
package directcallsamepkg

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
}

// @Activity(type="payments.charge",queue="payments",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID}, nil
}

// @Workflow(type="directcall-samepkg-flow",name="Direct Call Same Package Flow",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
	return Charge(context.Background(), in)
}
