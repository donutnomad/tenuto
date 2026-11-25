// Package flows 是同包 remote 契约的回归 fixture：contract、workflow 与桥都在同一个包里。
package flows

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
	Policy   string `json:"policy"`
}

// @RemoteActivity(type="payments.charge.v1",queue="payments",version=v1)
type ChargeRemote func(context.Context, ChargeInput) (ChargeOutput, error)

// @Workflow(type="samepkg-flow",name="Same Package Flow",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
	return CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
}
