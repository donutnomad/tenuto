// Package activities 是 directcall fixture 的本地 activity 实现（不生成桥调用）。
package activities

import "context"

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
}

// @Activity(type="payments.charge",queue="payments")
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID}, nil
}
