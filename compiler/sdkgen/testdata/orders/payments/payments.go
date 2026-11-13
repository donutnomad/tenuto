// Package payments 是 sdkgen 测试 fixture 的 remote activity 契约包。
package payments

import "context"

// SettleInput 是清算入参。
type SettleInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// SettleOutput 是清算结果。
type SettleOutput struct {
	Receipt string `json:"receipt"`
	Settled bool   `json:"settled"`
}

// @Activity(type="payments.settle",queue="payments",version=v1,remote=true)
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
