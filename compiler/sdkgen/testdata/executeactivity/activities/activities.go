// Package activities 是 executeactivity fixture 的 activity 实现。
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

// AuditInput 是审计入参。
type AuditInput struct {
	OrderID string `json:"order_id"`
}

// AuditOutput 是审计结果。
type AuditOutput struct {
	Recorded bool `json:"recorded"`
}

// Audit 没有 @Activity 注解：直接调用时 Activity type 就是符号写法 activities.Audit。
func Audit(ctx context.Context, in AuditInput) (AuditOutput, error) {
	return AuditOutput{Recorded: true}, nil
}
