// Package activities 是 lifecycle 示例的本地 activity 实现。
//
// 每个实现用 `@Activity(type=...,queue=...,version=...)` 声明协议身份；生成器据此产出
// 同包 typed 调用桥（zz_activities_contract_gen.go 里的 CallReserve/CallCharge/...）和
// 稳定 marker（实现函数本身）。
package activities

import "context"

// ReserveInput 是预留库存的入参。
type ReserveInput struct {
	OrderID string `json:"order_id"`
}

// ReserveOutput 是预留库存的结果。
type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
}

// @Activity(type="lifecycle.reserve",queue="lifecycle",version=v1)
func Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
	return ReserveOutput{ReservationID: "reservation-" + in.OrderID}, nil
}

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
}

// ChargeOutput 是扣款结果；Policy 记录本次实际走的扣款分支。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
	Policy   string `json:"policy"`
}

// @Activity(type="lifecycle.charge",queue="lifecycle",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID, Policy: "legacy"}, nil
}

// @Activity(type="lifecycle.charge-v2",queue="lifecycle",version=v2)
func ChargeV2(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-v2-" + in.OrderID, Policy: "v2"}, nil
}

// NotifyInput 是通知入参。
type NotifyInput struct {
	OrderID string `json:"order_id"`
	Note    string `json:"note"`
}

// NotifyOutput 既是通知结果，也是 v1 workflow 的返回值类型（v1 末尾直接 return 桥调用）。
type NotifyOutput struct {
	OrderID   string `json:"order_id"`
	Note      string `json:"note"`
	Delivered bool   `json:"delivered"`
}

// @Activity(type="lifecycle.notify",queue="lifecycle",version=v1)
func Notify(ctx context.Context, in NotifyInput) (NotifyOutput, error) {
	return NotifyOutput{OrderID: in.OrderID, Note: in.Note, Delivered: true}, nil
}
