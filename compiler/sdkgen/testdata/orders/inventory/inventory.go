// Package inventory 是 sdkgen 测试 fixture 的跨包本地 activity 实现包。
package inventory

import "context"

// ReserveInput 是预留入参。
type ReserveInput struct {
	OrderID string `json:"order_id"`
}

// ReserveOutput 是预留结果。
type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
}

// @Activity(type="inventory.reserve",queue="inventory",version=v1)
func Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
	return ReserveOutput{ReservationID: "reservation-" + in.OrderID}, nil
}
