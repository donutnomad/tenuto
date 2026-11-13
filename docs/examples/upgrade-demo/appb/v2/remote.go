// Package v2 是 app-b（订单）的第二个发布版本，跟进 app-a v2 的三处契约变化：
//
//   - fulfillment.reserve：入参加上可选字段 warehouse（指纹随之变化）。
//   - fulfillment.quote：version 改成 v2。
//   - fulfillment.ship-v2：新增契约；旧的 fulfillment.ship 契约保留给升级前启动的 Run。
package v2

import "context"

type ReserveInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	// Warehouse 与 app-a v2 的 ReserveInput 保持一致：同名、同类型、同样带 omitempty。
	Warehouse string `json:"warehouse,omitempty"`
}

type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
	Warehouse     string `json:"warehouse"`
	ServedBy      string `json:"served_by"`
}

// @Activity(type="fulfillment.reserve",queue="fulfillment",version=v1,remote=true)
type ReserveRemote func(context.Context, ReserveInput) (ReserveOutput, error)

type QuoteInput struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

type QuoteOutput struct {
	Fee      int64  `json:"fee"`
	ServedBy string `json:"served_by"`
}

// @Activity(type="fulfillment.quote",queue="fulfillment",version=v2,remote=true)
type QuoteRemote func(context.Context, QuoteInput) (QuoteOutput, error)

type ShipInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Address       string `json:"address"`
}

type ShipOutput struct {
	TrackingNo string `json:"tracking_no"`
	ServedBy   string `json:"served_by"`
}

// @Activity(type="fulfillment.ship",queue="fulfillment",version=v1,remote=true)
type ShipRemote func(context.Context, ShipInput) (ShipOutput, error)

type ShipV2Input struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	City          string `json:"city"`
	Street        string `json:"street"`
}

type ShipV2Output struct {
	TrackingNo string `json:"tracking_no"`
	Carrier    string `json:"carrier"`
	ServedBy   string `json:"served_by"`
}

// @Activity(type="fulfillment.ship-v2",queue="fulfillment",version=v1,remote=true)
type ShipV2Remote func(context.Context, ShipV2Input) (ShipV2Output, error)
