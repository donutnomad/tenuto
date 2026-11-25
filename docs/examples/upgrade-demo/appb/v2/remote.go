// Package v2 是 app-b（订单）的第二个发布版本，跟进 app-a v2 的三处契约变化：
//
//   - fulfillment.reserve：入参加上可选字段 warehouse（指纹随之变化）。
//   - fulfillment.quote-taxed：运费改用含税报价，调用新的 type；结构与 fulfillment.quote 相同。
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

// @RemoteActivity(type="fulfillment.reserve",queue="fulfillment")
type ReserveRemote func(context.Context, ReserveInput) (ReserveOutput, error)

type QuoteInput struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

type QuoteOutput struct {
	Fee      int64  `json:"fee"`
	ServedBy string `json:"served_by"`
}

// @RemoteActivity(type="fulfillment.quote",queue="fulfillment")
type QuoteRemote func(context.Context, QuoteInput) (QuoteOutput, error)

// @RemoteActivity(type="fulfillment.quote-taxed",queue="fulfillment")
type QuoteTaxedRemote func(context.Context, QuoteInput) (QuoteOutput, error)

type ShipInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Address       string `json:"address"`
}

type ShipOutput struct {
	TrackingNo string `json:"tracking_no"`
	ServedBy   string `json:"served_by"`
}

// @RemoteActivity(type="fulfillment.ship",queue="fulfillment")
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

// @RemoteActivity(type="fulfillment.ship-v2",queue="fulfillment")
type ShipV2Remote func(context.Context, ShipV2Input) (ShipV2Output, error)
