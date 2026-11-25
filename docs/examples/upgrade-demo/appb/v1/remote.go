// Package v1 是 app-b（订单）的第一个发布版本：订单 Workflow、本地 Activity，
// 以及对 app-a 履约 Activity 的远程契约声明。
//
// 本文件是远程契约。app-a 拥有这些 Activity 的实现；app-b 只声明自己依赖的协议形状：
// type、queue、version，以及入参出参的 JSON 结构。DTO 由 app-b 自己定义，
// 字段名、类型、可选性与 app-a 一致时两侧的 wire 指纹相同。
// 两侧的一致性由 contract_test.go 在 CI 里核对，由 Server 在调度时再核对一次。
package v1

import "context"

type ReserveInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
	Warehouse     string `json:"warehouse"`
	ServedBy      string `json:"served_by"`
}

// @RemoteActivity(type="fulfillment.reserve",queue="fulfillment",version=v1)
type ReserveRemote func(context.Context, ReserveInput) (ReserveOutput, error)

type QuoteInput struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

type QuoteOutput struct {
	Fee      int64  `json:"fee"`
	ServedBy string `json:"served_by"`
}

// @RemoteActivity(type="fulfillment.quote",queue="fulfillment",version=v1)
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

// @RemoteActivity(type="fulfillment.ship",queue="fulfillment",version=v1)
type ShipRemote func(context.Context, ShipInput) (ShipOutput, error)
