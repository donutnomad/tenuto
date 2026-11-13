// Package v1 是 app-a（履约）的第一个发布版本：履约 Activity 与退货 Workflow。
//
// 本文件是 Activity。Activity 是原生 Go 函数，签名固定为
// func(context.Context, In) (Out, error)，In / Out 是带 json tag 的 struct。
// 注解里的 type 是 Activity 的身份（调度与注册共用），version 是契约版本标签。
package v1

import "context"

// Build 是本发布版本的 Worker BuildID。每个 Activity 把它写进输出的 served_by，
// 示例据此展示「这次调用由哪个 build 执行」。
const Build = "app-a-v1"

// defaultWarehouse 是预留库存的默认仓库。
const defaultWarehouse = "west"

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

// Reserve 预留库存。
//
// @Activity(type="fulfillment.reserve",queue="fulfillment",version=v1)
func Reserve(_ context.Context, in ReserveInput) (ReserveOutput, error) {
	return ReserveOutput{
		ReservationID: "rsv-" + in.OrderID,
		Warehouse:     defaultWarehouse,
		ServedBy:      Build,
	}, nil
}

type QuoteInput struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

type QuoteOutput struct {
	Fee      int64  `json:"fee"`
	ServedBy string `json:"served_by"`
}

// Quote 计算运费。v1 的语义：fee 是不含税的运费。
//
// @Activity(type="fulfillment.quote",queue="fulfillment",version=v1)
func Quote(_ context.Context, in QuoteInput) (QuoteOutput, error) {
	return QuoteOutput{Fee: 500 * int64(in.Qty), ServedBy: Build}, nil
}

type ShipInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Address       string `json:"address"`
}

type ShipOutput struct {
	TrackingNo string `json:"tracking_no"`
	ServedBy   string `json:"served_by"`
}

// Ship 发货。address 是一整行地址。
//
// @Activity(type="fulfillment.ship",queue="fulfillment",version=v1)
func Ship(_ context.Context, in ShipInput) (ShipOutput, error) {
	return ShipOutput{TrackingNo: "trk-" + in.OrderID, ServedBy: Build}, nil
}

type InspectInput struct {
	OrderID string `json:"order_id"`
}

type InspectOutput struct {
	Condition string `json:"condition"`
}

// Inspect 登记退货申请并给出初步判定。
//
// @Activity(type="fulfillment.inspect",queue="fulfillment",version=v1)
func Inspect(_ context.Context, _ InspectInput) (InspectOutput, error) {
	return InspectOutput{Condition: "resellable"}, nil
}

type RestockInput struct {
	OrderID   string `json:"order_id"`
	Condition string `json:"condition"`
}

type RestockOutput struct {
	Restocked bool `json:"restocked"`
}

// Restock 把退回的商品重新入库。
//
// @Activity(type="fulfillment.restock",queue="fulfillment",version=v1)
func Restock(_ context.Context, in RestockInput) (RestockOutput, error) {
	return RestockOutput{Restocked: in.Condition == "resellable"}, nil
}
