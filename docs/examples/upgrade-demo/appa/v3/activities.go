// Package v3 是 app-a（履约）的第三个发布版本。Activity 与 v2 完全相同；
// 变化只在 workflow.go：退货 Workflow 调整了步骤顺序，并且没有加版本保护。
// 这是一次有问题的发布，示例用它展示 auto_upgrade 的 Run 遇到不兼容 Program 时的表现。
package v3

import "context"

// Build 是本发布版本的 Worker BuildID。每个 Activity 把它写进输出的 served_by，
// 示例据此展示「这次调用由哪个 build 执行」。
const Build = "app-a-v3"

// defaultWarehouse 是预留库存的默认仓库。
const defaultWarehouse = "west"

type ReserveInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	// Warehouse 是 v2 新增的可选字段：指定发货仓库。旧调用方不传，解码后是零值。
	Warehouse string `json:"warehouse,omitempty"`
}

type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
	Warehouse     string `json:"warehouse"`
	ServedBy      string `json:"served_by"`
}

// Reserve 预留库存。新增字段把零值当作「未提供」，旧调用得到与 v1 相同的结果。
// version 保持 v1：结构变化已经体现在入参指纹上，version 留给指纹覆盖不到的语义变化。
//
// @Activity(type="fulfillment.reserve",queue="fulfillment",version=v1)
func Reserve(_ context.Context, in ReserveInput) (ReserveOutput, error) {
	warehouse := in.Warehouse
	if warehouse == "" {
		warehouse = defaultWarehouse
	}
	return ReserveOutput{
		ReservationID: "rsv-" + in.OrderID,
		Warehouse:     warehouse,
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

// Quote 计算运费。v2 的语义：fee 是含 10% 税的运费。
// 入参出参的结构与 v1 完全相同，指纹不变；语义变了，所以手动把 version 升到 v2。
//
// @Activity(type="fulfillment.quote",queue="fulfillment",version=v2)
func Quote(_ context.Context, in QuoteInput) (QuoteOutput, error) {
	return QuoteOutput{Fee: 550 * int64(in.Qty), ServedBy: Build}, nil
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

// Ship 是旧的发货 Activity，保留给仍在调用旧 type 的 Run。
//
// @Activity(type="fulfillment.ship",queue="fulfillment",version=v1)
func Ship(_ context.Context, in ShipInput) (ShipOutput, error) {
	return ShipOutput{TrackingNo: "trk-" + in.OrderID, ServedBy: Build}, nil
}

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

// ShipV2 是新的发货 Activity：地址拆成 city + street，出参多了承运商。
// 入参出参都与旧结构不兼容，所以用新的 type，旧 type 原样保留。
//
// @Activity(type="fulfillment.ship-v2",queue="fulfillment",version=v1)
func ShipV2(_ context.Context, in ShipV2Input) (ShipV2Output, error) {
	return ShipV2Output{TrackingNo: "trk2-" + in.OrderID, Carrier: "carrier-" + in.City, ServedBy: Build}, nil
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

type NotifyInput struct {
	OrderID   string `json:"order_id"`
	Condition string `json:"condition"`
}

type NotifyOutput struct {
	Sent bool `json:"sent"`
}

// Notify 把初步判定通知给客户。v2 新增。
//
// @Activity(type="fulfillment.notify",queue="fulfillment",version=v1)
func Notify(_ context.Context, _ NotifyInput) (NotifyOutput, error) {
	return NotifyOutput{Sent: true}, nil
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
