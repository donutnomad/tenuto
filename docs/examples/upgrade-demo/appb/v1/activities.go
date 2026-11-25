package v1

import "context"

// Build 是本发布版本的 Worker BuildID。
const Build = "app-b-v1"

// unitPrice 是示例里所有商品的单价（分）。
const unitPrice = 1200

type PriceInput struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type PriceOutput struct {
	Amount int64 `json:"amount"`
}

// Price 是 app-b 自己的本地 Activity：给订单定价。
//
// @Activity(type="orders.price",queue="orders",version=v1)
func Price(_ context.Context, in PriceInput) (PriceOutput, error) {
	return PriceOutput{Amount: unitPrice * int64(in.Qty)}, nil
}
