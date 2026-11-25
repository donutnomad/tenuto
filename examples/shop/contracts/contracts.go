// Package contracts 是 shop 三个服务之间的 Activity 契约：type、queue 与入参出参。
//
// 契约写成用 @RemoteActivity 标注的命名函数类型，不含实现。order-service 的 Workflow 通过生成的
// CallXxx 调度它们；payment-service 与 warehouse-service 各自实现并在自己的队列上执行。
// 三个进程只共享这个包，不互相链接实现代码。
package contracts

//go:generate go tool tenuto-workflow-gen generate .

import "context"

// 业务错误类型。Activity 用 apperror.NewNonRetryable 返回，Workflow 用
// workflow.IsApplicationError 按类型分支。
const (
	ErrOutOfStock      = "OutOfStock"
	ErrPaymentDeclined = "PaymentDeclined"
)

// ---------- warehouse 队列：warehouse-service 实现 ----------

type ReserveInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

type ReserveOutput struct {
	ReservationID string `json:"reservation_id"`
}

// @RemoteActivity(type="warehouse.reserve",queue="warehouse")
type ReserveRemote func(context.Context, ReserveInput) (ReserveOutput, error)

type ReleaseInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason"`
}

type ReleaseOutput struct {
	Released bool `json:"released"`
}

// @RemoteActivity(type="warehouse.release",queue="warehouse")
type ReleaseRemote func(context.Context, ReleaseInput) (ReleaseOutput, error)

type ShipInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Address       string `json:"address"`
}

type ShipOutput struct {
	TrackingNo string `json:"tracking_no"`
}

// @RemoteActivity(type="warehouse.ship",queue="warehouse")
type ShipRemote func(context.Context, ShipInput) (ShipOutput, error)

// ---------- payments 队列：payment-service 实现 ----------

type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"` // 单位：分
}

type ChargeOutput struct {
	PaymentID string `json:"payment_id"`
}

// @RemoteActivity(type="payments.charge",queue="payments")
type ChargeRemote func(context.Context, ChargeInput) (ChargeOutput, error)
