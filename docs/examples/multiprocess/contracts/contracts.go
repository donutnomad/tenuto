// Package contracts 声明跨进程 activity 契约，供 app-1（编排方）与 app-2（执行方）共用。
//
// 契约写成合法的命名函数类型、不带实现：两边各自引用的只是协议形状（type/queue 与
// I/O wire 指纹）。app-1 的 Workflow 通过生成的 CallXxx 调度，app-2 在自己的队列上领取并执行。
package contracts

import "context"

// CheckStockInput 是库存检查入参。
type CheckStockInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// CheckStockOutput 是库存检查结果。
type CheckStockOutput struct {
	StockID  string `json:"stock_id"`
	Location string `json:"location"`
}

// @RemoteActivity(type="inventory.check",queue="inventory")
type CheckStockRemote func(context.Context, CheckStockInput) (CheckStockOutput, error)

// HoldStockInput 是库存占用入参。
type HoldStockInput struct {
	OrderID string `json:"order_id"`
	StockID string `json:"stock_id"`
}

// HoldStockOutput 是库存占用结果。
type HoldStockOutput struct {
	HoldID string `json:"hold_id"`
}

// @RemoteActivity(type="inventory.hold",queue="inventory")
type HoldStockRemote func(context.Context, HoldStockInput) (HoldStockOutput, error)

// ReleaseHoldInput 是释放库存占用的入参，同时也是 HoldStock 的补偿路径。
type ReleaseHoldInput struct {
	OrderID string `json:"order_id"`
	HoldID  string `json:"hold_id"`
	Reason  string `json:"reason"`
}

// ReleaseHoldOutput 是释放占用的结果。
type ReleaseHoldOutput struct {
	Released bool `json:"released"`
}

// @RemoteActivity(type="inventory.release",queue="inventory")
type ReleaseHoldRemote func(context.Context, ReleaseHoldInput) (ReleaseHoldOutput, error)

// RiskReviewInput 是 v2 新增的风控审查入参。
type RiskReviewInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// RiskReviewOutput 是风控审查结果。
type RiskReviewOutput struct {
	Level string `json:"level"`
}

// @RemoteActivity(type="risk.review",queue="ledger")
type RiskReviewRemote func(context.Context, RiskReviewInput) (RiskReviewOutput, error)

// SettleInput 是扣款结算入参。
type SettleInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// SettleOutput 是扣款结算结果。
type SettleOutput struct {
	Receipt string `json:"receipt"`
	Settled bool   `json:"settled"`
}

// @RemoteActivity(type="ledger.settle",queue="ledger")
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)

// ChargeInput 是 v3 新增的金库扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// ChargeOutput 是金库扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
	Remote   bool   `json:"remote"`
}

// @RemoteActivity(type="ledger.charge",queue="ledger")
type ChargeRemote func(context.Context, ChargeInput) (ChargeOutput, error)
