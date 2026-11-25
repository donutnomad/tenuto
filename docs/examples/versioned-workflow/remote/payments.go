// Package remote 声明一个跨包 remote activity 契约。
//
// remote 契约写成合法的命名函数类型（内部无函数体）：本仓只持有协议形状，实现在
// payments 队列的 worker 里。生成器为它产出可导出的零值 marker 变量
// `SettleRemoteActivity` 和桥 `CallSettle`，Workflow 编排里可直接调用 CallSettle。
package remote

import "context"

// SettleInput 是清算入参。
type SettleInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// SettleOutput 是清算结果。
type SettleOutput struct {
	Receipt string `json:"receipt"`
	Settled bool   `json:"settled"`
}

// @RemoteActivity(type="payments.settle",queue="payments",version=v1)
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
