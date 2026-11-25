---
title: 多进程下单示例
description: order、payment、warehouse 三个服务进程通过 Tenuto Server 协作下单：远程契约、补偿、幂等重试、Signal、持久 Timer 与服务宕机后恢复
---

# 多进程下单示例

[`examples/shop`](https://github.com/donutnomad/tenuto/tree/main/examples/shop) 是一个独立的 Go 模块，按真实业务的形态拆成三个服务进程，加上 Tenuto Server 共四个进程：

| 进程 | 职责 | Task Queue |
| --- | --- | --- |
| `tenuto-server` | Workflow 引擎，执行全部 Workflow Task | — |
| `order-service` | HTTP 下单 API；登记 `place-order` Workflow；发送顾客通知 | `orders` |
| `payment-service` | 扣款，以订单号为幂等键；可模拟网关超时 | `payments` |
| `warehouse-service` | 预留库存、释放库存、发货，以订单号为幂等键 | `warehouse` |

三个服务之间没有直接调用，也不链接彼此的实现代码。它们只共享 `contracts` 包里的 Activity 契约。

## 一键运行

在仓库根目录执行：

```bash
./examples/shop/run-demo.sh
```

脚本构建并启动四个进程。tenuto-server 使用内存后端，不需要数据库。随后依次演示：

| 场景 | 结果 |
| --- | --- |
| 正常下单 | payment-service 第一次扣款成功后模拟响应丢失；Tenuto 按 RetryPolicy 重试，幂等表返回同一笔支付，只扣一次款。发送 `delivered` Signal 后订单完成，`confirmed_by` 为 `customer` |
| 缺货 | warehouse 返回不可重试的 `OutOfStock`，订单 `rejected`，不扣款 |
| 拒付 | payment 返回不可重试的 `PaymentDeclined`，Workflow 释放已预留的库存，订单 `payment_declined` |
| 支付服务宕机 | 停掉 payment-service 后下单：库存预留成功，Run 显示 `blocked`（`activity_route_unavailable`）。重启 payment-service 后从扣款继续，5 秒后持久 Timer 自动确认收货，`confirmed_by` 为 `timeout` |

`SHOP_KEEP=1` 在演示后保持进程运行。手动逐个启动进程的步骤见示例目录的 [README](https://github.com/donutnomad/tenuto/blob/main/examples/shop/README.md)。

## 跨服务契约

Workflow 所在的 order-service 不链接 payment 与 warehouse 的实现。被调用的 Activity 在 `contracts` 包里声明为带 `remote=true` 的命名函数类型，生成器据此产出调用桥 `CallReserve`、`CallCharge` 等：

```go
// examples/shop/contracts/contracts.go（节选）
type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"` // 单位：分
}

type ChargeOutput struct {
	PaymentID string `json:"payment_id"`
}

// @Activity(type="payments.charge",queue="payments",version=v1,remote=true)
type ChargeRemote func(context.Context, ChargeInput) (ChargeOutput, error)
```

payment-service 实现同一个 type 与 version，在 `payments` 队列上执行：

```go
// examples/shop/payment/payment.go（节选）
func (g *Gateway) Register(registry *activity.Registry) error {
	return activity.RegisterActivityOnQueue(registry, "payments", "payments.charge", "v1", g.Charge)
}
```

Worker 上线时把契约（type、version、入参出参指纹）报给 Server；调度时 Server 核对 Workflow 声明的契约与在线 Worker 上报的一致。队列上没有在线 Worker 时 Run 以 `activity_route_unavailable` 退避等待，见[运行 Worker](/develop/workers)。

## Workflow

`orders/workflow.go` 的 `place-order` 按业务错误类型分支，补偿写在 Workflow 函数体里：

```go
// examples/shop/orders/workflow.go（节选）
paid, err := contracts.CallCharge(ctx, contracts.ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, remote)
if err != nil {
	if workflow.IsApplicationError(err, contracts.ErrPaymentDeclined) {
		// 补偿：扣款被明确拒绝，释放刚才预留的库存。
		if _, err := contracts.CallRelease(ctx, contracts.ReleaseInput{
			OrderID: in.OrderID, ReservationID: reserved.ReservationID, Reason: "payment declined",
		}, remote); err != nil {
			return out, err
		}
		out.Status = StatusPaymentDeclined
		return notify(ctx, out)
	}
	return out, err
}
```

发货后同时等待 Signal 和 Timer，先到者决定确认方式：

```go
confirmedBy := ""
workflow.Go(ctx, func(ctx workflow.Context) {
	var signal map[string]any
	if err := workflow.WaitSignal(ctx, DeliveredSignal).Get(ctx, &signal); err == nil {
		confirmedBy = "customer"
	}
})
workflow.Go(ctx, func(ctx workflow.Context) {
	if err := workflow.Sleep(ctx, time.Duration(in.AutoConfirmSeconds)*time.Second); err == nil {
		confirmedBy = "timeout"
	}
})
if err := workflow.Await(ctx, func() bool { return confirmedBy != "" }); err != nil {
	return out, err
}
```

order-service 的 `GET /orders/{id}` 组合三种读法：`DescribeWorkflowExecution` 取 Run 状态与 Blocked 原因，运行中用 Query `status` 读取阶段，结束后用 `GetWorkflow(...).Get` 取结果。

## 进程分工

三个服务都以 `worker.RoleActivity` 运行，Workflow Task 由 tenuto-server 在进程内执行。order-service 拥有 `place-order`，在自己的 Worker 里登记定义：

```go
// examples/shop/cmd/order-service/main.go（节选）
svc.RunWorker(ctx, "order-service", c, worker.Options{
	Workflows:           []registration.Definition{orders.WorkflowDefinition()},
	Activities:          registry, // orders.notify
	ActivityConcurrency: 8,
	BuildID:             svc.Env("BUILD_ID", "order-dev"),
})
```

任一服务进程停止期间，Run 停在 Event History 记录的位置；进程恢复后继续。示例的 `go.mod` 用 `replace` 指向仓库源码，在自己的项目里改用 `go get github.com/donutnomad/tenuto@latest`，步骤见[接入已有项目](/quickstart/integrate)。
