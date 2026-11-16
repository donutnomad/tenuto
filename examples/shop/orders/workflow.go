// Package orders 是下单流程：place-order Workflow 和 order-service 自己执行的通知 Activity。
//
// 流程：预留库存 → 扣款 → 发货 → 等待确认收货 → 通知顾客。
//   - 缺货：直接结束为 rejected，没有需要补偿的步骤。
//   - 拒付：释放已预留的库存（补偿），结束为 payment_declined。
//   - 网关超时、服务宕机等临时故障：按 RetryPolicy 重试，Workflow 停在原处等服务恢复。
//
// Workflow 由 tenuto-server 执行；order-service、payment-service、warehouse-service
// 只执行 Activity。任何一个服务重启，Run 都从 Event History 上的位置继续。
package orders

//go:generate go tool tenuto-workflow-gen generate .

import (
	"context"
	"log"
	"time"

	"github.com/donutnomad/tenuto/examples/shop/contracts"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// 订单最终状态。
const (
	StatusCompleted       = "completed"
	StatusRejected        = "rejected"
	StatusPaymentDeclined = "payment_declined"
)

// DeliveredSignal 是确认收货的 Signal 名称。
const DeliveredSignal = "delivered"

type OrderInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	Amount  int64  `json:"amount"` // 单位：分
	Address string `json:"address"`
	// AutoConfirmSeconds 秒内没有收到 delivered Signal 时自动确认收货。
	AutoConfirmSeconds int `json:"auto_confirm_seconds"`
}

type OrderOutput struct {
	OrderID       string `json:"order_id"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	TrackingNo    string `json:"tracking_no"`
	ConfirmedBy   string `json:"confirmed_by"`
}

// @Workflow(id="place-order",name="Place Order",queue="orders",policy=auto_upgrade,version=v1)
func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	out := OrderOutput{OrderID: in.OrderID}
	stage := "reserving"
	// GET /orders/{id} 通过这个 Query 读取进行中的阶段。
	if err := workflow.SetQueryHandler(ctx, "status", func(ctx workflow.Context, _ map[string]any) map[string]any {
		return map[string]any{
			"stage":          stage,
			"reservation_id": out.ReservationID,
			"payment_id":     out.PaymentID,
			"tracking_no":    out.TrackingNo,
		}
	}); err != nil {
		return out, err
	}

	// 下游临时故障时一直重试（MaxAttempts=0），间隔 1s 起、最长 5s；
	// NonRetryable 的业务错误（缺货、拒付）不重试。
	remote := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: workflow.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaxInterval:        5 * time.Second,
		},
	}

	reserved, err := contracts.CallReserve(ctx, contracts.ReserveInput{OrderID: in.OrderID, SKU: in.SKU, Qty: in.Qty}, remote)
	if err != nil {
		if workflow.IsApplicationError(err, contracts.ErrOutOfStock) {
			stage = StatusRejected
			out.Status = StatusRejected
			out.Reason = "out of stock"
			return notify(ctx, out)
		}
		return out, err
	}
	out.ReservationID = reserved.ReservationID

	stage = "charging"
	paid, err := contracts.CallCharge(ctx, contracts.ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, remote)
	if err != nil {
		if workflow.IsApplicationError(err, contracts.ErrPaymentDeclined) {
			// 补偿：扣款被明确拒绝，释放刚才预留的库存。
			stage = "releasing"
			if _, err := contracts.CallRelease(ctx, contracts.ReleaseInput{
				OrderID: in.OrderID, ReservationID: reserved.ReservationID, Reason: "payment declined",
			}, remote); err != nil {
				return out, err
			}
			stage = StatusPaymentDeclined
			out.Status = StatusPaymentDeclined
			out.Reason = "payment declined, stock released"
			return notify(ctx, out)
		}
		return out, err
	}
	out.PaymentID = paid.PaymentID

	stage = "shipping"
	shipped, err := contracts.CallShip(ctx, contracts.ShipInput{
		OrderID: in.OrderID, ReservationID: reserved.ReservationID, Address: in.Address,
	}, remote)
	if err != nil {
		return out, err
	}
	out.TrackingNo = shipped.TrackingNo

	// 等顾客确认收货，或到期自动确认。Timer 是持久的：期间所有进程重启都不影响到期时间。
	stage = "awaiting_delivery"
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

	stage = StatusCompleted
	out.Status = StatusCompleted
	out.ConfirmedBy = confirmedBy
	return notify(ctx, out)
}

// notify 通知顾客订单结果，然后把结果作为 Workflow 输出返回。
func notify(ctx workflow.Context, out OrderOutput) (OrderOutput, error) {
	if _, err := CallNotify(ctx, NotifyInput{OrderID: out.OrderID, Status: out.Status, Reason: out.Reason},
		workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}); err != nil {
		return out, err
	}
	return out, nil
}

type NotifyInput struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
}

type NotifyOutput struct {
	Sent bool `json:"sent"`
}

// Notify 在 order-service 进程里执行。真实实现发邮件或短信，以 OrderID+Status 作为幂等键。
//
// @Activity(type="orders.notify",version=v1)
func Notify(_ context.Context, in NotifyInput) (NotifyOutput, error) {
	log.Printf("notify customer: order=%s status=%s %s", in.OrderID, in.Status, in.Reason)
	return NotifyOutput{Sent: true}, nil
}
