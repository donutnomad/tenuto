package v1

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type OrderInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	Address string `json:"address"`
	// ConfirmWindowSeconds 是等待客户确认的秒数，超过后订单过期。
	ConfirmWindowSeconds int `json:"confirm_window_seconds"`
}

type OrderOutput struct {
	OrderRef   string `json:"order_ref"`
	Status     string `json:"status"`
	Amount     int64  `json:"amount"`
	Warehouse  string `json:"warehouse"`
	ShipFee    int64  `json:"ship_fee"`
	TrackingNo string `json:"tracking_no"`
	ReservedBy string `json:"reserved_by"`
	QuotedBy   string `json:"quoted_by"`
	ShippedVia string `json:"shipped_via"`
}

// PlaceResult 是 Update "place" 的返回值：调用方同步拿到订单号与下单时间。
type PlaceResult struct {
	OrderRef string `json:"order_ref"`
	PlacedAt string `json:"placed_at"`
}

// OrderFlow 处理一张订单：定价 → 等客户确认（带超时）→ 并行预留库存与算运费 → 发货。
//
// policy=auto_upgrade：运行中的 Run 在下一个 Workflow Task 边界采用最新发布的 Program。
//
// @Workflow(type="order-flow",name="Order Flow",policy=auto_upgrade,version=v1)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	// 确定性的随机值与时间：首次执行写进 History，重放读回同一个值。
	orderRef := workflow.RandomUUID(ctx)
	placedAt := workflow.Now(ctx)
	stage := "pricing"
	amount := int64(0)

	// Update：带返回值的持久化命令。Handler 在主流程第一次阻塞之前注册。
	if err := workflow.SetUpdateHandler(ctx, "place", func(ctx workflow.Context) (PlaceResult, error) {
		return PlaceResult{OrderRef: orderRef, PlacedAt: placedAt.Format(time.RFC3339)}, nil
	}); err != nil {
		return OrderOutput{}, err
	}
	// Query：只读，读的是已提交的 Workflow 内存状态，不写 History。
	if err := workflow.SetQueryHandler(ctx, "status", func(ctx workflow.Context, _ map[string]any) map[string]any {
		return map[string]any{"stage": stage, "order_ref": orderRef, "amount": amount}
	}); err != nil {
		return OrderOutput{}, err
	}

	// 本地 Activity，带超时与重试策略。
	priced, err := CallPrice(ctx, PriceInput{SKU: in.SKU, Qty: in.Qty}, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    200 * time.Millisecond,
			BackoffCoefficient: 2,
			MaxInterval:        2 * time.Second,
		},
	})
	if err != nil {
		return OrderOutput{}, err
	}
	amount = priced.Amount
	stage = "awaiting-confirm"

	// 等客户确认，或者确认窗口到期。两条协程各等一件事，主协程等其中任意一件发生。
	confirmed := false
	expired := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		var payload map[string]any
		if err := workflow.WaitSignal(ctx, "confirm").Get(ctx, &payload); err == nil {
			confirmed = true
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		if err := workflow.Sleep(ctx, time.Duration(in.ConfirmWindowSeconds)*time.Second); err == nil {
			expired = true
		}
	})
	workflow.Await(ctx, func() bool { return confirmed || expired })
	if !confirmed {
		return OrderOutput{OrderRef: orderRef, Status: "expired", Amount: amount}, nil
	}
	stage = "fulfilling"

	// 跨进程调用 app-a：两个 Future 在同一个 Workflow Task 里调度，并行执行。
	reserveFuture := workflow.ExecuteActivity(ctx, ReserveRemoteActivity, ReserveInput{OrderID: in.OrderID, SKU: in.SKU, Qty: in.Qty}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	quoteFuture := workflow.ExecuteActivity(ctx, QuoteRemoteActivity, QuoteInput{OrderID: in.OrderID, Qty: in.Qty}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var reserved ReserveOutput
	if err := reserveFuture.Get(ctx, &reserved); err != nil {
		return OrderOutput{}, err
	}
	var quoted QuoteOutput
	if err := quoteFuture.Get(ctx, &quoted); err != nil {
		return OrderOutput{}, err
	}

	shipped, err := CallShip(ctx, ShipInput{OrderID: in.OrderID, ReservationID: reserved.ReservationID, Address: in.Address}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	stage = "shipped"
	return OrderOutput{
		OrderRef:   orderRef,
		Status:     "shipped",
		Amount:     amount,
		Warehouse:  reserved.Warehouse,
		ShipFee:    quoted.Fee,
		TrackingNo: shipped.TrackingNo,
		ReservedBy: reserved.ServedBy,
		QuotedBy:   quoted.ServedBy,
		ShippedVia: "fulfillment.ship@" + shipped.ServedBy,
	}, nil
}
