package sagaflow

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/sdk/apperror"
)

// 幂等键 = 固定操作名 + 稳定业务 ID。rerun/fork 出的新 Run 落到同一业务键上，下游按它去重。
const (
	opReserveInventory = "reserve_inventory"
	opChargePayment    = "charge_payment"
	opRefundPayment    = "refund_payment"
)

func businessKey(operation, businessID string) string {
	return operation + ":" + businessID
}

// @Activity(type="saga.reserve-inventory",queue="saga",version=v1)
func ReserveInventory(_ context.Context, in ReserveInventoryInput) (ReserveInventoryOutput, error) {
	// 真实实现用 IdempotencyKey 写去重表，或作为下游的幂等请求头。
	return ReserveInventoryOutput{
		ReservationID:  "reservation-" + in.OrderID,
		IdempotencyKey: businessKey(opReserveInventory, in.OrderID),
	}, nil
}

// @Activity(type="saga.charge-payment",queue="saga",version=v1)
func ChargePayment(_ context.Context, in ChargePaymentInput) (ChargePaymentOutput, error) {
	switch in.Scenario {
	case "payment_declined_error":
		// 永久失败：业务明确拒绝，重试没有意义。NonRetryable 让重试判定跳过次数与时限，
		// Type 让 Workflow 能按业务语义分支，details 携带结构化上下文供补偿与对账使用。
		return ChargePaymentOutput{}, apperror.NewNonRetryable(
			"card declined", "PaymentDeclined", nil,
			map[string]any{"order_id": in.OrderID, "reason": "insufficient_funds"})
	case "payment_unavailable_error":
		// 临时失败：provider 不可用，按 RetryPolicy 重试；重试耗尽后 Workflow 拿到同一个
		// typed error（Type 不变），据此判断要不要人工核对。
		return ChargePaymentOutput{}, apperror.New(
			"payment provider unavailable", "PaymentUnavailable")
	case "payment_unknown":
		// 模拟超时/响应丢失：返回 error 表示结果不确定，不返回业务状态。
		return ChargePaymentOutput{}, errors.New("payment provider timeout: outcome unknown")
	case "payment_pending":
		// 未知业务状态：err=nil 但 Status 不是 charged/declined，Workflow 会走人工核对。
		return ChargePaymentOutput{Status: "pending"}, nil
	case "payment_declined":
		return ChargePaymentOutput{
			Status:         "declined",
			IdempotencyKey: businessKey(opChargePayment, in.OrderID),
		}, nil
	}
	// 真实实现先按幂等键查扣款记录，命中则直接返回上次结果（响应丢失后的安全重试）。
	return ChargePaymentOutput{
		Status:         "charged",
		PaymentID:      "payment-" + in.OrderID,
		IdempotencyKey: businessKey(opChargePayment, in.OrderID),
	}, nil
}

// @Activity(type="saga.create-shipment",queue="saga",version=v1)
func CreateShipment(_ context.Context, in CreateShipmentInput) (CreateShipmentOutput, error) {
	switch in.Scenario {
	case "shipment_unknown":
		return CreateShipmentOutput{}, errors.New("warehouse timeout: outcome unknown")
	case "shipment_rejected", "shipment_rejected_refund_failed":
		return CreateShipmentOutput{Status: "rejected"}, nil
	}
	return CreateShipmentOutput{Status: "created", ShipmentID: "shipment-" + in.OrderID}, nil
}

// @Activity(type="saga.refund-payment",queue="saga",version=v1)
func RefundPayment(_ context.Context, in RefundPaymentInput) (RefundPaymentOutput, error) {
	if in.Scenario == "shipment_rejected_refund_failed" {
		return RefundPaymentOutput{}, errors.New("refund provider timeout: outcome unknown")
	}
	// 幂等：同一 PaymentID 重复退款返回同一结果。去重键是 refund_payment:PaymentID，
	// 不是 RunID——rerun/fork 会创建新 Run 并换掉 RunID，业务键不变。
	return RefundPaymentOutput{
		Refunded:       true,
		IdempotencyKey: businessKey(opRefundPayment, in.PaymentID),
	}, nil
}

// @Activity(type="saga.release-inventory",queue="saga",version=v1)
func ReleaseInventory(_ context.Context, in ReleaseInventoryInput) (ReleaseInventoryOutput, error) {
	// 幂等：已释放的 ReservationID 再释放一次返回同样结果。
	return ReleaseInventoryOutput{
		Released:       true,
		IdempotencyKey: "release_inventory:" + in.ReservationID,
	}, nil
}
