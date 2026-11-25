// Package sagaflow 演示「显式逆序补偿」的 Saga 编排：Tenuto 没有 Saga 框架，
// 正向步骤与补偿步骤都写在 Workflow 函数体里，补偿按业务逆序调度。
//
// 两条语义边界（示例代码严格区分）：
//   - 业务明确拒绝：Activity 返回 nil error + 状态字段（ChargePayment.Status="declined"、
//     CreateShipment.Status="rejected"）。下游确认没有产生副作用，代码可以安全补偿。
//   - 结果不确定：Activity 返回 error（超时、进程退出、响应丢失）。下游可能已经生效，
//     代码不做自动补偿，直接返回 SagaManualReconciliation 由人工核对后再决定。
//
// 保险/库存类的幂等键是「固定操作名 + 稳定业务 ID」（OrderID/PaymentID/ReservationID）。
// activity.GetInfo(ctx) 给出的 RunID 每次 rerun/fork 都会变，只适合作日志标签。
//
// 补偿是否已执行不靠 Workflow 变量记录：变量随 history 重放重建，补偿调度由 event history
// 里的 ActivityScheduled 事件确定；Activity 侧再用业务幂等键去重。
//
// 本文件只放 Workflow 编排；Activity 实现在 activities.go，DTO 在 types.go。
package sagaflow

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// PaymentFailureDetail 是 PaymentDeclined 的结构化 detail。局部 DTO 由生成器收集 descriptor，
// Workflow 内用 workflow.ApplicationErrorDetail 按 payload 解码后直接读字段。
type PaymentFailureDetail struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// @Workflow(type="saga-order",name="Saga Order",policy=auto_upgrade,version=v1)
func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	reserved, err := CallReserveInventory(ctx, ReserveInventoryInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		// 预留结果不确定：库存可能已经被预留，但没有已完成步骤可以补偿。
		// 按 OrderID 查下游幂等键 reserve_inventory:<OrderID> 后再决定人工释放。
		return OrderOutput{}, workflow.NewApplicationError(
			"reserve inventory outcome unknown, manual reconciliation required by order_id; unknown=reserve_inventory, skipped=charge_payment create_shipment",
			"SagaManualReconciliation")
	}

	paid, err := CallChargePayment(ctx, ChargePaymentInput{
		OrderID:       in.OrderID,
		ReservationID: reserved.ReservationID,
		Amount:        in.Amount,
		Scenario:      in.Scenario,
	}, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		// 扣款可以安全重试：Activity 侧按幂等键 charge_payment:<OrderID> 先去重。
		// 临时失败（PaymentUnavailable）重试到上限；永久失败（PaymentDeclined）由
		// NonRetryable 直接跳过重试。
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    10 * time.Millisecond,
			BackoffCoefficient: 1,
			MaxInterval:        10 * time.Millisecond,
		},
	})
	if err != nil {
		// 类型化分支：Activity 的失败类型让 Workflow 能区分「下游确认未扣款」与「结果不确定」。
		// PaymentDeclined 是 NonRetryable 的永久失败，不会重试，此时可以自动补偿。
		if workflow.IsApplicationError(err, "PaymentDeclined") {
			// 结构化 detail 读取失败不阻塞补偿，但要体现在终态文案里，便于对账。
			var detail PaymentFailureDetail
			detailErr := workflow.ApplicationErrorDetail(err, 0, &detail)
			reason := "unknown"
			if detailErr == nil && detail.Reason != "" {
				reason = detail.Reason
			}
			released, releaseErr := CallReleaseInventory(ctx, ReleaseInventoryInput{
				OrderID:       in.OrderID,
				ReservationID: reserved.ReservationID,
				Reason:        "payment_declined",
			}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
			if releaseErr != nil || !released.Released {
				return OrderOutput{}, workflow.NewApplicationError(
					"payment declined then release inventory failed, manual intervention required; done=reserve_inventory, compensation_failed=release_inventory",
					"SagaCompensationFailed")
			}
			return OrderOutput{}, workflow.NewApplicationError(
				"payment declined by provider, reason="+reason+", reserved inventory released",
				"PaymentDeclined")
		} else {
			// 其余错误（临时失败重试耗尽、包装后的未知错误、旧 worker 的 untyped 错误）都表示
			// 扣款结果不确定：款可能已扣，退款和释放库存都可能与真实状态冲突，交给人工核对。
			// 原错误作为 cause 保留，临时失败的 type 仍可在终态里查到。
			return OrderOutput{}, workflow.NewApplicationErrorWithCause(
				"charge payment outcome unknown, manual reconciliation required; done=reserve_inventory, unknown=charge_payment, skipped=create_shipment refund_payment release_inventory",
				"SagaManualReconciliation", err)
		}
	}
	// 只有 charged/declined 是明确业务结论；其他状态一律按结果不确定处理。
	if paid.Status != "charged" && paid.Status != "declined" {
		return OrderOutput{}, workflow.NewApplicationError(
			"charge payment returned unknown status, manual reconciliation required; done=reserve_inventory, unknown=charge_payment, skipped=create_shipment refund_payment release_inventory",
			"SagaManualReconciliation")
	}
	if paid.Status == "declined" {
		// 明确拒绝：扣款未发生，只需释放先前预留的库存。
		released, releaseErr := CallReleaseInventory(ctx, ReleaseInventoryInput{
			OrderID:       in.OrderID,
			ReservationID: reserved.ReservationID,
			Reason:        "payment_declined",
		}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if releaseErr != nil || !released.Released {
			return OrderOutput{}, workflow.NewApplicationError(
				"payment declined then release inventory failed, manual intervention required; done=reserve_inventory, compensation_failed=release_inventory",
				"SagaCompensationFailed")
		}
		return OrderOutput{}, workflow.NewApplicationError(
			"payment declined, reserved inventory released",
			"PaymentDeclined")
	}

	shipped, err := CallCreateShipment(ctx, CreateShipmentInput{
		OrderID:       in.OrderID,
		ReservationID: reserved.ReservationID,
		PaymentID:     paid.PaymentID,
		Scenario:      in.Scenario,
	}, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		// 发货按幂等键去重，可以安全重试；有限次重试后仍失败就交给人工核对。
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    10 * time.Millisecond,
			BackoffCoefficient: 1,
			MaxInterval:        10 * time.Millisecond,
		},
	})
	if err != nil {
		// 发货结果不确定：货可能已经出库，同样交给人工核对。
		return OrderOutput{}, workflow.NewApplicationError(
			"create shipment outcome unknown, manual reconciliation required; done=reserve_inventory,charge_payment, unknown=create_shipment, skipped=refund_payment release_inventory",
			"SagaManualReconciliation")
	}
	// 只有 created/rejected 是明确业务结论；其他状态一律按结果不确定处理。
	if shipped.Status != "created" && shipped.Status != "rejected" {
		return OrderOutput{}, workflow.NewApplicationError(
			"create shipment returned unknown status, manual reconciliation required; done=reserve_inventory,charge_payment, unknown=create_shipment, skipped=refund_payment release_inventory",
			"SagaManualReconciliation")
	}
	if shipped.Status == "rejected" {
		// 明确拒绝发货：按逆序补偿，先退款再释放库存。退款失败也继续释放库存，
		// 避免库存被永久占用，最后把失败项写进一条明确的错误。
		refunded, refundErr := CallRefundPayment(ctx, RefundPaymentInput{
			OrderID:   in.OrderID,
			PaymentID: paid.PaymentID,
			Scenario:  in.Scenario,
		}, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			// 退款按 PaymentID 去重；有限次重试后仍失败就记为补偿失败，交给人工处理。
			RetryPolicy: workflow.RetryPolicy{
				MaxAttempts:        3,
				InitialInterval:    10 * time.Millisecond,
				BackoffCoefficient: 1,
				MaxInterval:        10 * time.Millisecond,
			},
		})
		released, releaseErr := CallReleaseInventory(ctx, ReleaseInventoryInput{
			OrderID:       in.OrderID,
			ReservationID: reserved.ReservationID,
			Reason:        "shipment_rejected",
		}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if refundErr != nil || !refunded.Refunded || releaseErr != nil || !released.Released {
			failed := "compensation_failed="
			if refundErr != nil || !refunded.Refunded {
				failed = failed + "refund_payment "
			}
			if releaseErr != nil || !released.Released {
				failed = failed + "release_inventory"
			}
			return OrderOutput{}, workflow.NewApplicationError(
				"shipment rejected then compensation failed, manual intervention required; "+failed,
				"SagaCompensationFailed")
		}
		return OrderOutput{}, workflow.NewApplicationError(
			"shipment rejected, compensation done=refund_payment,release_inventory",
			"ShipmentRejected")
	}

	return OrderOutput{
		ReservationID: reserved.ReservationID,
		PaymentID:     paid.PaymentID,
		ShipmentID:    shipped.ShipmentID,
	}, nil
}
