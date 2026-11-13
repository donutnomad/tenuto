// Package idempotency_test 是 Activity 幂等键三种来源的示例，与文档 best-practices/activity-idempotency 同步。
package idempotency_test

import (
	"context"
	"strconv"
	"time"

	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// PaymentGateway 是下游支付服务：同一个幂等键的请求只扣一次款，重复请求返回第一次的结果。
type PaymentGateway interface {
	Charge(ctx context.Context, idempotencyKey string, amountCents int64) (string, error)
	Refund(ctx context.Context, idempotencyKey, paymentID string, amountCents int64) (string, error)
}

// Mailer 是下游邮件服务：同一个幂等键只发一封。
type Mailer interface {
	Send(ctx context.Context, idempotencyKey, to, body string) error
}

var (
	gateway PaymentGateway
	mailer  Mailer
)

// ---- 1. 输入里已有的业务 ID ----

type ChargeInstallmentInput struct {
	OrderID       string `json:"order_id"`
	InstallmentNo int    `json:"installment_no"`
	AmountCents   int64  `json:"amount_cents"`
}

type ChargeInstallmentOutput struct {
	PaymentID string `json:"payment_id"`
}

// ChargeInstallment 的幂等键是操作名加 (订单, 期数)：同一订单的每一期各是一次操作。
func ChargeInstallment(ctx context.Context, in ChargeInstallmentInput) (ChargeInstallmentOutput, error) {
	key := "charge-installment:" + in.OrderID + ":" + strconv.Itoa(in.InstallmentNo)
	paymentID, err := gateway.Charge(ctx, key, in.AmountCents)
	if err != nil {
		return ChargeInstallmentOutput{}, err
	}
	return ChargeInstallmentOutput{PaymentID: paymentID}, nil
}

// ---- 2. Workflow 生成、随输入传入的 ID ----

type RefundInput struct {
	RefundID    string `json:"refund_id"`
	PaymentID   string `json:"payment_id"`
	AmountCents int64  `json:"amount_cents"`
}

type RefundOutput struct {
	RefundReference string `json:"refund_reference"`
}

// IssueRefund 的幂等键是操作名加 Workflow 生成的退款 ID。
func IssueRefund(ctx context.Context, in RefundInput) (RefundOutput, error) {
	reference, err := gateway.Refund(ctx, "issue-refund:"+in.RefundID, in.PaymentID, in.AmountCents)
	if err != nil {
		return RefundOutput{}, err
	}
	return RefundOutput{RefundReference: reference}, nil
}

type PartialRefundsInput struct {
	PaymentID string  `json:"payment_id"`
	Amounts   []int64 `json:"amounts"`
}

// PartialRefundsWorkflow 对同一笔支付做多次部分退款，业务上没有现成的退款 ID：
// 每次退款在 Workflow 里生成一个，RandomUUID 的值记在 History 里，重放与 Activity 重试拿到同一个值。
func PartialRefundsWorkflow(ctx workflow.Context, in PartialRefundsInput) error {
	for _, amount := range in.Amounts {
		refund := RefundInput{RefundID: workflow.RandomUUID(ctx), PaymentID: in.PaymentID, AmountCents: amount}
		var out RefundOutput
		if err := workflow.ExecuteActivity(ctx, IssueRefund, refund, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		}).Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}

// ---- 3. activity.GetInfo 的 Workflow ID + Activity ID ----

type SendReceiptInput struct {
	Email string `json:"email"`
	Body  string `json:"body"`
}

type SendReceiptOutput struct{}

// SendReceipt 的幂等键是操作名加 Workflow ID 与 Activity ID：同一个被调度的 Activity 的各次重试相同。
// 只用于不会 continue-as-new、Workflow ID 不复用的 Workflow。
func SendReceipt(ctx context.Context, in SendReceiptInput) (SendReceiptOutput, error) {
	info := activity.GetInfo(ctx)
	key := "send-receipt:" + info.WorkflowID + ":" + info.ActivityID
	return SendReceiptOutput{}, mailer.Send(ctx, key, in.Email, in.Body)
}
