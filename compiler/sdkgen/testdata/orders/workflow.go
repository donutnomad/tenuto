// Package orders 是 sdkgen 注解生成的测试 fixture。
//
// 它覆盖编排重写的全部受支持形态：同包桥（CallCharge/CallNotify）、跨包本地桥
// （inventory.CallReserve）、跨包 remote 契约（payments.CallSettle）、显式
// ActivityOptions、`out, err :=` / `out, err =` / `return` 三种语句形态，以及
// if 初始化里的 `_` 输出位。
package orders

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/orders/inventory"
	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/orders/payments"
)

// Input 是 fixture workflow 的入参。
type Input struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// Output 是 fixture workflow 的返回值。
type Output struct {
	ReservationID string `json:"reservation_id"`
	ChargeID      string `json:"charge_id"`
	Policy        string `json:"policy"`
	Receipt       string `json:"receipt"`
	Note          string `json:"note"`
}

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Amount        int64  `json:"amount"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
	Policy   string `json:"policy"`
}

// @Activity(type="orders.charge",queue="orders",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID, Policy: "legacy"}, nil
}

// @Activity(type="orders.charge-v2",queue="orders",version=v2)
func ChargeV2(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-v2-" + in.OrderID, Policy: "v2"}, nil
}

// NotifyInput 是通知入参。
type NotifyInput struct {
	OrderID string `json:"order_id"`
	Note    string `json:"note"`
}

// NotifyOutput 是通知结果。
type NotifyOutput struct {
	Note string `json:"note"`
}

// @Activity(type="orders.notify",queue="orders",version=v1)
func Notify(ctx context.Context, in NotifyInput) (NotifyOutput, error) {
	return NotifyOutput{Note: in.Note}, nil
}

// @Workflow(id="orders-demo",name="Orders Demo",policy=auto_upgrade,version=v2)
func Orders(ctx workflow.Context, in Input) (Output, error) {
	version := workflow.GetVersion(ctx, "charge-policy", 3, 6)
	reserved, err := inventory.CallReserve(ctx, inventory.ReserveInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Output{}, err
	}
	policy := "legacy"
	var charged ChargeOutput
	var settled payments.SettleOutput
	if version >= 6 {
		charged, err = CallChargeV2(ctx, ChargeInput{
			OrderID:       in.OrderID,
			ReservationID: reserved.ReservationID,
			Amount:        in.Amount,
		}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return Output{}, err
		}
		settled, err = payments.CallSettle(ctx, payments.SettleInput{OrderID: in.OrderID, Amount: in.Amount},
			workflow.ActivityOptions{StartToCloseTimeout: time.Minute, TaskQueue: "payments"})
		if err != nil {
			return Output{}, err
		}
		policy = "v2"
	} else {
		charged, err = CallCharge(ctx, ChargeInput{OrderID: in.OrderID, ReservationID: reserved.ReservationID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return Output{}, err
		}
	}
	// `_` 输出位 + if 初始化：生成器把临时变量提到 if 之前。
	if _, err := CallCash(ctx, ChargeInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return Output{}, err
	}
	notified, err := CallNotify(ctx, NotifyInput{OrderID: in.OrderID, Note: policy}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Output{}, err
	}
	return Output{
		ReservationID: reserved.ReservationID,
		ChargeID:      charged.ChargeID,
		Policy:        policy,
		Receipt:       settled.Receipt,
		Note:          notified.Note,
	}, nil
}

// Cash 是只用于覆盖 `_` 输出位的本地 activity。
//
// @Activity(type="orders.cash",queue="orders",version=v1)
func Cash(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "cash-" + in.OrderID, Policy: "cash"}, nil
}
