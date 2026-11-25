// Package payment 是 payment-service 的 Activity 实现：在 payments 队列上扣款。
package payment

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/donutnomad/tenuto/examples/shop/contracts"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// DeclineAbove 以上的金额（分）被模拟网关拒付。
const DeclineAbove = 100_000

// Gateway 模拟支付网关。charges 按订单号记录已完成的扣款，作为幂等表：同一订单重复扣款
// 返回第一次的结果。真实服务把它放在数据库里，用唯一索引保证。
type Gateway struct {
	// Flaky 为 true 时，每笔订单第一次扣款成功后丢失响应（返回超时），
	// 用来演示 Activity 重试与幂等。
	Flaky bool

	mu      sync.Mutex
	charges map[string]string
}

func NewGateway(flaky bool) *Gateway {
	return &Gateway{Flaky: flaky, charges: map[string]string{}}
}

// Register 把扣款 Activity 登记到 registry，契约与 contracts.ChargeRemote 一致。
func (g *Gateway) Register(registry *activity.Registry) error {
	return activity.RegisterActivityOnQueue(registry, "payments", "payments.charge", "v1", g.Charge)
}

func (g *Gateway) Charge(ctx context.Context, in contracts.ChargeInput) (contracts.ChargeOutput, error) {
	attempt := activity.GetInfo(ctx).Attempt
	if in.Amount > DeclineAbove {
		log.Printf("charge order=%s amount=%d attempt=%d: declined", in.OrderID, in.Amount, attempt)
		return contracts.ChargeOutput{}, apperror.NewNonRetryable(
			fmt.Sprintf("amount %d exceeds card limit", in.Amount), contracts.ErrPaymentDeclined, nil)
	}

	g.mu.Lock()
	paymentID, charged := g.charges[in.OrderID]
	if !charged {
		paymentID = "pay_" + in.OrderID
		g.charges[in.OrderID] = paymentID
	}
	g.mu.Unlock()

	if charged {
		log.Printf("charge order=%s amount=%d attempt=%d: already charged as %s, returning it", in.OrderID, in.Amount, attempt, paymentID)
		return contracts.ChargeOutput{PaymentID: paymentID}, nil
	}
	log.Printf("charge order=%s amount=%d attempt=%d: charged %s", in.OrderID, in.Amount, attempt, paymentID)
	if g.Flaky && attempt == 1 {
		// 钱已经扣了，响应在回程丢失。Tenuto 会重试，幂等表保证不重复扣款。
		log.Printf("charge order=%s: simulating gateway timeout after charging", in.OrderID)
		return contracts.ChargeOutput{}, errors.New("payment gateway timeout")
	}
	return contracts.ChargeOutput{PaymentID: paymentID}, nil
}
