// Command payment-service 在 payments 队列上执行扣款 Activity。
//
//	TENUTO_URL=http://localhost:7400 go run ./cmd/payment-service
//
// PAYMENT_FLAKY=1 时每笔订单第一次扣款后模拟网关超时，演示重试与幂等。
package main

import (
	"log"

	"github.com/donutnomad/tenuto/examples/shop/internal/svc"
	"github.com/donutnomad/tenuto/examples/shop/payment"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	log.SetPrefix("[payment] ")
	log.SetFlags(log.Ltime)
	ctx, stop := svc.SignalContext()
	defer stop()

	c, err := svc.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	registry := activity.NewRegistry()
	if err := payment.NewGateway(svc.Env("PAYMENT_FLAKY", "0") == "1").Register(registry); err != nil {
		log.Fatal(err)
	}
	if err := svc.RunWorker(ctx, "payment-service", c, worker.Options{
		Activities:          registry,
		ActivityTaskQueues:  []string{"payments"},
		ActivityConcurrency: 8,
		BuildID:             svc.Env("BUILD_ID", "payment-dev"),
	}); err != nil {
		log.Fatal(err)
	}
}
