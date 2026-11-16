// Command warehouse-service 在 warehouse 队列上执行预留、释放库存和发货 Activity。
//
//	TENUTO_URL=http://localhost:7400 go run ./cmd/warehouse-service
//
// 初始库存：book 100、mug 5、lamp 0。
package main

import (
	"log"

	"github.com/donutnomad/tenuto/examples/shop/internal/svc"
	"github.com/donutnomad/tenuto/examples/shop/warehouse"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	log.SetPrefix("[warehouse] ")
	log.SetFlags(log.Ltime)
	ctx, stop := svc.SignalContext()
	defer stop()

	c, err := svc.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	registry := activity.NewRegistry()
	stock := map[string]int{"book": 100, "mug": 5, "lamp": 0}
	if err := warehouse.New(stock).Register(registry); err != nil {
		log.Fatal(err)
	}
	if err := svc.RunWorker(ctx, "warehouse-service", c, worker.Options{
		Activities:          registry,
		ActivityTaskQueues:  []string{"warehouse"},
		ActivityConcurrency: 8,
		BuildID:             svc.Env("BUILD_ID", "warehouse-dev"),
	}); err != nil {
		log.Fatal(err)
	}
}
