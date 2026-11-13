package activity_test

import (
	"context"
	"fmt"
	"log"

	"github.com/donutnomad/tenuto/sdk/activity"
)

type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

type ChargeOutput struct {
	Receipt string `json:"receipt"`
}

func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{Receipt: "receipt-" + in.OrderID}, nil
}

// 手写登记一个 Activity；生成的 RegisterActivities 对包里每个 @Activity 做同样的事。
// Registry 交给 worker.Options.Activities，契约随 Worker 启动上报。
func ExampleRegisterActivity() {
	registry := activity.NewRegistry()
	if err := activity.RegisterActivity(registry, "payments.charge", "v1", Charge); err != nil {
		log.Fatal(err)
	}
	contract := registry.Contracts()[0]
	fmt.Println(registry.Types(), contract.Version)
	// Output: [payments.charge] v1
}
