package helpers

import (
	"fmt"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func money(cents int) string {
	return fmt.Sprintf("%d.%02d", cents/100, cents%100)
}

// charge 经生成的桥调度 Activity。
func charge(ctx workflow.Context, orderID string, cents int) (string, error) {
	out, err := CallCharge(ctx, ChargeInput{OrderID: orderID, Cents: cents}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return "", err
	}
	return out.ChargeID, nil
}
