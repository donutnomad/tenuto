// Package executeactivity 是「直接写 workflow.ExecuteActivity」的 fixture：被调函数带
// @Activity 注解时，直接调用与生成的 CallX 桥必须产出同一份 activity ref。
package executeactivity

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/executeactivity/activities"
)

// NotifyInput 是通知入参。
type NotifyInput struct {
	OrderID string `json:"order_id"`
}

// NotifyOutput 是通知结果。
type NotifyOutput struct {
	Sent bool `json:"sent"`
}

// @Activity(type="orders.notify",queue="notifications")
func Notify(ctx context.Context, in NotifyInput) (NotifyOutput, error) {
	return NotifyOutput{Sent: true}, nil
}

// @Workflow(type="executeactivity-flow",name="Execute Activity Flow",policy=pinned,version=v1)
func Flow(ctx workflow.Context, in activities.ChargeInput) (activities.ChargeOutput, error) {
	var charged activities.ChargeOutput
	if err := workflow.ExecuteActivity(ctx, activities.Charge, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &charged); err != nil {
		return activities.ChargeOutput{}, err
	}
	var notified NotifyOutput
	if err := workflow.ExecuteActivity(ctx, Notify, NotifyInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notified); err != nil {
		return activities.ChargeOutput{}, err
	}
	var audited map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Audit, map[string]any{"order_id": in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &audited); err != nil {
		return activities.ChargeOutput{}, err
	}
	return charged, nil
}
