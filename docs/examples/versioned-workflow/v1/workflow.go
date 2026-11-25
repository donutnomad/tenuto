// Package v1 是 lifecycle 示例的历史 fixture：单一路径，Reserve → Charge → Notify。
//
// 源码里直接调用生成器产出的桥 CallXxx；生成器在把源码交给 go2wlang 之前，会把
// `return activities.CallNotify(...)` 这种调用重写成 ExecuteActivity + Get。
package v1

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/activities"
)

// Input 是 lifecycle 的入参。
type Input struct {
	OrderID string `json:"order_id"`
}

// @Workflow(type="lifecycle-demo",name="Lifecycle Demo",policy=auto_upgrade,version=v1)
func Lifecycle(ctx workflow.Context, in Input) (activities.NotifyOutput, error) {
	// v1 记录 change marker（3..4）：新执行写入 4，后续版本提高 maxSupported 时旧历史读回 4。
	version := workflow.GetVersion(ctx, "charge-policy", 3, 4)
	note := "legacy"
	if version >= 4 {
		// v1 只有一条业务路径，这里保留判断是为了让 marker 值参与代码，而不是被丢弃。
		note = "legacy"
	}
	reserved, err := activities.CallReserve(ctx, activities.ReserveInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return activities.NotifyOutput{}, err
	}
	if _, err := activities.CallCharge(ctx, activities.ChargeInput{
		OrderID:       in.OrderID,
		ReservationID: reserved.ReservationID,
	}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return activities.NotifyOutput{}, err
	}
	// 末尾直接 return 桥调用：生成器补临时输出变量 + Get + return。
	return activities.CallNotify(ctx, activities.NotifyInput{OrderID: in.OrderID, Note: note}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
}
