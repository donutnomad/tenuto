// Package shadowed 是「activity 实现名被局部声明遮蔽」的回归 fixture：重写出来的 marker
// 指向函数体内的局部函数值，翻译产物里只能是变量引用（Go 语义），生成器必须给出明确诊断。
package shadowed

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// Input 是 fixture workflow 的入参。
type Input struct {
	OrderID string `json:"order_id"`
}

// Output 是 fixture workflow 的返回值。
type Output struct {
	ChargeID string `json:"charge_id"`
}

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
}

// @Activity(type="shadowed.charge",queue="shadowed",version=v1)
func Charge(_ context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID}, nil
}

// @Workflow(type="shadowed-flow",name="Shadowed Flow",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in Input) (Output, error) {
	// 局部函数值遮蔽同名 activity 实现：Go 语义下 marker 标识符指向它，不是包级函数。
	Charge := func(input ChargeInput) ChargeOutput { return ChargeOutput{ChargeID: "local:" + input.OrderID} }
	_ = Charge
	charged, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Output{}, err
	}
	return Output{ChargeID: charged.ChargeID}, nil
}
