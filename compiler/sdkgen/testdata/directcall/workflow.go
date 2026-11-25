// Package directcall 是“直调注解函数”的回归 fixture：workflow 里直接调用
// activities 包里的 @Activity 实现，而不是生成的 CallX 桥。
package directcall

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/directcall/activities"
)

// @Workflow(type="directcall-flow",name="Direct Call Flow",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in activities.ChargeInput) (activities.ChargeOutput, error) {
	// 直调本地实现（合法 Go）：生成器必须把它重写成 ExecuteActivity，
	// 不能静默内联成普通函数调用而丢失 activity 派发。
	return activities.Charge(context.Background(), in)
}
