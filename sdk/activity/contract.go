package activity

import (
	"context"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

// Handler 是本地 activity 实现的 ABI 形状：`@Activity` 标注的实现函数必须可赋值给它。
//
// 生成器为每个 activity 契约产出 `var _ activity.Handler[In, Out] = Charge` 断言，
// 实现函数签名漂移在编译期暴露，而不是等到运行期反序列化失败。
type Handler[I, O any] func(ctx context.Context, in I) (O, error)

// Caller 是生成器产出的 typed 调用桥 ABI 形状：
//
//	func CallCharge(ctx workflow.Context, in ChargeInput, opts ...workflow.ActivityOptions) (ChargeOutput, error)
//
// 桥体只有 ExecuteActivity + Get，形状固定，因此生成器用
// `var _ activity.Caller[In, Out] = CallCharge` 锁住签名。Workflow 编排源码里直接
// 调用 CallXxx；生成器在交给 go2wlang 之前把该调用重写成等价的 ExecuteActivity + Get。
type Caller[I, O any] func(ctx workflow.Context, in I, opts ...workflow.ActivityOptions) (O, error)
