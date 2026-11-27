// Package ifinit 是 if 初始化语句里写闭包的差分 fixture：闭包体经桥调度 Activity。
package ifinit

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/initclosure/calc"
)

// apply 调用 f 并把结果加一。
func apply(n int, f func(int) (int, error)) (int, error) {
	v, err := f(n)
	return v + 1, err
}

// @Workflow(type="initclosure-if",name="If Init Closure",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in calc.Num) (calc.Num, error) {
	result := in.N
	if double := func(n int) (int, error) {
		out, err := calc.CallDouble(ctx, calc.Num{N: n}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out.N, err
	}; result > 0 {
		v, err := double(result)
		if err != nil {
			return calc.Num{}, err
		}
		result = v
	}
	if v, err := apply(result, func(n int) (int, error) {
		out, err := calc.CallDouble(ctx, calc.Num{N: n + 3}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out.N, err
	}); err != nil {
		return calc.Num{}, err
	} else {
		result = v
	}
	return calc.Num{N: result}, nil
}
