// Package switchinit 是 switch 初始化语句里写闭包的差分 fixture：闭包体经桥调度 Activity。
package switchinit

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/initclosure/calc"
)

// @Workflow(type="initclosure-switch",name="Switch Init Closure",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in calc.Num) (calc.Num, error) {
	switch double := func(n int) (int, error) {
		out, err := calc.CallDouble(ctx, calc.Num{N: n}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out.N, err
	}; {
	case in.N%2 == 0:
		v, err := double(in.N)
		if err != nil {
			return calc.Num{}, err
		}
		return calc.Num{N: v}, nil
	default:
		v, err := double(in.N + 10)
		if err != nil {
			return calc.Num{}, err
		}
		return calc.Num{N: v + 1}, nil
	}
}
