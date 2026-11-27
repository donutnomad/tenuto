// Package forinit 是 for 初始化语句里写闭包的差分 fixture：闭包体经桥调度 Activity。
package forinit

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/compiler/sdkgen/testdata/initclosure/calc"
)

// @Workflow(type="initclosure-for",name="For Init Closure",policy=auto_upgrade,version=v1)
func Flow(ctx workflow.Context, in calc.Num) (calc.Num, error) {
	total := in.N
	for step := func(n int) (int, error) {
		out, err := calc.CallDouble(ctx, calc.Num{N: n}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return out.N, err
	}; total < 100; {
		v, err := step(total + 1)
		if err != nil {
			return calc.Num{}, err
		}
		total = v
	}
	return calc.Num{N: total}, nil
}
