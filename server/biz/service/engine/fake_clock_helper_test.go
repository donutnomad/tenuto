package engine

import (
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// testClockStart 是假时钟的起点，固定时刻让时间断言可以写成精确值。
var testClockStart = time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)

// newFakeClockServer 用假时钟装配 Server，测试经 clk.Advance 推进全部超时与定时器。
func newFakeClockServer(mgr execution.ExecutionManager, wfQueue, actQueue *testTaskQueue) (*Server, *clocktest.Fake) {
	clk := clocktest.NewFake(testClockStart)
	return assembleServerWithClock(mgr, wfQueue, actQueue, clk), clk
}
