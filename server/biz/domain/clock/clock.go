// Package clock 定义 Server 读取当前时间与排定一次性定时器的接口。
// 业务代码只经 Clock 取时间：生产环境注入系统时钟，测试注入手动推进的假时钟（见 clocktest）。
package clock

import "time"

// Clock 提供当前时间与一次性定时器。
type Clock interface {
	Now() time.Time
	// AfterFunc 在 d 之后于独立的 goroutine 调用 f；d <= 0 时立即调度。
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer 是 AfterFunc 排定的一次性定时器。
type Timer interface {
	// Stop 阻止尚未执行的回调；回调已开始执行或定时器已停止时返回 false。
	Stop() bool
}
