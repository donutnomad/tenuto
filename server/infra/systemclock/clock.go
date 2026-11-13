// Package systemclock 是 clock.Clock 的系统时钟实现。
package systemclock

import (
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
)

// Clock 读取系统时间，定时器由 time.AfterFunc 实现。
type Clock struct{}

var _ clock.Clock = Clock{}

func (Clock) Now() time.Time { return time.Now() }

func (Clock) AfterFunc(d time.Duration, f func()) clock.Timer { return time.AfterFunc(d, f) }
