// Package clocktest 提供手动推进的假时钟，供测试确定性地驱动超时与定时器。
package clocktest

import (
	"sync"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
)

// Fake 只在 Advance 时前进。到期的定时器在 Advance 的调用方 goroutine 内按截止时间先后依次执行，
// 截止时间相同的按排定顺序执行；执行每个回调时 Now 等于它的截止时间。
// d <= 0 的 AfterFunc 与系统时钟一样立即在新的 goroutine 里执行。
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers map[*fakeTimer]struct{}
}

var _ clock.Clock = (*Fake)(nil)

// NewFake 返回停在 start 的假时钟。
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, timers: make(map[*fakeTimer]struct{})}
}

type fakeTimer struct {
	clock    *Fake
	deadline time.Time
	seq      uint64
	fn       func()
	done     bool
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) AfterFunc(d time.Duration, fn func()) clock.Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	timer := &fakeTimer{clock: f, deadline: f.now.Add(d), seq: f.seq, fn: fn}
	if d <= 0 {
		timer.done = true
		go fn()
		return timer
	}
	f.timers[timer] = struct{}{}
	return timer
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.done {
		return false
	}
	t.done = true
	delete(t.clock.timers, t)
	return true
}

// Advance 把时钟推进 d，依次执行截止时间不晚于新时刻的定时器，包括回调在推进期间新排定的定时器。
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()
	for {
		f.mu.Lock()
		next := f.nextDueLocked(target)
		if next == nil {
			f.now = target
			f.mu.Unlock()
			return
		}
		next.done = true
		delete(f.timers, next)
		if next.deadline.After(f.now) {
			f.now = next.deadline
		}
		f.mu.Unlock()
		next.fn()
	}
}

func (f *Fake) nextDueLocked(target time.Time) *fakeTimer {
	var next *fakeTimer
	for timer := range f.timers {
		if timer.deadline.After(target) {
			continue
		}
		if next == nil || timer.deadline.Before(next.deadline) || (timer.deadline.Equal(next.deadline) && timer.seq < next.seq) {
			next = timer
		}
	}
	return next
}

// Pending 返回尚未执行也未停止的定时器个数。
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}
