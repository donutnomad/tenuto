package clocktest_test

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
)

var start = time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)

func TestAdvanceFiresDueTimersInDeadlineOrder(t *testing.T) {
	clk := clocktest.NewFake(start)
	var order []string
	var seenAt []time.Time
	record := func(name string) func() {
		return func() {
			order = append(order, name)
			seenAt = append(seenAt, clk.Now())
		}
	}
	clk.AfterFunc(3*time.Second, record("c"))
	clk.AfterFunc(time.Second, record("a"))
	clk.AfterFunc(2*time.Second, record("b1"))
	clk.AfterFunc(2*time.Second, record("b2"))
	clk.AfterFunc(5*time.Second, record("late"))

	clk.Advance(3 * time.Second)

	want := []string{"a", "b1", "b2", "c"}
	if len(order) != len(want) {
		t.Fatalf("fired %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("fired %v, want %v", order, want)
		}
	}
	// 每个回调执行时 Now 等于它的截止时间。
	if !seenAt[0].Equal(start.Add(time.Second)) || !seenAt[3].Equal(start.Add(3*time.Second)) {
		t.Fatalf("callback times %v", seenAt)
	}
	if !clk.Now().Equal(start.Add(3*time.Second)) || clk.Pending() != 1 {
		t.Fatalf("now=%v pending=%d", clk.Now(), clk.Pending())
	}
}

func TestAdvanceFiresTimersArmedByCallbacks(t *testing.T) {
	clk := clocktest.NewFake(start)
	fired := 0
	clk.AfterFunc(time.Second, func() {
		fired++
		clk.AfterFunc(time.Second, func() { fired++ })
	})
	clk.Advance(2 * time.Second)
	if fired != 2 {
		t.Fatalf("fired = %d, want 2", fired)
	}
}

func TestStoppedTimerDoesNotFire(t *testing.T) {
	clk := clocktest.NewFake(start)
	fired := false
	timer := clk.AfterFunc(time.Second, func() { fired = true })
	if !timer.Stop() {
		t.Fatal("Stop on a pending timer returned false")
	}
	if timer.Stop() {
		t.Fatal("second Stop returned true")
	}
	clk.Advance(time.Minute)
	if fired {
		t.Fatal("a stopped timer fired")
	}
}

func TestNonPositiveDelayRunsImmediately(t *testing.T) {
	clk := clocktest.NewFake(start)
	done := make(chan struct{})
	clk.AfterFunc(0, func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a due timer did not run without Advance")
	}
	if !clk.Now().Equal(start) {
		t.Fatalf("a due timer moved the clock: %v", clk.Now())
	}
}
