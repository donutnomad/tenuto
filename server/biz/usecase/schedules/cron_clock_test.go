package schedules_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// Happy Path: cron 按假时钟逐次到期，每个到期时刻触发一次，NextRunAt 前移到下一个 cron 时刻；
// 到期之前的 FireDue 不触发。
func TestFireDueFollowsCronAsTheClockAdvances(t *testing.T) {
	clk := clocktest.NewFake(time.Date(2026, 11, 10, 9, 1, 0, 0, time.UTC))
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	uc := schedules.NewUseCase(store, firer, clk, slog.New(slog.DiscardHandler))
	created, err := uc.Create(schedules.CreateRequest{
		WorkflowType: "wf-cron-clock", CronExpression: "*/5 * * * *", Timezone: "UTC",
		OverlapPolicy: string(schedule.OverlapPolicyEnums.AllowAll),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 11, 10, 9, 5, 0, 0, time.UTC)
	if created.NextRunAt == nil || !created.NextRunAt.Equal(first) {
		t.Fatalf("NextRunAt = %v, want %v", created.NextRunAt, first)
	}

	clk.Advance(4*time.Minute - time.Second)
	uc.FireDue(context.Background())
	if got := firer.startCallCount(); got != 0 {
		t.Fatalf("fired %d times before the first cron time", got)
	}

	for i, want := range []time.Time{first.Add(5 * time.Minute), first.Add(10 * time.Minute)} {
		clk.Advance(want.Sub(clk.Now()) - 5*time.Minute)
		uc.FireDue(context.Background())
		if got := firer.startCallCount(); got != i+1 {
			t.Fatalf("after cron time %d: start calls = %d, want %d", i+1, got, i+1)
		}
		stored, err := store.GetSchedule(created.Namespace, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.NextRunAt == nil || !stored.NextRunAt.Equal(want) {
			t.Fatalf("after cron time %d: NextRunAt = %v, want %v", i+1, stored.NextRunAt, want)
		}
	}
}
