package schedules_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// catchupFixture 是一个 08:30 创建的每小时 Schedule（下一个到期点 09:00），时钟可推进。
type catchupFixture struct {
	clk   *clocktest.Fake
	store *memory.ScheduleRepository
	firer *fakeFirer
	uc    *schedules.UseCase
	id    string
}

func newCatchupFixture(t *testing.T, cron, policy string, window time.Duration) *catchupFixture {
	t.Helper()
	clk := clocktest.NewFake(time.Date(2026, 11, 10, 8, 30, 0, 0, time.UTC))
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	uc := schedules.NewUseCase(store, firer, clk, slog.New(slog.DiscardHandler))
	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf", CronExpression: cron, Timezone: "UTC", OverlapPolicy: policy, CatchupWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	return &catchupFixture{clk: clk, store: store, firer: firer, uc: uc, id: created.ID}
}

func (f *catchupFixture) get(t *testing.T) *schedule.Schedule {
	t.Helper()
	got, err := f.store.GetSchedule("default", f.id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (f *catchupFixture) startedWorkflowIDs() []string {
	f.firer.mu.Lock()
	defer f.firer.mu.Unlock()
	ids := make([]string, 0, len(f.firer.startCalls))
	for _, call := range f.firer.startCalls {
		ids = append(ids, call.workflowID)
	}
	return ids
}

// 停机 90 分钟后恢复：09:00 落后 90 分钟（超出 1h 窗口）跳过并计入 missed，10:00 落后 30 分钟（窗口内）照常补触发。
// Temporal 对每个错过的到期点单独按 catchup window 判定。
func TestOutageLongerThanCatchupWindowFiresMissedTimeInsideWindow(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "", time.Hour)
	f.clk.Advance(2 * time.Hour) // 10:30
	f.uc.FireDue(context.Background())
	f.uc.FireDue(context.Background())

	ids := f.startedWorkflowIDs()
	if len(ids) != 1 || ids[0] != "schedule-"+f.id+"-2026-11-10T10:00:00Z" {
		t.Fatalf("after a 90-minute outage started %v, want only the 10:00 fire", ids)
	}
	got := f.get(t)
	if got.MissedCatchupWindow != 1 || got.BufferDropped != 0 {
		t.Fatalf("missed_catchup_window=%d buffer_dropped=%d, want 1 and 0", got.MissedCatchupWindow, got.BufferDropped)
	}
	if !got.NextRunAt.Equal(time.Date(2026, 11, 10, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("next_run_at = %v, want 11:00", got.NextRunAt)
	}
}

// ALLOW_ALL：停机期间错过的、都在窗口内的到期点逐个补触发，各自用到期点的 Workflow ID。
func TestOutageCatchupAllowAllStartsEveryMissedTime(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 0)
	f.clk.Advance(3 * time.Hour) // 11:30，错过 09:00、10:00、11:00
	f.uc.FireDue(context.Background())

	want := []string{"09:00", "10:00", "11:00"}
	ids := f.startedWorkflowIDs()
	if len(ids) != len(want) {
		t.Fatalf("started %v, want %d runs", ids, len(want))
	}
	for i, at := range want {
		if ids[i] != "schedule-"+f.id+"-2026-11-10T"+at+":00Z" {
			t.Fatalf("run %d workflow id = %s, want the %s fire", i, ids[i], at)
		}
	}
	got := f.get(t)
	if got.RunCount != 3 || got.MissedCatchupWindow != 0 || !got.NextRunAt.Equal(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("run_count=%d missed=%d next_run_at=%v, want 3, 0, 12:00", got.RunCount, got.MissedCatchupWindow, got.NextRunAt)
	}
}

// SKIP：逐个到期点按 Overlap Policy 判定。第一个补触发的 Run 仍在运行，之后错过的到期点都跳过（不计入 missed）。
func TestOutageCatchupSkipStartsOnlyWhilePreviousRunIsDone(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "SKIP", 0)
	f.firer.runStatuses["run-wf"] = history.StatusRunning
	f.clk.Advance(3 * time.Hour) // 11:30
	f.uc.FireDue(context.Background())

	ids := f.startedWorkflowIDs()
	if len(ids) != 1 || ids[0] != "schedule-"+f.id+"-2026-11-10T09:00:00Z" {
		t.Fatalf("started %v, want only the 09:00 fire while its run is still running", ids)
	}
	got := f.get(t)
	if got.RunCount != 1 || got.MissedCatchupWindow != 0 || !got.NextRunAt.Equal(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("run_count=%d missed=%d next_run_at=%v, want 1, 0, 12:00", got.RunCount, got.MissedCatchupWindow, got.NextRunAt)
	}
}

// 窗口内待补的到期点超过 MaxBufferSize 时保留最早的 MaxBufferSize 个，更晚的计入 buffer_dropped（Temporal 的 buffer 满时丢弃
// 新到的触发）；保留的分批补触发，每轮扫描最多 FireBatchSize 个，处理完后跳过丢弃的区间。
func TestOutageCatchupBufferLimitAndBatches(t *testing.T) {
	f := newCatchupFixture(t, "* * * * *", "ALLOW_ALL", 0)
	// 08:31 起每分钟一个到期点，推进到 08:30 + 1200 分钟：1200 个已到期。
	f.clk.Advance(1200 * time.Minute)
	f.uc.FireDue(context.Background())

	got := f.get(t)
	if got.BufferDropped != 200 || got.MissedCatchupWindow != 0 {
		t.Fatalf("buffer_dropped=%d missed=%d, want 200 and 0", got.BufferDropped, got.MissedCatchupWindow)
	}
	if n := len(f.startedWorkflowIDs()); n != schedules.FireBatchSize {
		t.Fatalf("first scan started %d runs, want one batch of %d", n, schedules.FireBatchSize)
	}
	if first := f.startedWorkflowIDs()[0]; first != "schedule-"+f.id+"-2026-11-10T08:31:00Z" {
		t.Fatalf("first catch-up run = %s, want the oldest fire time 08:31", first)
	}
	for range 9 {
		f.uc.FireDue(context.Background())
	}
	if n := len(f.startedWorkflowIDs()); n != schedule.MaxBufferSize {
		t.Fatalf("after 10 scans started %d runs, want %d", n, schedule.MaxBufferSize)
	}
	ids := f.startedWorkflowIDs()
	if last := ids[len(ids)-1]; last != "schedule-"+f.id+"-2026-11-11T01:10:00Z" {
		t.Fatalf("last catch-up run = %s, want the 1000th fire time 01:10", last)
	}
	// 丢弃的 01:11 至 04:30 不再触发，之后的到期点照常触发。
	f.uc.FireDue(context.Background())
	if n := len(f.startedWorkflowIDs()); n != schedule.MaxBufferSize {
		t.Fatalf("an extra scan started more runs: %d", n)
	}
	got = f.get(t)
	if got.BufferDropped != 200 || got.BufferDropFrom != nil || !got.NextRunAt.Equal(time.Date(2026, 11, 11, 4, 31, 0, 0, time.UTC)) {
		t.Fatalf("after the buffer drained: buffer_dropped=%d drop_from=%v next_run_at=%v", got.BufferDropped, got.BufferDropFrom, got.NextRunAt)
	}
	f.clk.Advance(time.Minute)
	f.uc.FireDue(context.Background())
	if ids := f.startedWorkflowIDs(); len(ids) != schedule.MaxBufferSize+1 || ids[len(ids)-1] != "schedule-"+f.id+"-2026-11-11T04:31:00Z" {
		t.Fatalf("the next fire time after the dropped range did not fire: %d runs", len(ids))
	}
}

// 手动 Trigger 不改变 NextRunAt（Temporal TriggerImmediately）：停机积压的到期点在 Trigger 之后照常补触发，不计入 missed。
func TestTriggerKeepsBacklogOfMissedFireTimes(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 0)
	f.clk.Advance(3 * time.Hour) // 11:30，积压 09:00、10:00、11:00
	before := f.get(t).NextRunAt

	_, runID, err := f.uc.Trigger(context.Background(), "default", f.id)
	if err != nil {
		t.Fatal(err)
	}
	got := f.get(t)
	if !got.NextRunAt.Equal(*before) || got.LastRunID != runID || got.RunCount != 1 {
		t.Fatalf("after trigger next_run_at=%v (want %v) last_run_id=%q run_count=%d", got.NextRunAt, before, got.LastRunID, got.RunCount)
	}

	f.uc.FireDue(context.Background())
	want := []string{"11:30", "09:00", "10:00", "11:00"}
	ids := f.startedWorkflowIDs()
	if len(ids) != len(want) {
		t.Fatalf("started %v, want the manual run and the 3 backlogged fires", ids)
	}
	for i, at := range want {
		if ids[i] != "schedule-"+f.id+"-2026-11-10T"+at+":00Z" {
			t.Fatalf("run %d workflow id = %s, want %s", i, ids[i], at)
		}
	}
	if got := f.get(t); got.MissedCatchupWindow != 0 || got.RunCount != 4 {
		t.Fatalf("missed=%d run_count=%d, want 0 and 4", got.MissedCatchupWindow, got.RunCount)
	}
}
