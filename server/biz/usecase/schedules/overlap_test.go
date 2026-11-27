package schedules_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

var overlapClockStart = time.Date(2026, 11, 10, 9, 1, 0, 0, time.UTC)

func newOverlapUseCase(t *testing.T, policy string) (*schedules.UseCase, *fakeFirer, *clocktest.Fake, *schedule.Schedule) {
	t.Helper()
	clk := clocktest.NewFake(overlapClockStart)
	firer := newFakeFirer(memory.NewScheduleRepository())
	uc := schedules.NewUseCase(firer.store, firer, clk, slog.New(slog.DiscardHandler))
	created, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf-overlap", CronExpression: "*/5 * * * *", OverlapPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return uc, firer, clk, created
}

// Error Case: 未知的 Overlap Policy 在创建与更新时返回 InvalidRequest，已保存的值不变。
func TestOverlapPolicyIsValidated(t *testing.T) {
	uc, _, _, created := newOverlapUseCase(t, "")
	if created.OverlapPolicy != "SKIP" {
		t.Fatalf("default overlap policy = %q, want SKIP", created.OverlapPolicy)
	}
	if _, err := uc.Create(schedules.CreateRequest{WorkflowType: "wf", CronExpression: "* * * * *", OverlapPolicy: "TERMINATE_OTHER"}); !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("create with unknown policy: err = %v, want InvalidRequest", err)
	}
	unknown := "allow"
	if _, err := uc.Update("default", created.ID, schedules.UpdateRequest{OverlapPolicy: &unknown}); !errors.Is(err, protocol.ErrInvalidRequest) {
		t.Fatalf("update with unknown policy: err = %v, want InvalidRequest", err)
	}
	stored, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.OverlapPolicy != "SKIP" {
		t.Fatalf("stored overlap policy = %q, want SKIP", stored.OverlapPolicy)
	}
}

// Happy Path: 到期触发的 Workflow ID 取 cron 到期点，而不是扫描到它的时刻。
func TestFireDueUsesTheDueTimeInTheWorkflowID(t *testing.T) {
	uc, firer, clk, created := newOverlapUseCase(t, "ALLOW_ALL")
	due := *created.NextRunAt
	clk.Advance(due.Sub(clk.Now()) + 30*time.Second)

	uc.FireDue(context.Background())

	if firer.startCallCount() != 1 {
		t.Fatalf("start calls = %d, want 1", firer.startCallCount())
	}
	if want := created.RunWorkflowID(due); firer.startCalls[0].workflowID != want {
		t.Fatalf("workflow id = %q, want %q", firer.startCalls[0].workflowID, want)
	}
}

// SKIP 的手动触发：上一次触发的执行链仍在运行时返回 ErrOverlapSkipped，不启动 Run，NextRunAt 不变；执行链结束后可以触发。
func TestTriggerFollowsSkipPolicy(t *testing.T) {
	ctx := context.Background()
	uc, firer, clk, created := newOverlapUseCase(t, "SKIP")
	_, firstRunID, err := uc.Trigger(ctx, "default", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	firer.runStatuses[firstRunID] = history.StatusRunning
	before, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatal(err)
	}

	clk.Advance(time.Second)
	if _, _, err := uc.Trigger(ctx, "default", created.ID); !errors.Is(err, schedule.ErrOverlapSkipped) {
		t.Fatalf("trigger while running: err = %v, want ErrOverlapSkipped", err)
	}
	after, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firer.startCallCount() != 1 || after.RunCount != 1 || !after.NextRunAt.Equal(*before.NextRunAt) {
		t.Fatalf("skipped trigger changed the schedule: start calls = %d run_count = %d next_run_at = %v", firer.startCallCount(), after.RunCount, after.NextRunAt)
	}

	firer.runStatuses[firstRunID] = history.StatusCompleted
	if _, _, err := uc.Trigger(ctx, "default", created.ID); err != nil {
		t.Fatalf("trigger after the chain finished: %v", err)
	}
}

// 同一个到期点的 Run 已在运行（上一次启动已提交、应答丢失）：这次触发算作已完成，NextRunAt 保持推进后的值，不再重试。
func TestFireDueKeepsTheDuePointWhenItsRunIsAlreadyRunning(t *testing.T) {
	uc, firer, clk, created := newOverlapUseCase(t, "ALLOW_ALL")
	due := *created.NextRunAt
	clk.Advance(due.Sub(clk.Now()))
	firer.startErr = fmt.Errorf("start: %w", execution.ErrCurrentRunConflict)

	uc.FireDue(context.Background())

	stored, err := uc.Get("default", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextRunAt == nil || !stored.NextRunAt.After(due) {
		t.Fatalf("next_run_at = %v, want advanced past %v", stored.NextRunAt, due)
	}
}
