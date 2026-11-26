package schedules_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

func (f *catchupFixture) setStartErr(err error) {
	f.firer.mu.Lock()
	defer f.firer.mu.Unlock()
	f.firer.startErr = err
}

func (f *catchupFixture) startAttempts() int {
	f.firer.mu.Lock()
	defer f.firer.mu.Unlock()
	return f.firer.startAttempts
}

// scanEvery 按 step 推进时钟并扫描，直到推进了 total；每轮之后调用 check（可为 nil）。
func (f *catchupFixture) scanEvery(step, total time.Duration, check func()) {
	for elapsed := time.Duration(0); elapsed < total; elapsed += step {
		f.clk.Advance(step)
		f.uc.FireDue(context.Background())
		if check != nil {
			check()
		}
	}
}

func (f *catchupFixture) advanceTo(target time.Time) {
	f.clk.Advance(target.Sub(f.clk.Now()))
}

func at(hour, minute, second int) time.Time {
	return time.Date(2026, 11, 10, hour, minute, second, 0, time.UTC)
}

// 暂时性启动错误按退避重试（1s、2s、4s…），NextRunAt 保持为到期点；退避期间的扫描不尝试启动；恢复后启动这个到期点。
func TestTransientStartFailureRetriesWithBackoff(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 0)
	f.setStartErr(errors.New("store temporarily unavailable"))
	f.advanceTo(at(9, 0, 0))

	f.uc.FireDue(context.Background())
	f.uc.FireDue(context.Background())
	if n := f.startAttempts(); n != 1 {
		t.Fatalf("attempts right after the first failure = %d, want 1 (the second scan is inside the 1s backoff)", n)
	}
	f.clk.Advance(time.Second) // 09:00:01，第二次
	f.uc.FireDue(context.Background())
	f.clk.Advance(time.Second) // 09:00:02，退避 2s 之内
	f.uc.FireDue(context.Background())
	if n := f.startAttempts(); n != 2 {
		t.Fatalf("attempts at 09:00:02 = %d, want 2", n)
	}
	f.clk.Advance(time.Second) // 09:00:03，第三次
	f.uc.FireDue(context.Background())
	if n := f.startAttempts(); n != 3 {
		t.Fatalf("attempts at 09:00:03 = %d, want 3", n)
	}
	if got := f.get(t); !got.NextRunAt.Equal(at(9, 0, 0)) || got.StartFailed != 0 {
		t.Fatalf("while retrying next_run_at=%v start_failed=%d, want 09:00 and 0", got.NextRunAt, got.StartFailed)
	}

	f.setStartErr(nil)
	f.clk.Advance(4 * time.Second)
	f.uc.FireDue(context.Background())
	ids := f.startedWorkflowIDs()
	if len(ids) != 1 || ids[0] != "schedule-"+f.id+"-2026-11-10T09:00:00Z" {
		t.Fatalf("after recovery started %v, want the 09:00 fire", ids)
	}
	if got := f.get(t); got.RunCount != 1 || got.StartFailed != 0 || !got.NextRunAt.Equal(at(10, 0, 0)) {
		t.Fatalf("after recovery run_count=%d start_failed=%d next_run_at=%v, want 1, 0, 10:00", got.RunCount, got.StartFailed, got.NextRunAt)
	}
}

// 启动持续失败三天（每 30 秒扫描一次，缺省 365 天的 catchup window）：每个到期点最多重试到到期之后 1 小时就丢弃并计入
// start_failed，NextRunAt 落后 now 不超过 1 小时；恢复后只补出 1 小时内的到期点，之后每小时照常触发。
func TestPersistentStartFailureDropsEachFireTimeAfterAnHour(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 0)
	f.setStartErr(errors.New("workflow store shard unavailable"))

	var maxLag time.Duration
	f.scanEvery(30*time.Second, 72*time.Hour, func() {
		if lag := f.clk.Now().Sub(*f.get(t).NextRunAt); lag > maxLag {
			maxLag = lag
		}
	})
	if maxLag > time.Hour {
		t.Fatalf("next_run_at fell %s behind now while starts failed, want at most 1h", maxLag)
	}
	got := f.get(t)
	// 08:30 + 72h = 11-13 08:30：09:00 起到 11-13 07:00 的 71 个到期点过了 1 小时期限，08:00 仍在重试。
	if got.StartFailed != 71 || got.MissedCatchupWindow != 0 || !got.NextRunAt.Equal(time.Date(2026, 11, 13, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("start_failed=%d missed=%d next_run_at=%v, want 71, 0, 11-13 08:00", got.StartFailed, got.MissedCatchupWindow, got.NextRunAt)
	}

	f.setStartErr(nil)
	f.scanEvery(30*time.Second, 5*time.Minute, nil)
	ids := f.startedWorkflowIDs()
	if len(ids) != 1 || ids[0] != "schedule-"+f.id+"-2026-11-13T08:00:00Z" {
		t.Fatalf("right after recovery started %v, want only the 08:00 fire still inside its retry hour", ids)
	}
	f.scanEvery(30*time.Second, 2*time.Hour, nil)
	if n := len(f.startedWorkflowIDs()); n != 3 {
		t.Fatalf("two hours after recovery started %d runs, want 3 (08:00, 09:00, 10:00)", n)
	}
	if got := f.get(t); got.StartFailed != 71 || !got.NextRunAt.Equal(time.Date(2026, 11, 13, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("after recovery start_failed=%d next_run_at=%v, want 71 and 11:00", got.StartFailed, got.NextRunAt)
	}
}

// 每分钟一次的 Schedule 启动持续失败三小时：期限从到期点起算，积压不超过 1 小时内的到期点。恢复后 ALLOW_ALL 只补出这
// 1 小时内的到期点，SKIP 在补出的 Run 仍在运行时跳过其余的。
func TestPersistentStartFailureBacklogIsBoundedToTheRetryHour(t *testing.T) {
	for _, tc := range []struct {
		policy      string
		maxRecovery int
	}{
		{"ALLOW_ALL", 61},
		{"SKIP", 1},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			f := newCatchupFixture(t, "* * * * *", tc.policy, 0)
			f.firer.runStatuses["run-wf"] = history.StatusRunning
			f.setStartErr(errors.New("history store timeout"))
			var maxLag time.Duration
			f.scanEvery(10*time.Second, 3*time.Hour, func() {
				if lag := f.clk.Now().Sub(*f.get(t).NextRunAt); lag > maxLag {
					maxLag = lag
				}
			})
			if maxLag > time.Hour {
				t.Fatalf("next_run_at fell %s behind now, want at most 1h", maxLag)
			}
			if got := f.get(t); got.StartFailed < 115 {
				t.Fatalf("start_failed=%d after 3h of failures, want the fire times older than 1h dropped", got.StartFailed)
			}

			f.setStartErr(nil)
			f.uc.FireDue(context.Background())
			f.clk.Advance(time.Minute) // 退避最长 60s：此后所有到期点都可尝试
			f.uc.FireDue(context.Background())
			if n := len(f.startedWorkflowIDs()); n == 0 || n > tc.maxRecovery {
				t.Fatalf("recovery started %d runs, want between 1 and %d", n, tc.maxRecovery)
			}
			if got := f.get(t); got.NextRunAt.Before(f.clk.Now()) {
				t.Fatalf("after recovery next_run_at=%v is still behind now %v", got.NextRunAt, f.clk.Now())
			}
		})
	}
}

// 配置错误（Workflow 类型没有部署、请求不合法）不重试：到期点立即丢弃并计入 start_failed，后续到期点照常处理。
func TestConfigStartErrorDropsFireTimeWithoutRetry(t *testing.T) {
	for _, startErr := range []error{
		fmt.Errorf("workflow default/wf has no registered config version: %w", definition.ErrWorkflowConfigNotFound),
		protocol.InvalidRequestf("workflow input is too large"),
	} {
		t.Run(startErr.Error(), func(t *testing.T) {
			f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 0)
			f.setStartErr(startErr)
			f.advanceTo(at(9, 0, 0))
			f.uc.FireDue(context.Background())
			if got := f.get(t); f.startAttempts() != 1 || got.StartFailed != 1 || !got.NextRunAt.Equal(at(10, 0, 0)) {
				t.Fatalf("attempts=%d start_failed=%d next_run_at=%v, want 1, 1, 10:00", f.startAttempts(), got.StartFailed, got.NextRunAt)
			}

			// 停机三小时后恢复：09:30 之后的 10:00、11:00、12:00 各尝试一次、各丢弃一次，不重试。
			f.advanceTo(at(12, 30, 0))
			f.uc.FireDue(context.Background())
			f.uc.FireDue(context.Background())
			if got := f.get(t); f.startAttempts() != 4 || got.StartFailed != 4 || !got.NextRunAt.Equal(at(13, 0, 0)) {
				t.Fatalf("attempts=%d start_failed=%d next_run_at=%v, want 4, 4, 13:00", f.startAttempts(), got.StartFailed, got.NextRunAt)
			}

			f.setStartErr(nil)
			f.advanceTo(at(13, 0, 0))
			f.uc.FireDue(context.Background())
			if ids := f.startedWorkflowIDs(); len(ids) != 1 || ids[0] != "schedule-"+f.id+"-2026-11-10T13:00:00Z" {
				t.Fatalf("after the fix started %v, want only the 13:00 fire", ids)
			}
		})
	}
}

// 重试期间每轮扫描照常检查 catchup window：窗口（30 秒）比重试期限短时，到期点超出窗口后计入 missed_catchup_window，
// 不计入 start_failed，NextRunAt 推进到下一个到期点。
func TestStartRetryStopsAtCatchupWindow(t *testing.T) {
	f := newCatchupFixture(t, "0 * * * *", "ALLOW_ALL", 30*time.Second)
	f.setStartErr(errors.New("store temporarily unavailable"))
	f.advanceTo(at(9, 0, 0))
	f.scanEvery(time.Second, 40*time.Second, nil)

	got := f.get(t)
	if got.MissedCatchupWindow != 1 || got.StartFailed != 0 || !got.NextRunAt.Equal(at(10, 0, 0)) {
		t.Fatalf("missed=%d start_failed=%d next_run_at=%v, want 1, 0, 10:00", got.MissedCatchupWindow, got.StartFailed, got.NextRunAt)
	}
	if n := f.startAttempts(); n < 2 || n > 6 {
		t.Fatalf("attempts within the 30s window = %d, want a few backoff retries", n)
	}
}
