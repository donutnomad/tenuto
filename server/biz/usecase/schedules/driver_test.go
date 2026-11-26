package schedules_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/usecase/schedules"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// ============================================
// FireDue 驱动行为测试
// ============================================
// 测试策略:
// - Happy Path: ctx 退出时 FireDue 立即返回
// - Happy Path: tick 到期触发，NextRunAt 更新到未来
// - Edge Case: 未到期不触发
// - Edge Case: Paused 不触发
// - Edge Case: catchup 超窗口跳过但仍重算 NextRunAt
// - Error Case: CAS 失败不触发（幂等）
// - OverlapPolicy SKIP: 上个 run Running → 跳过
// - OverlapPolicy SKIP: 上个 run Completed → 正常起
// - OverlapPolicy SKIP: not-found → 正常起
// - OverlapPolicy SKIP: 其他 error → 保守 skip
// - OverlapPolicy ALLOW_ALL: 不检查直接起
// ============================================

// fakeFirer 是 schedule.Firer 的测试桩，支持注入 runStatus/err 和记录调用
type fakeFirer struct {
	mu            sync.Mutex
	store         schedule.Repository
	startCalls    []startCall
	startAttempts int
	startErr      error
	// commitDespiteErr 让 startErr 在启动提交之后返回（模拟提交后入队失败或应答丢失）。
	commitDespiteErr bool
	// cronRuns 是已提交的 Cron 启动：workflowID → runID，供 CronRunStarted 查询。
	cronRuns          map[string]string
	runStatuses       map[string]history.Status
	runStatusErr      error
	runStatusNotFound bool
}

type startCall struct {
	namespace    string
	workflowType string
	workflowID   string
	input        map[string]any
	attributes   map[string]string
}

func newFakeFirer(store schedule.Repository) *fakeFirer {
	return &fakeFirer{
		store:       store,
		cronRuns:    make(map[string]string),
		runStatuses: make(map[string]history.Status),
	}
}

func (f *fakeFirer) StartWorkflowForCron(_ context.Context, namespace, workflowType, workflowID string, input map[string]any, attributes map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startAttempts++
	if f.startErr != nil && !f.commitDespiteErr {
		return "", f.startErr
	}
	runID := "run-" + workflowType
	f.startCalls = append(f.startCalls, startCall{namespace: namespace, workflowType: workflowType, workflowID: workflowID, input: input, attributes: attributes})
	f.cronRuns[workflowID] = runID
	return runID, f.startErr
}

func (f *fakeFirer) CronRunStarted(_, workflowID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cronRuns[workflowID], nil
}

func (f *fakeFirer) CurrentRunStatus(runID string) (history.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runStatusNotFound {
		return "", execution.ErrRunNotFound
	}
	if f.runStatusErr != nil {
		return "", f.runStatusErr
	}
	st, ok := f.runStatuses[runID]
	if !ok {
		return history.StatusCompleted, nil
	}
	return st, nil
}

// newTestUseCase 用 firer 持有的仓储与 firer 本身装配被测用例。
func newTestUseCase(firer *fakeFirer) *schedules.UseCase {
	return schedules.NewUseCase(firer.store, firer, systemclock.Clock{}, slog.New(slog.DiscardHandler))
}

func (f *fakeFirer) startCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.startCalls)
}

// newTestSchedule creates a Schedule for testing
func newTestSchedule(id, workflowType, cron, overlapPolicy string, nextRunAt *time.Time) *schedule.Schedule {
	return &schedule.Schedule{
		ID:             id,
		Namespace:      "default",
		WorkflowType:   workflowType,
		CronExpression: cron,
		Timezone:       "UTC",
		OverlapPolicy:  overlapPolicy,
		CatchupWindow:  schedule.DefaultCatchupWindow,
		NextRunAt:      nextRunAt,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

// TestScheduleDriverTask_CtxCancelStopsTick tests ctx 退出时 FireDue 立即返回
func TestScheduleDriverTask_CtxCancelStopsTick(t *testing.T) {
	// Happy Path: ctx 退出时 Start() 立即退出
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	task := schedules.NewDriverTask(newTestUseCase(firer))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = task.Start(ctx)
	}()

	// 给少量时间让 goroutine 启动
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// 正常退出
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return after ctx cancel")
	}
}

// TestScheduleDriverTask_DueScheduleTriggered tests that a due schedule is triggered
// and NextRunAt is advanced to the future
func TestScheduleDriverTask_DueScheduleTriggered(t *testing.T) {
	// Happy Path: NextRunAt <= now 的 schedule 被触发，NextRunAt 更新到未来
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-1", "wf-1", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 1 {
		t.Fatalf("expected 1 start call, got %d", firer.startCallCount())
	}
	// 触发的 Run 带 Schedule ID 与名义触发时间（这次的到期点），与 Workflow ID 用同一个时间。
	call := firer.startCalls[0]
	fireTime := past.UTC().Truncate(time.Second).Format(time.RFC3339)
	if call.attributes[history.ScheduledByIDSearchAttribute] != "sch-1" || call.attributes[history.ScheduledStartTimeSearchAttribute] != fireTime ||
		call.workflowID != "schedule-sch-1-"+fireTime {
		t.Fatalf("start call = %+v, want schedule search attributes for fire time %s", call, fireTime)
	}

	// Verify NextRunAt was advanced to future
	got, err := store.GetSchedule("default", "sch-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt == nil {
		t.Fatal("expected NextRunAt to be set after trigger")
	}
	if !got.NextRunAt.After(time.Now().Add(-1 * time.Second)) {
		t.Fatalf("expected NextRunAt in future, got %v", *got.NextRunAt)
	}
}

// TestScheduleDriverTask_NotDueNotTriggered tests that a non-due schedule is not triggered
func TestScheduleDriverTask_NotDueNotTriggered(t *testing.T) {
	// Edge Case: NextRunAt 在未来，不应触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	future := time.Now().Add(10 * time.Minute)
	sch := newTestSchedule("sch-future", "wf-future", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &future)
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 0 {
		t.Fatalf("expected 0 start calls for future schedule, got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_PausedNotTriggered tests that a paused schedule is not triggered
func TestScheduleDriverTask_PausedNotTriggered(t *testing.T) {
	// Edge Case: Paused=true 的 schedule 不触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-paused", "wf-paused", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	sch.Paused = true
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 0 {
		t.Fatalf("expected 0 start calls for paused schedule, got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_CatchupWindowExceeded tests that over-due schedules are skipped
// but NextRunAt is still advanced to the future
func TestScheduleDriverTask_CatchupWindowExceeded(t *testing.T) {
	// Edge Case: 到期点落后超过 catchup window → 跳过触发、计入 MissedCatchupWindow，NextRunAt 推进到未来
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	// 超过 catchup 窗口 (1h)；每年一次的 cron，下一个到期点在未来。
	veryPast := time.Now().Add(-2 * time.Hour)
	sch := newTestSchedule("sch-catchup", "wf-catchup", "0 0 1 1 *", string(schedule.OverlapPolicyEnums.AllowAll), &veryPast)
	sch.CatchupWindow = time.Hour
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	// 不应该触发
	if firer.startCallCount() != 0 {
		t.Fatalf("expected 0 start calls for catchup-exceeded schedule, got %d", firer.startCallCount())
	}

	// 但 NextRunAt 应该被重算到未来
	got, err := store.GetSchedule("default", "sch-catchup")
	if err != nil {
		t.Fatal(err)
	}
	if got.NextRunAt == nil {
		t.Fatal("expected NextRunAt to be recalculated after catchup skip")
	}
	if !got.NextRunAt.After(time.Now().Add(-1 * time.Second)) {
		t.Fatalf("expected NextRunAt in future after catchup, got %v", *got.NextRunAt)
	}
	if got.MissedCatchupWindow != 1 {
		t.Fatalf("missed_catchup_window = %d, want 1", got.MissedCatchupWindow)
	}
}

// TestScheduleDriverTask_CASFailDoesNotTrigger tests CAS失败不触发（幂等）
func TestScheduleDriverTask_CASFailDoesNotRecordRun(t *testing.T) {
	// CAS 失败（另一实例已推进 NextRunAt）：启动请求按 Workflow ID 去重到同一个 Run，这个实例不记录它
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(newCASFailStore(store))

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-cas", "wf-cas", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	got, err := store.GetSchedule("default", "sch-cas")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunCount != 0 || got.LastRunID != "" {
		t.Fatalf("expected the CAS loser not to record the run, got run_count=%d last_run_id=%q", got.RunCount, got.LastRunID)
	}
}

// TestScheduleDriverTask_SKIP_RunningDoesNotTrigger tests SKIP policy: running run → skip
func TestScheduleDriverTask_SKIP_RunningDoesNotTrigger(t *testing.T) {
	// OverlapPolicy SKIP: 上一个 run 仍在 Running → 跳过
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	firer.runStatuses["run-wf-skip"] = history.StatusRunning

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-skip", "wf-skip", "* * * * *", string(schedule.OverlapPolicyEnums.Skip), &past)
	sch.LastRunID = "run-wf-skip"
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 0 {
		t.Fatalf("expected 0 start calls when last run is still running (SKIP policy), got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_SKIP_CompletedTriggers tests SKIP policy: completed run → trigger normally
func TestScheduleDriverTask_SKIP_CompletedTriggers(t *testing.T) {
	// OverlapPolicy SKIP: 上一个 run 已 Completed → 正常触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	firer.runStatuses["run-wf-completed"] = history.StatusCompleted

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-completed", "wf-completed", "* * * * *", string(schedule.OverlapPolicyEnums.Skip), &past)
	sch.LastRunID = "run-wf-completed"
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 1 {
		t.Fatalf("expected 1 start call when last run completed (SKIP policy), got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_SKIP_NotFoundTriggers tests not-found → trigger normally
func TestScheduleDriverTask_SKIP_NotFoundTriggers(t *testing.T) {
	// OverlapPolicy SKIP: CurrentRunStatus 返回 ErrRunNotFound → 视为已完成，正常触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	firer.runStatusNotFound = true

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-notfound", "wf-notfound", "* * * * *", string(schedule.OverlapPolicyEnums.Skip), &past)
	sch.LastRunID = "run-old"
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 1 {
		t.Fatalf("expected 1 start call when run not found, got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_SKIP_OtherErrorSkips tests other error → conservative skip
func TestScheduleDriverTask_SKIP_OtherErrorSkips(t *testing.T) {
	// OverlapPolicy SKIP: CurrentRunStatus 返回其他 error → 保守 skip
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	firer.runStatusErr = errors.New("database connection error")

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-err", "wf-err", "* * * * *", string(schedule.OverlapPolicyEnums.Skip), &past)
	sch.LastRunID = "run-old"
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 0 {
		t.Fatalf("expected 0 start calls when CurrentRunStatus returns other error, got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_ALLOW_ALL_Triggers tests ALLOW_ALL: always trigger without status check
func TestScheduleDriverTask_ALLOW_ALL_Triggers(t *testing.T) {
	// OverlapPolicy ALLOW_ALL: 不检查 lastRun 状态，直接触发
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)
	firer.runStatuses["run-last"] = history.StatusRunning // running 但不阻塞 ALLOW_ALL

	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	sch := newTestSchedule("sch-allow", "wf-allow", "* * * * *", string(schedule.OverlapPolicyEnums.AllowAll), &past)
	sch.LastRunID = "run-last"
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	uc := newTestUseCase(firer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uc.FireDue(ctx)

	if firer.startCallCount() != 1 {
		t.Fatalf("expected 1 start call for ALLOW_ALL policy, got %d", firer.startCallCount())
	}
}

// TestScheduleDriverTask_CtxCancelInLoop tests loop内的ctx.Err()检查
func TestScheduleDriverTask_CtxCancelInLoop(t *testing.T) {
	// Happy Path: 循环内 ctx.Err() 检查—— ctx 在遍历中途取消时立即停止
	store := memory.NewScheduleRepository()
	firer := newFakeFirer(store)

	// 添加多个到期 schedule，确保循环有机会检查 ctx
	past := time.Now().Truncate(time.Minute) // 与 cron 对齐，下一个到期点在未来
	for i := range 3 {
		sch := newTestSchedule(
			"sch-loop-"+string(rune('a'+i)),
			"wf-loop",
			"* * * * *",
			string(schedule.OverlapPolicyEnums.AllowAll),
			&past,
		)
		if err := store.CreateSchedule(sch); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	uc := newTestUseCase(firer)
	uc.FireDue(ctx) // 应立即返回，不触发任何 schedule
	// 不 assert 触发数，仅确保不 panic 且能返回
}

// ────────────────────────────────────────────────────────────
// casFailStore: 包装内存仓储，UpdateScheduleNextRunAtCAS 始终返回 (false, nil)
// ────────────────────────────────────────────────────────────

type casFailStore struct {
	schedule.Repository
}

func newCASFailStore(s schedule.Repository) *casFailStore {
	return &casFailStore{Repository: s}
}

func (c *casFailStore) UpdateScheduleNextRunAtCAS(_ *schedule.Schedule, _ *time.Time) (bool, error) {
	return false, nil
}

func (c *casFailStore) ListDueSchedules(now time.Time) ([]*schedule.Schedule, error) {
	return c.Repository.ListDueSchedules(now)
}
