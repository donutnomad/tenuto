package batches_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/batches"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 批量操作用例测试（假时钟 + 内存仓储）
// ============================================
// 测试策略:
// - Happy Path: Start 只保存 pending 记录；驱动处理多于数页的 Run，全部处理、计数准确、completed
// - 崩溃恢复: 第一任驱动在一页中途停止，新的驱动实例从游标继续，总数准确，重复执行只发生在未保存的那一页
// - 取消: 处理中途取消，驱动在下一页之前停下，取消期间处理完的那一页仍计入
// - Reset: 选择以批量操作的创建时间为上界，Reset 产生的新 Run 不被处理，处理必然结束；恢复后已 Reset 的 Run 跳过
// - 失败: 逐 Run 失败计数，错误样本有上限；全部失败为 failed
// - 两个驱动: 同时处理同一个批量操作时，保存进度的 version 冲突让落后的一方停下，计数不重复
// - Error Case: 请求校验失败不落记录
//
// 真实取消、终止与 Reset 的端到端行为在 engine 的 workflow_runtime_batch*_test.go。
// ============================================

var start = time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)

type fixture struct {
	clock   *clocktest.Fake
	runs    *memory.ExecutionManager
	repo    *memory.BatchRepository
	actions *fakeActions
	useCase *batches.UseCase
	runIDs  []string
}

// newFixture 建 n 个 running 的 Run，创建时间各不相同，返回之前把时钟推到它们之后。
func newFixture(t *testing.T, n int) *fixture {
	t.Helper()
	f := &fixture{clock: clocktest.NewFake(start), runs: memory.NewExecutionManager(), repo: memory.NewBatchRepository()}
	f.actions = &fakeActions{f: f, calls: map[string]int{}}
	for i := range n {
		f.runIDs = append(f.runIDs, f.createRun(t, fmt.Sprintf("run-%04d", i), start.Add(time.Duration(i)*time.Millisecond)))
	}
	f.clock.Advance(time.Hour)
	f.useCase = f.newUseCase()
	return f
}

// newUseCase 是同一组仓储上的一个新驱动实例，如同重启后或另一个副本上的驱动。
func (f *fixture) newUseCase() *batches.UseCase {
	return batches.NewUseCase(f.repo, f.runs, f.actions, f.clock, slog.New(slog.DiscardHandler))
}

func (f *fixture) createRun(t *testing.T, id string, createdAt time.Time) string {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{ID: id, Namespace: protocol.DefaultNamespace, WorkflowID: "wf-" + id, WorkflowConfigVersion: "v1", CreatedAt: createdAt})
	if err := f.runs.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *fixture) get(t *testing.T, op *batch.Operation) *batch.Operation {
	t.Helper()
	got, err := f.useCase.Get(op.Namespace, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// fakeActions 记录每个 Run 实际生效的动作次数。hook 在每次动作之前调用，可取消驱动的 ctx、取消批量操作或运行另一个驱动；
// ctx 已取消时动作不生效并返回 ctx 的错误，如同进程在这一刻停下。
type fakeActions struct {
	f     *fixture
	mu    sync.Mutex
	calls map[string]int
	total int
	fail  func(runID string) error
	hook  func(ctx context.Context, call int)
	t     *testing.T
}

func (a *fakeActions) act(ctx context.Context, runID string) error {
	a.mu.Lock()
	a.total++
	call, hook := a.total, a.hook
	a.mu.Unlock()
	if hook != nil {
		hook(ctx, call)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.fail != nil {
		if err := a.fail(runID); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.calls[runID]++
	a.mu.Unlock()
	return nil
}

func (a *fakeActions) CancelWorkflow(ctx context.Context, _, _, runID, _ string) error {
	return a.act(ctx, runID)
}

func (a *fakeActions) TerminateWorkflow(ctx context.Context, _, _, runID, _ string) error {
	return a.act(ctx, runID)
}

// ResetWorkflow 像真实的 Reset 一样产生一个新 Run（同样匹配过滤条件），并把它记为源 Run 的 ResetRunID。
func (a *fakeActions) ResetWorkflow(ctx context.Context, req execution.ResetWorkflowRequest) (string, error) {
	if err := a.act(ctx, req.RunID); err != nil {
		return "", err
	}
	newID := fmt.Sprintf("%s-reset-%d", req.RunID, a.calls[req.RunID])
	a.f.createRun(a.t, newID, a.f.clock.Now())
	if err := a.f.runs.SetResetRunID(req.RunID, newID); err != nil {
		return "", err
	}
	return newID, nil
}

func (a *fakeActions) callCounts() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maps(a.calls)
}

func maps(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func startTerminate(t *testing.T, f *fixture) *batch.Operation {
	t.Helper()
	op, err := f.useCase.Start(batches.StartRequest{Type: batch.TypeEnums.Terminate, Reason: "cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func TestStartOnlyRecordsPendingOperation(t *testing.T) {
	// Happy Path: Start 立即返回 pending 记录，不执行任何动作
	f := newFixture(t, 3)
	op := startTerminate(t, f)
	if op.ID == "" || op.Status != batch.StatusEnums.Pending || op.Namespace != "default" || !op.CreatedAt.Equal(f.clock.Now()) {
		t.Fatalf("started = %+v", op)
	}
	if f.actions.total != 0 {
		t.Fatalf("Start executed %d actions, want 0", f.actions.total)
	}
	if got := f.get(t, op); got.Status != batch.StatusEnums.Pending {
		t.Fatalf("stored = %+v", got)
	}
}

func TestStartRejectsInvalidRequestsWithoutRecording(t *testing.T) {
	// Error Case: 未知类型、Reset 缺 reset_type 返回 InvalidRequest，不落记录
	f := newFixture(t, 0)
	for _, req := range []batches.StartRequest{{Type: "pause"}, {Type: batch.TypeEnums.Reset}} {
		if _, err := f.useCase.Start(req); !errors.Is(err, protocol.ErrInvalidRequest) {
			t.Errorf("Start(%+v): err = %v, want InvalidRequest", req, err)
		}
	}
	if list, err := f.useCase.List(batches.ListFilter{}); err != nil || list.Total != 0 {
		t.Fatalf("list = %+v, err = %v", list, err)
	}
}

func TestDriverProcessesEveryPage(t *testing.T) {
	// Happy Path: 2.5 页的 Run 全部处理一次，计数、总数与时间戳准确
	n := batches.PageSize*2 + batches.PageSize/2
	f := newFixture(t, n)
	op := startTerminate(t, f)
	f.clock.Advance(time.Second)

	f.useCase.ProcessActive(context.Background())

	got := f.get(t, op)
	if got.Status != batch.StatusEnums.Completed || got.TotalCount != n || got.SucceededCount != n || got.FailedCount != 0 || got.SkippedCount != 0 {
		t.Fatalf("finished = %+v", got)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(f.clock.Now()) || got.FinishedAt == nil || !got.FinishedAt.Equal(f.clock.Now()) {
		t.Fatalf("timestamps: started %v finished %v, want the fake clock's now", got.StartedAt, got.FinishedAt)
	}
	calls := f.actions.callCounts()
	for _, id := range f.runIDs {
		if calls[id] != 1 {
			t.Fatalf("run %s acted on %d times, want 1", id, calls[id])
		}
	}
	// 结束后不再处理。
	f.useCase.ProcessActive(context.Background())
	if f.actions.total != n {
		t.Fatalf("a finished batch was processed again: %d actions", f.actions.total)
	}
}

func TestDriverResumesFromCursorAfterCrash(t *testing.T) {
	// 崩溃恢复: 第一任驱动在第二页中途停下；新驱动从第一页之后继续，只重做第二页
	n := batches.PageSize * 3
	f := newFixture(t, n)
	op := startTerminate(t, f)
	ctx, crash := context.WithCancel(context.Background())
	crashAt := batches.PageSize + batches.PageSize/2
	f.actions.hook = func(_ context.Context, call int) {
		if call == crashAt {
			crash()
		}
	}
	f.useCase.ProcessActive(ctx)

	mid := f.get(t, op)
	if mid.Status != batch.StatusEnums.Running || mid.SucceededCount != batches.PageSize || mid.Cursor == nil || mid.Cursor.RunID != f.runIDs[batches.PageSize-1] {
		t.Fatalf("after the crash = %+v (cursor %+v), want running with the first page saved", mid, mid.Cursor)
	}
	applied := 0
	for _, count := range f.actions.callCounts() {
		applied += count
	}
	if applied != crashAt-1 || f.actions.total != 2*batches.PageSize {
		t.Fatalf("the crashed driver applied %d actions over %d attempts, want %d applied and the third page untouched", applied, f.actions.total, crashAt-1)
	}

	f.actions.hook = nil
	f.newUseCase().ProcessActive(context.Background())

	got := f.get(t, op)
	if got.Status != batch.StatusEnums.Completed || got.TotalCount != n || got.SucceededCount != n {
		t.Fatalf("after resume = %+v, want completed with exact totals", got)
	}
	calls := f.actions.callCounts()
	for i, id := range f.runIDs {
		want := 1
		// 只有未保存的第二页里、崩溃前已生效的 Run 被重复执行。
		if i >= batches.PageSize && i < crashAt-1 {
			want = 2
		}
		if calls[id] != want {
			t.Fatalf("run %d (%s) acted on %d times, want %d", i, id, calls[id], want)
		}
	}
}

func TestCancelStopsBeforeNextPage(t *testing.T) {
	// 取消: 第二页处理期间取消；这一页仍计入已取消的记录，第三页不处理
	n := batches.PageSize * 3
	f := newFixture(t, n)
	op := startTerminate(t, f)
	f.actions.hook = func(_ context.Context, call int) {
		if call == batches.PageSize+20 {
			f.clock.Advance(time.Minute)
			if _, err := f.useCase.Cancel(op.Namespace, op.ID); err != nil {
				t.Error(err)
			}
		}
	}
	f.useCase.ProcessActive(context.Background())

	got := f.get(t, op)
	if got.Status != batch.StatusEnums.Cancelled || got.SucceededCount != 2*batches.PageSize || got.FinishedAt == nil || !got.FinishedAt.Equal(f.clock.Now()) {
		t.Fatalf("cancelled = %+v, want two pages counted and the cancel time", got)
	}
	if f.actions.total != 2*batches.PageSize {
		t.Fatalf("%d actions, want %d: the page after the cancel must not run", f.actions.total, 2*batches.PageSize)
	}
	f.useCase.ProcessActive(context.Background())
	if f.actions.total != 2*batches.PageSize {
		t.Fatal("a cancelled batch was processed again")
	}

	// 重复取消返回已取消的记录；取消已结束的批量操作是非法迁移。
	if again, err := f.useCase.Cancel(op.Namespace, op.ID); err != nil || again.Status != batch.StatusEnums.Cancelled {
		t.Fatalf("second cancel: %+v, %v", again, err)
	}
	done, err := f.useCase.Start(batches.StartRequest{Type: batch.TypeEnums.Cancel, Filter: execution.RunListFilter{WorkflowID: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	f.useCase.ProcessActive(context.Background())
	if _, err := f.useCase.Cancel(done.Namespace, done.ID); !errors.Is(err, batch.ErrInvalidTransition) {
		t.Fatalf("cancel a completed batch: err = %v, want ErrInvalidTransition", err)
	}
}

func TestCancelPendingOperation(t *testing.T) {
	// 取消: pending 的批量操作取消后驱动不处理任何 Run
	f := newFixture(t, 5)
	op := startTerminate(t, f)
	if _, err := f.useCase.Cancel(op.Namespace, op.ID); err != nil {
		t.Fatal(err)
	}
	f.useCase.ProcessActive(context.Background())
	if got := f.get(t, op); got.Status != batch.StatusEnums.Cancelled || got.Processed() != 0 || f.actions.total != 0 {
		t.Fatalf("got %+v with %d actions", got, f.actions.total)
	}
}

func TestResetTerminatesAndSkipsRunsItCreated(t *testing.T) {
	// Reset: 每次 Reset 产生一个同样匹配过滤条件的新 Run；新 Run 不在选择之内，处理结束，每个源 Run 恰好 Reset 一次
	n := batches.PageSize + 30
	f := newFixture(t, n)
	f.actions.t = t
	op, err := f.useCase.Start(batches.StartRequest{Type: batch.TypeEnums.Reset, ResetType: execution.ResetTypeEnums.LastWorkflowTask})
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Second)
	// 第一任驱动在第二页中途停下：恢复后，已经 Reset 过的 Run 跳过而不是再 Reset 一次。
	ctx, crash := context.WithCancel(context.Background())
	crashAt := batches.PageSize + 10
	f.actions.hook = func(_ context.Context, call int) {
		if call == crashAt {
			crash()
		}
	}
	f.useCase.ProcessActive(ctx)
	f.actions.hook = nil
	f.newUseCase().ProcessActive(context.Background())

	got := f.get(t, op)
	resetBeforeCrash := crashAt - 1 - batches.PageSize
	if got.Status != batch.StatusEnums.Completed || got.TotalCount != n || got.SucceededCount != n-resetBeforeCrash || got.SkippedCount != resetBeforeCrash {
		t.Fatalf("reset batch = %+v, want %d reset and %d skipped as already reset", got, n-resetBeforeCrash, resetBeforeCrash)
	}
	calls := f.actions.callCounts()
	if len(calls) != n {
		t.Fatalf("%d distinct runs reset, want %d: runs created by the batch must not be reset", len(calls), n)
	}
	for _, id := range f.runIDs {
		if calls[id] != 1 {
			t.Fatalf("run %s reset %d times, want exactly 1", id, calls[id])
		}
	}
	all, err := f.runs.List(execution.StoreListFilter{Limit: 10 * n})
	if err != nil {
		t.Fatal(err)
	}
	if all.Total != 2*n {
		t.Fatalf("%d runs exist, want %d sources + %d resets", all.Total, n, n)
	}
}

func TestSelectionExcludesRunsCreatedAfterStart(t *testing.T) {
	// 选择以创建时间为上界：Start 之后创建的 Run 不处理
	f := newFixture(t, 3)
	op := startTerminate(t, f)
	f.clock.Advance(time.Millisecond)
	late := f.createRun(t, "run-late", f.clock.Now())
	f.useCase.ProcessActive(context.Background())
	got := f.get(t, op)
	if got.TotalCount != 3 || f.actions.callCounts()[late] != 0 {
		t.Fatalf("batch = %+v, late run acted on %d times", got, f.actions.callCounts()[late])
	}
}

func TestPerRunFailuresAreCountedWithBoundedSamples(t *testing.T) {
	// 失败: 一半 Run 失败，错误样本截在上限，批量操作仍 completed；全部失败为 failed
	n := batches.PageSize + 10
	f := newFixture(t, n)
	failing := map[string]bool{}
	for i, id := range f.runIDs {
		if i%2 == 1 {
			failing[id] = true
		}
	}
	f.actions.fail = func(runID string) error {
		if failing[runID] {
			return fmt.Errorf("workflow %s is busy", runID)
		}
		return nil
	}
	op := startTerminate(t, f)
	f.useCase.ProcessActive(context.Background())
	got := f.get(t, op)
	if got.Status != batch.StatusEnums.Completed || got.FailedCount != n/2 || got.SucceededCount != n/2 || len(got.Errors) != batch.MaxErrorSamples {
		t.Fatalf("partial failure = %+v (%d samples)", got, len(got.Errors))
	}
	if got.Errors[0].RunID != f.runIDs[1] || got.Errors[0].Message != "workflow run-0001 is busy" {
		t.Fatalf("first sample = %+v", got.Errors[0])
	}

	f.actions.fail = func(string) error { return errors.New("boom") }
	allFail, err := f.useCase.Start(batches.StartRequest{Type: batch.TypeEnums.Cancel})
	if err != nil {
		t.Fatal(err)
	}
	f.useCase.ProcessActive(context.Background())
	if got := f.get(t, allFail); got.Status != batch.StatusEnums.Failed || got.FailedCount != n {
		t.Fatalf("all failed = %+v", got)
	}
}

func TestTwoDriversDoNotDoubleCount(t *testing.T) {
	// 两个驱动: A 处理第一页期间 B 处理完整个批量操作；A 保存时发现游标已被推进，丢弃本页并停下。
	// 两个驱动并行只会在租约切换瞬间发生（leader 选举是防线），重复执行限于 A 的那一页，计数不重复。
	n := batches.PageSize * 2
	f := newFixture(t, n)
	op := startTerminate(t, f)
	other := f.newUseCase()
	f.actions.hook = func(ctx context.Context, call int) {
		if call == 1 {
			f.actions.hook = nil
			other.ProcessActive(ctx)
		}
	}
	f.useCase.ProcessActive(context.Background())

	got := f.get(t, op)
	if got.Status != batch.StatusEnums.Completed || got.SucceededCount != n || got.TotalCount != n {
		t.Fatalf("batch = %+v, want completed with exact counters", got)
	}
	calls := f.actions.callCounts()
	twice := 0
	for i, id := range f.runIDs {
		if calls[id] == 2 {
			twice++
			if i >= batches.PageSize {
				t.Fatalf("run %d outside the first page was acted on twice", i)
			}
		}
	}
	if twice != batches.PageSize {
		t.Fatalf("%d runs acted on twice, want the %d runs of the overlapping page", twice, batches.PageSize)
	}
}

func TestListNewestFirst(t *testing.T) {
	// Happy Path: 列表从新到旧分页
	f := newFixture(t, 0)
	var ids []string
	for range 3 {
		op := startTerminate(t, f)
		ids = append(ids, op.ID)
		f.clock.Advance(time.Second)
	}
	slices.Reverse(ids)
	list, err := f.useCase.List(batches.ListFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 3 || len(list.Items) != 2 || list.Items[0].ID != ids[0] || list.Items[1].ID != ids[1] {
		t.Fatalf("list = %+v, want newest first", list)
	}
}
