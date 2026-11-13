package engine

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

const (
	// runWatchPollInterval 是有等待者时读取被等待 Run 的 execution state 的间隔，决定其他副本提交的变化
	// 最晚多久被本副本的等待者看到。
	runWatchPollInterval = 100 * time.Millisecond
	// runWatchBatch 是一次读取的 Run 数上限（一条 IN 查询的长度）。
	runWatchBatch = 500
)

// runWatch 唤醒等待某个 Run 变化的调用方：等待 Update 结果（awaitUpdateResult）与等待 Run 结束（WaitRunClosed）。
//
// Run 的变化以 execution state 的 NextEventID 表示：History 每追加一条事件它就增大，并与事件在同一事务提交。
// 等待方先读 NextEventID，再读它关心的事实（History、Run 状态），事实不满足时带着读到的 NextEventID 调用 Wait。
// 之后提交的变化总会让 NextEventID 超过这个值，因此唤醒不会丢失。唤醒只表示 Run 变了，等待方重读事实再判断。
//
// 唤醒有两个来源：
//   - 本副本提交 Update 完成或 Run 结束之后调用 Notify，立即唤醒；
//   - 有等待者期间每 runWatchPollInterval 批量读取一次被等待 Run 的 execution state（每 runWatchBatch 个 Run
//     一条查询），NextEventID 超过已知值时唤醒。其他副本提交的变化经这条路径在一个间隔内被看到。
//
// 没有等待者时不排定定时器、不读库。等待者只存在于本进程：副本重启时进行中的请求随连接断开，
// 调用方重发请求（Update 按 Update ID 幂等，Run 结束的等待可以重复）。
type runWatch struct {
	clock  clock.Clock
	states func(runIDs []string) (map[string]*execution.WorkflowExecutionState, error)
	logger *slog.Logger

	mu   sync.Mutex
	runs map[string]*watchedRun
	// timer 非 nil 表示已排定下一次读取。
	timer   clock.Timer
	stopped bool
}

// watchedRun 是一个被等待的 Run。
type watchedRun struct {
	// seen 是已知的最大 NextEventID。
	seen int64
	// changed 在 Run 变化时 close 并换成新的 channel。
	changed chan struct{}
	waiters int
}

func newRunWatch(clk clock.Clock, states func([]string) (map[string]*execution.WorkflowExecutionState, error), logger *slog.Logger) *runWatch {
	return &runWatch{clock: clk, states: states, logger: logger, runs: make(map[string]*watchedRun)}
}

// wake 唤醒这个 Run 当前的全部等待者。
func (r *watchedRun) wake() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// Wait 阻塞到 runID 在 seen 之后发生变化、ctx 结束或 timeout 到期（timeout <= 0 时只看 ctx），
// 发生变化时返回 true。seen 是调用方读取事实之前读到的 NextEventID。
func (w *runWatch) Wait(ctx context.Context, runID string, seen int64, timeout time.Duration) bool {
	w.mu.Lock()
	run := w.runs[runID]
	if run == nil {
		run = &watchedRun{seen: seen, changed: make(chan struct{})}
		w.runs[runID] = run
	}
	if seen < run.seen {
		// 已经观察到比调用方更新的变化：调用方读到的事实已过时。
		w.mu.Unlock()
		return true
	}
	if seen > run.seen {
		// 调用方比轮询先看到变化：唤醒持有旧值的等待者。
		run.seen = seen
		run.wake()
	}
	run.waiters++
	changed := run.changed
	w.armLocked()
	w.mu.Unlock()
	defer w.release(runID, run)

	var expired chan struct{}
	if timeout > 0 {
		expired = make(chan struct{})
		timer := w.clock.AfterFunc(timeout, func() { close(expired) })
		defer timer.Stop()
	}
	select {
	case <-changed:
		return true
	case <-ctx.Done():
		return false
	case <-expired:
		return false
	}
}

func (w *runWatch) release(runID string, run *watchedRun) {
	w.mu.Lock()
	defer w.mu.Unlock()
	run.waiters--
	if run.waiters == 0 && w.runs[runID] == run {
		delete(w.runs, runID)
	}
}

// Notify 在本副本提交 runID 的变化之后立即唤醒它的等待者。非阻塞，只持 w.mu，可以在 keyed lock 内调用。
func (w *runWatch) Notify(runID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if run := w.runs[runID]; run != nil {
		run.wake()
	}
}

// armLocked 在有等待者且没有排定读取时排定下一次读取。调用方持有 w.mu。
func (w *runWatch) armLocked() {
	if w.timer == nil && !w.stopped && len(w.runs) > 0 {
		w.timer = w.clock.AfterFunc(runWatchPollInterval, w.poll)
	}
}

// poll 读取被等待 Run 的 execution state，唤醒发生变化的 Run 的等待者；仍有等待者时排定下一次读取。
// 读库在 w.mu 之外进行。
func (w *runWatch) poll() {
	w.mu.Lock()
	w.timer = nil
	if w.stopped || len(w.runs) == 0 {
		w.mu.Unlock()
		return
	}
	runIDs := slices.Collect(maps.Keys(w.runs))
	w.mu.Unlock()

	states := make(map[string]*execution.WorkflowExecutionState, len(runIDs))
	var err error
	for chunk := range slices.Chunk(runIDs, runWatchBatch) {
		var part map[string]*execution.WorkflowExecutionState
		if part, err = w.states(chunk); err != nil {
			break
		}
		maps.Copy(states, part)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.logger.Warn("read watched runs failed; retrying", slog.Int("runs", len(runIDs)), slog.Any("err", err))
	} else {
		for _, runID := range runIDs {
			run := w.runs[runID]
			if run == nil {
				continue
			}
			state, ok := states[runID]
			switch {
			case !ok:
				// Run 已被删除：唤醒等待者，由它们重读得到 not found。
				run.wake()
			case state.NextEventID > run.seen:
				run.seen = state.NextEventID
				run.wake()
			}
		}
	}
	w.armLocked()
}

// Stop 停止读取；之后的 Wait 只能由 Notify、ctx 或 timeout 结束。
func (w *runWatch) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
}
