package memory

import (
	"crypto/rand"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

var _ execution.ExecutionManager = (*ExecutionManager)(nil)
var _ execution.HistoryBranchStore = (*ExecutionManager)(nil)

// ExecutionManager 用单一把锁 txMu 串行化所有写，使 RunInTx 内的 history + state
// 写在单个临界区内完成；fn 返回 error 时丢弃临时缓冲（两阶段提交），保证原子回滚。
//
// 锁层级：txMu 是最内层资源；底层 EventStore / RunStore / ActivityStateStore /
// TimerStateStore / ChildWorkflowStateStore 各自的锁在 txMu 持有期间被获取
// （顺序固定：先 events 再 states），不与 Server 的 keyedMu/mu 交叉。
type ExecutionManager struct {
	*EventStore
	*RunStore
	*ActivityStateStore
	*TimerStateStore
	*ChildWorkflowStateStore
	*WorkflowExecutionStateStore

	txMu            sync.Mutex
	historyTokenKey []byte
	historyLimits   history.HistoryLimits
	// clock 是存储的时间来源：事件时间、execution state 的时间字段与分页令牌有效期都取自它。
	clock clock.Clock
}

// NewExecutionManager 以缺省的 History 上限构造内存存储。
func NewExecutionManager() *ExecutionManager {
	return newExecutionManager(history.DefaultHistoryLimits())
}

func NewExecutionManagerWithHistoryLimits(limits history.HistoryLimits) (*ExecutionManager, error) {
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("memory workflow history limits: %w", err)
	}
	return newExecutionManager(limits), nil
}

func newExecutionManager(limits history.HistoryLimits) *ExecutionManager {
	executionStates := NewWorkflowExecutionStateStore()
	// crypto/rand.Read 不返回错误：随机源不可用时它直接终止进程。
	tokenKey := make([]byte, 32)
	_, _ = rand.Read(tokenKey)
	return &ExecutionManager{
		EventStore:                  NewEventStore(),
		RunStore:                    NewRunStore(),
		ActivityStateStore:          NewActivityStateStore(),
		TimerStateStore:             NewTimerStateStore(),
		ChildWorkflowStateStore:     NewChildWorkflowStateStore(),
		WorkflowExecutionStateStore: executionStates,
		historyTokenKey:             tokenKey,
		historyLimits:               limits,
		clock:                       systemclock.Clock{},
	}
}

// WithClock 让存储从 clk 取时间并返回存储本身，须在首次使用前调用；默认是系统时钟。
// 与 Server 共用一个存储时传入 Server 的时钟。
func (m *ExecutionManager) WithClock(clk clock.Clock) *ExecutionManager {
	m.clock = clk
	m.WorkflowExecutionStateStore.clock = clk
	return m
}

// List 用 execution state 投影的搜索属性与 blocked 过滤，与 gorm 后端读同一份投影。
func (m *ExecutionManager) List(filter execution.StoreListFilter) (execution.StoreListResult, error) {
	return m.RunStore.list(filter, func(run *execution.Run) (map[string]string, bool, bool) {
		state, err := m.WorkflowExecutionStateStore.GetWorkflowExecutionState(run.ID)
		if err != nil {
			return nil, false, false
		}
		return state.SearchAttributes, state.Blocked.Blocked, true
	})
}

// ListRecoveryRunIDs 按与 gorm 后端相同的条件选出周期补偿的候选 Run（见 execution.Store）。
func (m *ExecutionManager) ListRecoveryRunIDs(dueBefore time.Time) ([]string, error) {
	candidates := map[string]bool{}
	m.TimerStateStore.mu.RLock()
	for runID, timers := range m.TimerStateStore.states {
		for _, timer := range timers {
			if timer.IsScheduled() && timer.Deadline.Before(dueBefore) {
				candidates[runID] = true
			}
		}
	}
	m.TimerStateStore.mu.RUnlock()
	m.ActivityStateStore.mu.RLock()
	for runID, activities := range m.ActivityStateStore.states {
		for _, activity := range activities {
			if execution.RecoveryCandidateActivity(activity, dueBefore) {
				candidates[runID] = true
			}
		}
	}
	m.ActivityStateStore.mu.RUnlock()
	m.WorkflowExecutionStateStore.mu.RLock()
	for runID, state := range m.WorkflowExecutionStateStore.states {
		if state.OpenWorkflowTaskEventID > 0 || state.NeedsWorkflowTask() || state.Blocked.Blocked {
			candidates[runID] = true
		}
	}
	m.WorkflowExecutionStateStore.mu.RUnlock()

	m.RunStore.mu.RLock()
	defer m.RunStore.mu.RUnlock()
	var runIDs []string
	for runID, run := range m.RunStore.runs {
		if run.Status != history.StatusRunning {
			continue
		}
		due := func(deadline *time.Time) bool { return deadline != nil && deadline.Before(dueBefore) }
		if candidates[runID] || due(run.DeadlineAt) || due(run.RunDeadlineAt) {
			runIDs = append(runIDs, runID)
		}
	}
	slices.Sort(runIDs)
	return runIDs, nil
}

func (m *ExecutionManager) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	var eventID int64
	err := m.RunInTx(func(tx execution.ExecutionTx) error {
		id, err := tx.Append(runID, treeID, branchID, event)
		eventID = id
		return err
	})
	return eventID, err
}

func (m *ExecutionManager) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	var eventID int64
	var inserted bool
	err := m.RunInTx(func(tx execution.ExecutionTx) error {
		id, ok, err := tx.AppendIdempotent(runID, treeID, branchID, event)
		eventID, inserted = id, ok
		return err
	})
	return eventID, inserted, err
}

func (m *ExecutionManager) AppendWorkflowTaskCompletedWithEvents(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	var result execution.WorkflowTaskCompletionResult
	err := m.RunInTx(func(tx execution.ExecutionTx) error {
		var err error
		result, err = tx.AppendWorkflowTaskCompleted(runID, treeID, branchID, closeEvent, commandEvents)
		return err
	})
	return result, err
}

// AppendWorkflowTaskScheduled 在 Run 没有 open 的 Workflow Task 时追加 scheduled 事件；已有时返回它的 ID
// 与 inserted=false。open 的 Workflow Task 由 execution state 记录，与 gorm 后端在状态行锁内的判定一致。
func (m *ExecutionManager) AppendWorkflowTaskScheduled(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if event.WorkflowTaskScheduled == nil {
		event.WorkflowTaskScheduled = &history.WorkflowTaskScheduledEvent{}
	}
	var eventID int64
	var inserted bool
	err := m.RunInTx(func(tx execution.ExecutionTx) error {
		state, err := tx.GetWorkflowExecutionStateForUpdate(runID)
		if err != nil {
			return err
		}
		if state.OpenWorkflowTaskEventID > 0 {
			eventID = state.OpenWorkflowTaskEventID
			return nil
		}
		eventID, err = tx.Append(runID, treeID, branchID, event)
		inserted = err == nil
		return err
	})
	return eventID, inserted, err
}

func (m *ExecutionManager) AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	var eventID int64
	var inserted bool
	err := m.RunInTx(func(tx execution.ExecutionTx) error {
		id, ok, err := tx.AppendWorkflowTaskClosed(runID, treeID, branchID, event)
		eventID, inserted = id, ok
		return err
	})
	return eventID, inserted, err
}

// requireRun 与 gorm 后端的 executionStateForUpdate 对齐：run 不存在时 append 返回
// ErrRunNotFound，不为它写出事件与 execution state。
func requireRun(runs *RunStore, runID string) error {
	if _, ok := runs.snapshot(runID); !ok {
		return runNotFound(runID)
	}
	return nil
}

func runNotFound(runID string) error {
	return fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
}

func (m *ExecutionManager) GetEventsRange(runID string, afterExclusive, throughInclusive int64) ([]history.Event, error) {
	return m.EventStore.getEventsRange(runID, afterExclusive, throughInclusive), nil
}

func (m *ExecutionManager) GetEventsByDedupeKeys(runID string, keys []string) ([]history.Event, error) {
	return m.EventStore.eventsByDedupeKeys(runID, keys), nil
}

func (m *ExecutionManager) GetEventPage(request execution.HistoryPageRequest) (execution.HistoryPage, error) {
	request, err := execution.NormalizeHistoryPageRequest(request)
	if err != nil {
		return execution.HistoryPage{}, err
	}
	fingerprint := execution.HistoryBranchFingerprint(nil, request.RunID)
	snapshotMax, cursorID := int64(0), int64(0)
	var count, size int64
	if request.PageToken != "" {
		cursor, err := execution.DecodeHistoryPageToken(m.historyTokenKey, request.PageToken, m.clock.Now())
		if err != nil {
			return execution.HistoryPage{}, err
		}
		if cursor.RunID != request.RunID || cursor.Direction != request.Direction || cursor.BranchFingerprint != fingerprint || len(cursor.SnapshotUpperBounds) != 1 {
			return execution.HistoryPage{}, execution.ErrInvalidHistoryPageToken
		}
		snapshotMax, cursorID = cursor.SnapshotUpperBounds[0], cursor.NodeID
		count, size = cursor.HistoryEventCount, cursor.HistorySizeBytes
	}
	pageEvents, snapshotMax := m.EventStore.getEventWindow(request.RunID, snapshotMax, cursorID, request.Direction, request.PageSize+1)
	// 快照的事件数与字节数在首页按快照上界计算，后续页沿用分页令牌里的值，与 gorm 后端一致。
	if request.PageToken == "" {
		if count, size, err = m.snapshotMetrics(request.RunID, snapshotMax); err != nil {
			return execution.HistoryPage{}, err
		}
	}
	var nextToken string
	if len(pageEvents) > request.PageSize {
		pageEvents = pageEvents[:request.PageSize]
		now := m.clock.Now()
		nextToken, err = execution.EncodeHistoryPageToken(m.historyTokenKey, execution.HistoryPageCursor{
			Version: 1, RunID: request.RunID, BranchFingerprint: fingerprint, Direction: request.Direction,
			RangeOrdinal: 0, NodeID: pageEvents[len(pageEvents)-1].ID, SnapshotUpperBounds: []int64{snapshotMax},
			HistoryEventCount: count, HistorySizeBytes: size, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(15 * time.Minute).Unix(),
		})
		if err != nil {
			return execution.HistoryPage{}, err
		}
	}
	return execution.HistoryPage{Events: pageEvents, NextPageToken: nextToken, SnapshotMaxEventID: snapshotMax, HistoryEventCount: count, HistorySizeBytes: size}, nil
}

// snapshotMetrics 返回 Run 在快照上界 snapshotMax 之内的事件数与字节数；快照覆盖全部事件时直接取 execution state。
func (m *ExecutionManager) snapshotMetrics(runID string, snapshotMax int64) (count, size int64, err error) {
	if state, err := m.GetWorkflowExecutionState(runID); err == nil && state.LastEventID == snapshotMax {
		return state.HistoryEventCount, state.HistorySizeBytes, nil
	}
	visible := m.EventStore.getEventsRange(runID, 0, snapshotMax)
	for _, event := range visible {
		eventSize, err := history.CanonicalEventSize(event)
		if err != nil {
			return 0, 0, err
		}
		size += eventSize
	}
	return int64(len(visible)), size, nil
}

// ForkBranch 与 gorm 后端产出同样的 branch token；源 Run 不存在时返回 execution.ErrRunNotFound。
func (m *ExecutionManager) ForkBranch(req execution.ForkBranchRequest) (*execution.ForkBranchResponse, error) {
	m.txMu.Lock()
	defer m.txMu.Unlock()
	source, err := m.RunStore.Get(req.SourceRunID)
	if err != nil {
		return nil, fmt.Errorf("fork branch: get source run: %w", err)
	}
	sourceBranch, err := execution.DeserializeBranchToken(source.BranchToken)
	if err != nil {
		return nil, fmt.Errorf("fork branch: source branch token: %w", err)
	}
	token, err := execution.SerializeBranchToken(execution.ForkHistoryBranch(sourceBranch, req.ForkNodeID, req.NewBranchID))
	if err != nil {
		return nil, err
	}
	m.forkEventsTo(req.SourceRunID, req.NewRunID, req.ForkNodeID)
	return &execution.ForkBranchResponse{BranchToken: token}, nil
}

func (m *ExecutionManager) RunInTx(fn func(tx execution.ExecutionTx) error) error {
	m.txMu.Lock()
	defer m.txMu.Unlock()

	tx := &memExecutionTx{
		events:          m.EventStore,
		states:          m.ActivityStateStore,
		timers:          m.TimerStateStore,
		children:        m.ChildWorkflowStateStore,
		runs:            m.RunStore,
		executionStates: m.WorkflowExecutionStateStore,
		historyLimits:   m.historyLimits,
		clock:           m.clock,
	}
	if err := fn(tx); err != nil {
		return err // 未 commit，缓冲丢弃 → 回滚
	}
	return tx.commit()
}

// memExecutionTx 把写缓冲在临时切片，fn 成功才 commit 到底层 store（两阶段提交）。
// AppendIdempotent 需要在事务内即时返回 eventID/inserted，因此采用"预分配 id + 缓冲行"
// 策略：去重判定同时看已提交事件与本事务待提交缓冲。
type memExecutionTx struct {
	events          *EventStore
	states          *ActivityStateStore
	timers          *TimerStateStore
	children        *ChildWorkflowStateStore
	runs            *RunStore
	executionStates *WorkflowExecutionStateStore
	historyLimits   history.HistoryLimits
	clock           clock.Clock

	pendingEvents         []pendingEvent
	pendingStates         []*execution.ActivityState
	pendingTimers         []*execution.TimerState
	pendingTimerCloses    []string
	pendingActivityCloses []string
	pendingChildStates    []*execution.ChildWorkflowState
	pendingRuns           []*execution.Run
	pendingRunUpdates     []*execution.Run
	pendingAttrSyncs      []string
	pendingResetRunIDs    []pendingResetRunID
	pendingCurrentCASes   []pendingCurrentRunCAS
	pendingActivations    []pendingActivation
}

// pendingActivation 是事务内开始的一次 Workflow Task 激活，先于同一事务追加的事件应用。
type pendingActivation struct {
	runID                                 string
	scheduledEventID                      int64
	executionGeneration, stickyGeneration int64
	activationID                          string
}

type pendingResetRunID struct {
	runID      string
	resetRunID string
}

type pendingCurrentRunCAS struct {
	namespace        string
	workflowID       string
	expectedOldRunID string
	newRunID         string
}

type pendingEvent struct {
	runID            string
	treeID, branchID string
	event            history.Event
	size             int64
}

// prepareEvent 是事务内全部追加的入口：没有时间的事件取存储时钟的当前时刻，再归一化失败载荷。
// 归一化与 gorm 落库共用 execution.NormalizeEventFailures 这一份实现：两个后端因此接受同一份失败载荷范围，
// 超限在写入前明确失败，不静默截断或降级。
func (t *memExecutionTx) prepareEvent(event history.Event) (history.Event, error) {
	normalized, err := execution.NormalizeEventFailures(execution.StampEvent(event, t.clock.Now()))
	if err != nil {
		return history.Event{}, fmt.Errorf("memory event store: %w", err)
	}
	return normalized, nil
}

// GetEventIDByDedupeKey 先看本 Run branch 上已提交的事件，再看本事务缓冲。
func (t *memExecutionTx) GetEventIDByDedupeKey(runID, _, dedupeKey string) (int64, bool, error) {
	if id, found := t.events.findDedupe(runID, dedupeKey); found {
		return id, true, nil
	}
	for _, pe := range t.pendingEvents {
		if pe.runID != runID {
			continue
		}
		if k, ok := history.DedupeKey(pe.event); ok && k == dedupeKey {
			return pe.event.ID, true, nil
		}
	}
	return 0, false, nil
}

func (t *memExecutionTx) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if err := t.requireRun(runID); err != nil {
		return 0, false, err
	}
	normalized, err := t.prepareEvent(event)
	if err != nil {
		return 0, false, err
	}
	event = normalized
	if key, ok := history.DedupeKey(event); ok {
		if id, found, _ := t.GetEventIDByDedupeKey(runID, branchID, key); found {
			return id, false, nil
		}
	}
	if err := t.checkOpen(runID, event); err != nil {
		return 0, false, err
	}
	id := t.nextEventID(runID)
	event.ID = id
	event, size, err := storedEvent(event)
	if err != nil {
		return 0, false, err
	}
	if err := t.checkHistoryBudget(runID, []int64{size}, history.UsesReservedHistoryCloseBudget(event)); err != nil {
		return 0, false, err
	}
	t.pendingEvents = append(t.pendingEvents, pendingEvent{runID: runID, treeID: treeID, branchID: branchID, event: event, size: size})
	return id, true, nil
}

func (t *memExecutionTx) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	if err := t.requireRun(runID); err != nil {
		return 0, err
	}
	normalized, err := t.prepareEvent(event)
	if err != nil {
		return 0, err
	}
	event = normalized
	if err := t.checkOpen(runID, event); err != nil {
		return 0, err
	}
	id := t.nextEventID(runID)
	event.ID = id
	event, size, err := storedEvent(event)
	if err != nil {
		return 0, err
	}
	if err := t.checkHistoryBudget(runID, []int64{size}, history.UsesReservedHistoryCloseBudget(event)); err != nil {
		return 0, err
	}
	t.pendingEvents = append(t.pendingEvents, pendingEvent{runID: runID, treeID: treeID, branchID: branchID, event: event, size: size})
	return id, nil
}

func (t *memExecutionTx) AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if !history.IsWorkflowTaskCloseEventType(event.EventType()) {
		return 0, false, fmt.Errorf("workflow task close event type required: %s", event.EventType())
	}
	if err := t.requireRun(runID); err != nil {
		return 0, false, err
	}
	normalized, nerr := t.prepareEvent(event)
	if nerr != nil {
		return 0, false, nerr
	}
	event = normalized
	for _, existing := range t.branchAndPending(runID) {
		if history.IsWorkflowTaskCloseEventType(existing.EventType()) && existing.FlatScheduledEventID() == event.FlatScheduledEventID() {
			return existing.ID, false, nil
		}
	}
	if err := t.checkOpen(runID, event); err != nil {
		return 0, false, err
	}
	id := t.nextEventID(runID)
	event.ID = id
	event, size, err := storedEvent(event)
	if err != nil {
		return 0, false, err
	}
	if err := t.checkHistoryBudget(runID, []int64{size}, true); err != nil {
		return 0, false, err
	}
	t.pendingEvents = append(t.pendingEvents, pendingEvent{runID: runID, treeID: treeID, branchID: branchID, event: event, size: size})
	return id, true, nil
}

// AppendWorkflowTaskCompleted 在事务缓冲内写 WFT close 与命令事件（幂等）。去重判定同时看本 Run
// branch 上已提交的事件与本事务待提交缓冲，写入与 ActivityState upsert 在同一事务提交。
func (t *memExecutionTx) AppendWorkflowTaskCompleted(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	if closeEvent.EventType() != history.EventWorkflowTaskCompleted {
		return execution.WorkflowTaskCompletionResult{}, fmt.Errorf("workflow task completed event type required: %s", closeEvent.EventType())
	}
	// 同一次提交的事件共用一个时刻。
	now := t.clock.Now()
	closeID, inserted, err := t.AppendWorkflowTaskClosed(runID, treeID, branchID, execution.StampEvent(closeEvent, now))
	if err != nil {
		return execution.WorkflowTaskCompletionResult{}, err
	}
	if !inserted {
		return execution.WorkflowTaskCompletionResult{ClosedInserted: false}, nil
	}
	result := execution.WorkflowTaskCompletionResult{ClosedInserted: true, ClosedEventID: closeID, CommandEvents: make([]execution.WorkflowTaskCommandEventResult, 0, len(commandEvents))}
	for _, ev := range commandEvents {
		id, inserted, err := t.AppendIdempotent(runID, treeID, branchID, execution.StampEvent(ev, now))
		if err != nil {
			return execution.WorkflowTaskCompletionResult{}, err
		}
		result.CommandEvents = append(result.CommandEvents, execution.WorkflowTaskCommandEventResult{EventID: id, Inserted: inserted})
	}
	return result, nil
}

// GetWorkflowExecutionStateForUpdate 返回本事务可见的 execution state：已提交的 state 叠加本事务内
// 创建的 Run、Run 更新与已追加的事件，与 commit 的应用顺序一致。RunInTx 持有 txMu，事务之间串行。
func (t *memExecutionTx) GetWorkflowExecutionStateForUpdate(runID string) (*execution.WorkflowExecutionState, error) {
	state, err := t.executionStates.GetWorkflowExecutionState(runID)
	if err != nil {
		for _, run := range t.pendingRuns {
			if run.ID == runID {
				events, _ := t.events.GetEvents(runID)
				state, err = execution.BuildWorkflowExecutionState(run, events)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	for _, run := range t.pendingRunUpdates {
		if run.ID == runID {
			state.SyncFromRun(run, t.clock.Now())
		}
	}
	for _, activation := range t.pendingActivations {
		if activation.runID == runID {
			state.BeginActivation(activation.scheduledEventID, activation.executionGeneration, activation.stickyGeneration, activation.activationID, t.clock.Now())
		}
	}
	for _, pending := range t.pendingEvents {
		if pending.runID == runID {
			execution.ApplyEventToExecutionState(state, pending.event, pending.size)
		}
	}
	return state, nil
}

func (t *memExecutionTx) HistoryLimits() history.HistoryLimits { return t.historyLimits }

// BeginWorkflowActivation 在本事务可见的 state 上判定激活；成功的激活缓冲到提交时写入。
// 同一事务里激活先于事件追加。
func (t *memExecutionTx) BeginWorkflowActivation(runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error) {
	state, err := t.GetWorkflowExecutionStateForUpdate(runID)
	if err != nil {
		return false, err
	}
	if !state.BeginActivation(scheduledEventID, executionGeneration, stickyGeneration, activationID, t.clock.Now()) {
		return false, nil
	}
	t.pendingActivations = append(t.pendingActivations, pendingActivation{
		runID: runID, scheduledEventID: scheduledEventID,
		executionGeneration: executionGeneration, stickyGeneration: stickyGeneration, activationID: activationID,
	})
	return true, nil
}

// requireRun 接受已提交的 run 与本事务内 CreateRun 的 run。
// checkOpen 与 gorm 后端的 appendEvent 对齐：本事务可见的 execution state 已终态时拒绝追加 event。
// 去重命中的事件在调用前已返回。
func (t *memExecutionTx) checkOpen(runID string, event history.Event) error {
	state, err := t.GetWorkflowExecutionStateForUpdate(runID)
	if err != nil {
		return err
	}
	if state.IsTerminal() {
		return execution.RunClosedError(runID, event)
	}
	return nil
}

func (t *memExecutionTx) requireRun(runID string) error {
	for _, run := range t.pendingRuns {
		if run.ID == runID {
			return nil
		}
	}
	return requireRun(t.runs, runID)
}

func (t *memExecutionTx) checkHistoryBudget(runID string, eventSizes []int64, serverClose bool) error {
	metrics := committedHistoryMetrics(t.executionStates, runID)
	for _, pending := range t.pendingEvents {
		if pending.runID == runID {
			metrics.EventCount++
			metrics.SizeBytes += pending.size
		}
	}
	return history.CheckHistoryBudget(runID, metrics, eventSizes, t.historyLimits, serverClose)
}

// committedHistoryMetrics 读取 runID 已提交的 history 计数/字节数。缺少 execution state
// （run 尚无事件）时返回零值，与 CheckHistoryBudget 的首次 append 语义一致。
func committedHistoryMetrics(states *WorkflowExecutionStateStore, runID string) history.HistoryMetrics {
	metrics := history.HistoryMetrics{}
	if state, err := states.GetWorkflowExecutionState(runID); err == nil {
		metrics.EventCount = state.HistoryEventCount
		metrics.SizeBytes = state.HistorySizeBytes
	}
	return metrics
}

func (t *memExecutionTx) nextEventID(runID string) int64 {
	var next int64
	if state, err := t.executionStates.GetWorkflowExecutionState(runID); err == nil {
		next = state.NextEventID
	}
	if next <= 0 {
		t.events.mu.RLock()
		next = t.events.nextID[runID] + 1
		t.events.mu.RUnlock()
	}
	for _, pending := range t.pendingEvents {
		if pending.runID == runID && pending.event.ID >= next {
			next = pending.event.ID + 1
		}
	}
	return next
}

// branchAndPending 返回 runID 在自己 branch 上已提交的事件与本事务待提交事件（供 close 幂等判定）。
func (t *memExecutionTx) branchAndPending(runID string) []history.Event {
	out := t.events.branchEvents(runID)
	for _, pe := range t.pendingEvents {
		if pe.runID == runID {
			out = append(out, pe.event)
		}
	}
	return out
}

func (t *memExecutionTx) UpsertActivityState(state *execution.ActivityState) error {
	t.pendingStates = append(t.pendingStates, cloneActivityState(state))
	state.ClearPatch()
	return nil
}

// UpdateActivityState 与 UpsertActivityState 相同：提交时对已存在的 state 只应用改过的字段。
func (t *memExecutionTx) UpdateActivityState(state *execution.ActivityState) error {
	return t.UpsertActivityState(state)
}

// GetActivityStateForUpdate 在 txMu 临界区内读 state（RunInTx 已持 txMu，故天然串行——
// 这就是 memory 的"行级锁"等价物）。先看本事务待提交缓冲，再回退到已提交存储。
func (t *memExecutionTx) GetActivityStateForUpdate(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	for i := len(t.pendingStates) - 1; i >= 0; i-- {
		st := t.pendingStates[i]
		if st.RunID == runID && st.ScheduledEventID == scheduledEventID {
			return cloneActivityState(st), true, nil
		}
	}
	return t.states.GetActivityState(runID, scheduledEventID)
}

func (t *memExecutionTx) UpsertTimerState(state *execution.TimerState) error {
	t.pendingTimers = append(t.pendingTimers, cloneTimerState(state))
	state.ClearPatch()
	return nil
}

// UpdateTimerState 与 UpsertTimerState 相同：提交时对已存在的 state 只应用改过的字段。
func (t *memExecutionTx) UpdateTimerState(state *execution.TimerState) error {
	return t.UpsertTimerState(state)
}

// CloseScheduledTimers 在提交时关闭 runID 仍为 Scheduled 的 timer，排在本事务的 timer 写入之后。
func (t *memExecutionTx) CloseScheduledTimers(runID string) error {
	t.pendingTimerCloses = append(t.pendingTimerCloses, runID)
	return nil
}

// CloseOpenActivities 在提交时关闭 runID 仍为 Scheduled 或 Started 的 Activity，排在本事务的 Activity 写入之后。
func (t *memExecutionTx) CloseOpenActivities(runID string) error {
	t.pendingActivityCloses = append(t.pendingActivityCloses, runID)
	return nil
}

// GetTimerStateForUpdate 在 txMu 临界区内读 timer state（RunInTx 已持 txMu，故天然串行——
// 这就是 memory 的"行级锁"等价物）。先看本事务待提交缓冲，再回退到已提交存储。
func (t *memExecutionTx) GetTimerStateForUpdate(runID string, scheduledEventID int64) (*execution.TimerState, bool, error) {
	for i := len(t.pendingTimers) - 1; i >= 0; i-- {
		st := t.pendingTimers[i]
		if st.RunID == runID && st.ScheduledEventID == scheduledEventID {
			return cloneTimerState(st), true, nil
		}
	}
	return t.timers.GetTimerState(runID, scheduledEventID)
}

func (t *memExecutionTx) UpsertChildWorkflowState(state *execution.ChildWorkflowState) error {
	t.pendingChildStates = append(t.pendingChildStates, cloneChildWorkflowState(state))
	state.ClearPatch()
	return nil
}

// CreateRun 在事务缓冲内建新 run（commit 时写入 RunStore）；ID 已存在时返回 execution.ErrRunAlreadyExists。
func (t *memExecutionTx) CreateRun(run *execution.Run) error {
	if len(run.BranchToken) == 0 {
		return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunBranchTokenMissing)
	}
	if t.requireRun(run.ID) == nil {
		return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunAlreadyExists)
	}
	t.pendingRuns = append(t.pendingRuns, cloneRun(run))
	run.ClearPatch()
	return nil
}

// GetRunForUpdate 返回本事务可见的 Run：已提交的 Run（或本事务 CreateRun 的 Run）叠加本事务缓冲的写入，
// 叠加顺序与 commit 一致。RunInTx 持有 txMu，事务之间串行，这就是内存后端的 state 行锁：读到的是已提交的
// 最新值，本事务提交前没有别的事务能改它。
func (t *memExecutionTx) GetRunForUpdate(runID string) (*execution.Run, error) {
	var run *execution.Run
	for _, pending := range t.pendingRuns {
		if pending.ID == runID {
			run = cloneRun(pending)
		}
	}
	if run == nil {
		committed, err := t.runs.Get(runID)
		if err != nil {
			return nil, runNotFound(runID)
		}
		run = committed
	}
	for _, update := range t.pendingRunUpdates {
		if update.ID == runID {
			applyPatch(run, update, update.ExportPatch())
		}
	}
	for _, reset := range t.pendingResetRunIDs {
		if reset.runID == runID {
			run.ResetRunID = reset.resetRunID
		}
	}
	snap, _ := t.runSnapshotOf(runID)
	run.IsCurrent = snap.isCurrent
	for _, synced := range t.pendingAttrSyncs {
		if synced == runID {
			state, err := t.GetWorkflowExecutionStateForUpdate(runID)
			if err != nil {
				return nil, err
			}
			run.SearchAttributes = cloneStringMap(state.SearchAttributes)
			break
		}
	}
	run.ClearPatch()
	return run, nil
}

// UpdateRun 在事务缓冲内更新现有 run（commit 时调 RunStore.update）。
func (t *memExecutionTx) UpdateRun(run *execution.Run) error {
	if err := t.requireRun(run.ID); err != nil {
		return err
	}
	t.pendingRunUpdates = append(t.pendingRunUpdates, cloneRun(run))
	run.ClearPatch()
	return nil
}

// SyncRunSearchAttributes 在事务缓冲内登记写回，commit 时事件投影完成后再执行。
func (t *memExecutionTx) SyncRunSearchAttributes(runID string) error {
	t.pendingAttrSyncs = append(t.pendingAttrSyncs, runID)
	return nil
}

// SetResetRunID 在事务缓冲内记录 reset_run_id（commit 时调 RunStore.SetResetRunID）。
func (t *memExecutionTx) SetResetRunID(runID, resetRunID string) error {
	if err := t.requireRun(runID); err != nil {
		return err
	}
	t.pendingResetRunIDs = append(t.pendingResetRunIDs, pendingResetRunID{runID, resetRunID})
	return nil
}

// SetCurrentRunCAS 真 CAS：compare 在调用时即时计算（看已提交 + 本事务待提交的 run），
// 命中才把切换记入缓冲（commit 时落底层 store）。compare 失败立即返回 (false,nil)，不入缓冲。
func (t *memExecutionTx) SetCurrentRunCAS(namespace, workflowID, expectedOldRunID, newRunID string) (bool, error) {
	ns := protocol.NormalizeNamespace(namespace)
	currentID, hasCurrent := t.currentRunID(ns, workflowID)
	if expectedOldRunID == "" {
		if hasCurrent {
			return false, nil
		}
	} else {
		if !hasCurrent || currentID != expectedOldRunID {
			return false, nil
		}
	}
	if !t.runExists(newRunID) {
		return false, nil
	}
	t.pendingCurrentCASes = append(t.pendingCurrentCASes, pendingCurrentRunCAS{ns, workflowID, expectedOldRunID, newRunID})
	return true, nil
}

// currentRunID 返回 (namespace,workflowID) 当前 is_current run 的 ID，叠加本事务待提交写
// （pendingRuns / pendingRunUpdates / 之前的 pendingCurrentCASes）覆盖已提交快照。
func (t *memExecutionTx) currentRunID(ns, workflowID string) (string, bool) {
	state := t.runStateSnapshot()
	for id, st := range state {
		if st.isCurrent && st.namespace == ns && st.workflowID == workflowID {
			return id, true
		}
	}
	return "", false
}

func (t *memExecutionTx) runExists(runID string) bool {
	_, ok := t.runSnapshotOf(runID)
	return ok
}

type runSnapshot struct {
	namespace  string
	workflowID string
	isCurrent  bool
}

func newRunSnapshot(run *execution.Run) runSnapshot {
	return runSnapshot{protocol.NormalizeNamespace(run.Namespace), run.WorkflowID, run.IsCurrent}
}

// runSnapshotOf 是 runStateSnapshot 里 runID 这一项，只读这一个 Run：按同样的顺序叠加本事务缓冲。
func (t *memExecutionTx) runSnapshotOf(runID string) (runSnapshot, bool) {
	snap, ok := t.runs.snapshot(runID)
	for _, run := range t.pendingRuns {
		if run.ID == runID {
			snap, ok = newRunSnapshot(run), true
		}
	}
	for _, run := range t.pendingRunUpdates {
		if run.ID != runID {
			continue
		}
		next := newRunSnapshot(run)
		if ok {
			next.isCurrent = snap.isCurrent
		}
		snap, ok = next, true
	}
	if !ok {
		return runSnapshot{}, false
	}
	for _, cas := range t.pendingCurrentCASes {
		if cas.expectedOldRunID != "" && cas.expectedOldRunID == runID {
			snap.isCurrent = false
		}
		if cas.newRunID == runID {
			snap.isCurrent = true
		}
	}
	return snap, true
}

// runStateSnapshot 合并已提交 run 与本事务缓冲（Create/Update/CAS 顺序应用），用于 CAS compare。
func (t *memExecutionTx) runStateSnapshot() map[string]runSnapshot {
	snap := t.runs.snapshots()
	for _, run := range t.pendingRuns {
		snap[run.ID] = newRunSnapshot(run)
	}
	for _, run := range t.pendingRunUpdates {
		// UpdateRun 不改 current：保留快照中已有的 isCurrent（与 RunStore.update 语义一致）。
		next := newRunSnapshot(run)
		if prev, ok := snap[run.ID]; ok {
			next.isCurrent = prev.isCurrent
		}
		snap[run.ID] = next
	}
	for _, cas := range t.pendingCurrentCASes {
		if cas.expectedOldRunID != "" {
			if s, ok := snap[cas.expectedOldRunID]; ok {
				s.isCurrent = false
				snap[cas.expectedOldRunID] = s
			}
		}
		if s, ok := snap[cas.newRunID]; ok {
			s.isCurrent = true
			snap[cas.newRunID] = s
		}
	}
	return snap
}

// GetChildWorkflowStateForUpdate 在 txMu 临界区内读 child state（RunInTx 已持 txMu，天然串行——
// memory 的"行级锁"等价物）。先看本事务待提交缓冲，再回退到已提交存储。
func (t *memExecutionTx) GetChildWorkflowStateForUpdate(runID string, scheduledEventID int64) (*execution.ChildWorkflowState, bool, error) {
	for i := len(t.pendingChildStates) - 1; i >= 0; i-- {
		st := t.pendingChildStates[i]
		if st.RunID == runID && st.ScheduledEventID == scheduledEventID {
			return cloneChildWorkflowState(st), true, nil
		}
	}
	return t.children.GetChildWorkflowState(runID, scheduledEventID)
}

// commit 把缓冲的写入按顺序应用到底层 store。各写入方法在调用时已校验 Run 是否存在，
// 这里遇到的错误仍然向上返回，RunInTx 不把失败的提交报告成成功。
func (t *memExecutionTx) commit() error {
	for _, run := range t.pendingRuns {
		if err := t.runs.create(run); err != nil {
			return err
		}
		if _, err := t.executionStates.GetWorkflowExecutionState(run.ID); err != nil {
			events, _ := t.events.GetEvents(run.ID)
			state, err := execution.BuildWorkflowExecutionState(run, events)
			if err != nil {
				return err
			}
			t.executionStates.put(state)
		}
	}
	for _, p := range t.pendingResetRunIDs {
		if err := t.runs.SetResetRunID(p.runID, p.resetRunID); err != nil {
			return err
		}
	}
	for _, c := range t.pendingCurrentCASes {
		if _, err := t.runs.setCurrentRunCAS(c.namespace, c.workflowID, c.expectedOldRunID, c.newRunID); err != nil {
			return err
		}
	}
	for _, activation := range t.pendingActivations {
		begun, err := t.executionStates.beginWorkflowActivation(activation.runID, activation.scheduledEventID, activation.executionGeneration, activation.stickyGeneration, activation.activationID)
		if err != nil {
			return err
		}
		if !begun {
			return fmt.Errorf("workflow activation %s of run %s changed during the transaction", activation.activationID, activation.runID)
		}
	}
	for _, pe := range t.pendingEvents {
		state, err := t.executionStates.GetWorkflowExecutionState(pe.runID)
		if err != nil {
			return err
		}
		t.events.appendPrebuilt(pe.runID, pe.event)
		execution.ApplyEventToExecutionState(state, pe.event, pe.size)
		t.executionStates.put(state)
	}
	// Run 的更新排在事件之后：读方看到 Run 进入终态时，它的终态事件已经可读。
	for _, run := range t.pendingRunUpdates {
		if err := t.runs.update(run); err != nil {
			return err
		}
		if err := t.executionStates.updateFromRun(run); err != nil {
			return err
		}
	}
	for _, runID := range t.pendingAttrSyncs {
		state, err := t.executionStates.GetWorkflowExecutionState(runID)
		if err != nil {
			return err
		}
		t.runs.syncSearchAttributes(runID, state.SearchAttributes)
	}
	for _, st := range t.pendingStates {
		if err := t.states.UpsertActivityState(st); err != nil {
			return err
		}
	}
	for _, st := range t.pendingTimers {
		if err := t.timers.UpsertTimerState(st); err != nil {
			return err
		}
	}
	for _, runID := range t.pendingTimerCloses {
		t.timers.closeScheduled(runID, t.clock.Now())
	}
	for _, runID := range t.pendingActivityCloses {
		t.states.closeOpen(runID, t.clock.Now())
	}
	for _, st := range t.pendingChildStates {
		if err := t.children.UpsertChildWorkflowState(st); err != nil {
			return err
		}
	}
	return nil
}
