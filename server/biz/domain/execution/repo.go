package execution

import (
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// RunClosedError 返回追加 event 到已结束的 runID 时的错误，它满足 errors.Is(err, ErrRunClosed)。
func RunClosedError(runID string, event history.Event) error {
	return fmt.Errorf("append %s to run %s: %w", event.EventType(), runID, ErrRunClosed)
}

type WorkflowTaskCommandEventResult struct {
	EventID  int64
	Inserted bool
}

type WorkflowTaskCompletionResult struct {
	// Superseded 表示 started 之后有新事实，本次完成没有提交，已写入 WorkflowTaskFailed，调用方立即重新调度。
	Superseded     bool
	ClosedInserted bool
	ClosedEventID  int64
	CommandEvents  []WorkflowTaskCommandEventResult
	// CancelledActivities 是本次提交里因取消请求立即结束（尚未开始）的 Activity 的调度事件 ID：
	// 提交后停掉它们的 timer 并调度 Workflow Task。
	CancelledActivities []int64
}

// EventStore persists History with first-wins idempotency for scheduled and
// closed workflow tasks and content-based idempotency for command events.
type EventStore interface {
	GetEvents(runID string) ([]history.Event, error)
	Append(runID, treeID, branchID string, event history.Event) (int64, error)
	AppendIdempotent(runID, treeID, branchID string, event history.Event) (eventID int64, inserted bool, err error)
	AppendWorkflowTaskScheduled(runID, treeID, branchID string, event history.Event) (eventID int64, inserted bool, err error)
	AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (eventID int64, inserted bool, err error)
	AppendWorkflowTaskCompletedWithEvents(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (WorkflowTaskCompletionResult, error)
	// GetWorkflowTaskScheduledEvent 返回持久化的 WorkflowTaskScheduled 事件，供复用 open WFT 的调度读取 trace context。
	GetWorkflowTaskScheduledEvent(runID string, scheduledEventID int64) (history.Event, bool, error)
}

// StoreListFilter 的全部条件都由存储层在分页之前应用。
type StoreListFilter struct {
	Namespace string
	// RunIDs 非空时只返回这些 Run。
	RunIDs     []string
	WorkflowID string
	Statuses   []history.Status
	// SearchAttributes 按 execution state 投影的搜索属性过滤，匹配规则是 MatchSearchAttributes。
	SearchAttributes map[string]string
	// BlockedOnly 只返回 execution state 投影为 blocked 的 Run。
	BlockedOnly     bool
	IncludeTerminal bool
	// CreatedBefore 非空时只返回 created_at 早于它的 Run。
	CreatedBefore *time.Time
	// After 非空时只返回排序上位于它之后的 Run（升序时更大，降序时更小），用于按游标翻页：
	// 翻页期间离开或进入过滤条件的 Run 不会让后续页漂移。
	After     *RunCursor
	OrderDesc bool
	Offset    int
	Limit     int
	// SkipTotal 为 true 时不计算 StoreListResult.Total（保持 0）：按游标翻页的扫描与只需要「是否有下一页」的
	// 列表页用它省掉每页一次的计数。
	SkipTotal bool
}

// RunCursor 是 Run 在列表排序键 (created_at, id) 上的位置。
type RunCursor struct {
	CreatedAt time.Time `json:"created_at"`
	RunID     string    `json:"run_id"`
}

// CursorOf 返回 run 在列表排序键上的位置。
func CursorOf(run *Run) RunCursor {
	return RunCursor{CreatedAt: run.CreatedAt, RunID: run.ID}
}

// StoreListResult.Total 是满足全部过滤条件的 Run 总数，与分页无关（SkipTotal 时为 0）；HasMore 表示这一页之后
// 还有满足条件的 Run。
type StoreListResult struct {
	Runs    []*Run
	Total   int
	HasMore bool
}

// RunCount 是一组 (namespace, status) 下的 Run 数量。
type RunCount struct {
	Namespace string
	Status    history.Status
	Count     int
}

// Store 是 Run 的读取与少量单列写入。Run 的状态变更经 ExecutionTx.UpdateRun 与同一业务事实的 History、
// state 写入在同一事务提交。
type Store interface {
	Get(runID string) (*Run, error)
	// SetResetRunID 把 Run 的 ResetRunID 写成最新一次 reset 出的 Run，后写覆盖先写；
	// Run 不存在时返回满足 errors.Is(err, ErrRunNotFound) 的错误。
	SetResetRunID(runID, resetRunID string) error
	GetCurrentRunID(namespace, workflowID string) (string, error)
	List(filter StoreListFilter) (StoreListResult, error)
	// CountRuns 返回按 namespace 与 Run 行状态分组的 Run 数量；namespace 非空时只统计这个 namespace。
	CountRuns(namespace string) ([]RunCount, error)
	// ListRecoveryRunIDs 返回周期补偿要处理的 Running Run 的 ID（去重、升序），即满足任一条件的 Run：
	//   - Run 的执行截止时间或 Run 截止时间早于 dueBefore；
	//   - 有 deadline 早于 dueBefore 的 Scheduled timer；
	//   - 有 Scheduled 的 Activity（待派发、等重试退避或等领取），或有 ActivityTimeoutAt 早于 dueBefore 的 Started Activity；
	//   - execution state 有 open Workflow Task、有新事实待调度 Workflow Task（NeedsWorkflowTask）或处于 blocked。
	// 每个条件都走索引，结果规模与到期或待处理的事项数成正比，与 Running Run 总数无关。
	ListRecoveryRunIDs(dueBefore time.Time) ([]string, error)
}

var (
	ErrRunNotFound = errors.New("run not found")
	// ErrRunAlreadyExists 表示要创建的 Run ID 已经有记录。
	ErrRunAlreadyExists = errors.New("run already exists")
	// ErrRunBranchTokenMissing 表示要创建的 Run 没有 History branch token。
	ErrRunBranchTokenMissing   = errors.New("run has no branch token")
	ErrNoCurrentRun            = errors.New("no current run")
	ErrRunOrWorkflowIDRequired = errors.New("run id or workflow id required")
	ErrCurrentRunConflict      = errors.New("current run conflict")
	// ErrRunClosed 表示 Run 的 History 已有终态事件。存储在持有 execution state 锁的事务内拒绝在终态之后追加事件，
	// 带去重键且已记录过的事件除外（返回已记录的事件、inserted=false）。它满足 errors.Is(err, protocol.ErrInvalidRequest)：
	// 打到已结束 Run 的 Signal、Update 与取消请求按请求错误应答。
	ErrRunClosed = protocol.InvalidRequestf("run is closed")
	// ErrTreeNotTerminal 表示整棵 History 树还有未终态的 Run，DeleteTree 不删除它。保留期扫描据此把
	// 任务推后重试，不计入失败次数。
	ErrTreeNotTerminal = errors.New("history tree still has non-terminal runs")
)

// ExecutionManager owns the atomic History and mutable-state commit boundary.
// Every backend implements the whole interface; callers use it without probing
// for capabilities.
type ExecutionManager interface {
	EventStore
	HistoryPageStore
	HistoryBranchStore
	Store
	ActivityStateStore
	TimerStateStore
	ChildWorkflowStateStore
	WorkflowExecutionStateStore
	RunInTx(fn func(tx ExecutionTx) error) error
}

type ExecutionTx interface {
	AppendIdempotent(runID, treeID, branchID string, event history.Event) (eventID int64, inserted bool, err error)
	Append(runID, treeID, branchID string, event history.Event) (eventID int64, err error)
	AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (eventID int64, inserted bool, err error)
	AppendWorkflowTaskCompleted(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (WorkflowTaskCompletionResult, error)
	UpsertActivityState(state *ActivityState) error
	// UpdateActivityState 只写已存在的 Activity state 经 setter 改过的列，MySQL 上是一条 UPDATE。
	// state 来自本事务的 GetActivityStateForUpdate，行已存在且已加锁。
	UpdateActivityState(state *ActivityState) error
	GetActivityStateForUpdate(runID string, scheduledEventID int64) (*ActivityState, bool, error)
	UpsertTimerState(state *TimerState) error
	// UpdateTimerState 只写已存在的 timer state 经 setter 改过的列，MySQL 上是一条 UPDATE。
	// state 来自本事务的 GetTimerStateForUpdate，行已存在且已加锁。
	UpdateTimerState(state *TimerState) error
	GetTimerStateForUpdate(runID string, scheduledEventID int64) (*TimerState, bool, error)
	// CloseScheduledTimers 把 Run 仍为 Scheduled 的 timer（含本事务写入的）置为 RunClosed，UpdatedAt 取存储的时钟；
	// MySQL 上是一条 UPDATE。Run 结束的事务调用它。
	CloseScheduledTimers(runID string) error
	UpsertChildWorkflowState(state *ChildWorkflowState) error
	GetChildWorkflowStateForUpdate(runID string, scheduledEventID int64) (*ChildWorkflowState, bool, error)
	// CreateRun 在 Run ID 已存在时返回满足 errors.Is(err, ErrRunAlreadyExists) 的错误，
	// Run 没有 branch token 时返回满足 errors.Is(err, ErrRunBranchTokenMissing) 的错误。
	CreateRun(run *Run) error
	// GetRunForUpdate 在事务内读取 Run，结果包含本事务已做的写入。它先让事务持有这个 Run 的 execution state
	// 行锁（GetWorkflowExecutionStateForUpdate 用的同一把锁），再以加锁读读 Run 行。写 Run 的事务都先取
	// state 行锁，所以读到的是已提交的最新值，本事务提交之前其它事务改不了它。写路径依据 Run 字段做的判定
	// 都基于它读到的 Run。Run 不存在时返回满足 errors.Is(err, ErrRunNotFound) 的错误。
	//
	// 锁顺序：一个事务在 MySQL 上先取 state 行锁，再读写 Run 行。事务里涉及两个已存在的 Run 时按固定顺序取
	// state 行锁：子 Workflow 关闭向父 Run 传播时先子后父；reset 按 Run ID 升序锁被 reset 的 Run 与被取代的
	// current；continue-as-new、重试与 reset 切换 current 时，本事务新建的 Run 只有本事务可见，不参与排序。
	GetRunForUpdate(runID string) (*Run, error)
	// UpdateRun 先取 Run 的 execution state 行锁，再写 Run 经 setter 改过的列。
	UpdateRun(run *Run) error
	// SyncRunSearchAttributes 把本事务 execution state 投影出的搜索属性写回 Run，只更新这一列。
	SyncRunSearchAttributes(runID string) error
	// GetWorkflowExecutionStateForUpdate 在事务内带行锁读 execution state，结果包含本事务已做的写入。
	GetWorkflowExecutionStateForUpdate(runID string) (*WorkflowExecutionState, error)
	// BeginWorkflowActivation 在事务内加锁的 execution state 上执行 BeginActivation，语义见它；
	// 返回 true 时激活随事务提交。
	BeginWorkflowActivation(runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error)
	// HistoryLimits 是存储对每个 Run 的 History 执行的上限；WorkflowTaskStarted 按它给出 continue-as-new 建议。
	HistoryLimits() history.HistoryLimits
	// SetResetRunID 的语义同 Store.SetResetRunID；先取 runID 的 execution state 行锁。
	SetResetRunID(runID, resetRunID string) error
	// SetCurrentRunCAS 把 (namespace, workflowID) 的 current 从 expectedOldRunID 切到 newRunID，compare 不成立时返回
	// false。先取 expectedOldRunID（非空时）与 newRunID 的 execution state 行锁。
	SetCurrentRunCAS(namespace, workflowID, expectedOldRunID, newRunID string) (bool, error)
}
