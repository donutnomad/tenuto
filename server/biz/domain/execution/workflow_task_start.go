package execution

import (
	"errors"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

// WorkflowTaskStartToCloseTimeout 是 Server 为一次 Workflow Task 领取计时的 start-to-close 超时：领取它的 Worker
// 声明的超时，Worker 没有声明（非正）时用 protocol.DefaultWorkflowTaskTimeout。
func WorkflowTaskStartToCloseTimeout(declared time.Duration) time.Duration {
	if declared <= 0 {
		return protocol.DefaultWorkflowTaskTimeout
	}
	return declared
}

// WorkflowTaskStart 描述一次 Workflow Task 领取。ExecutionGeneration、StickyGeneration 与 ActivationID 标识
// 这次激活，按 BeginActivation 的规则校验。ProgramVersion 是调用方在事务之前按 Run 的读取结果为这次领取准备的
// Program 版本，Candidate 是它所依据的升级候选。Timeout 是这次领取的 start-to-close 超时，从 Now 起算。
type WorkflowTaskStart struct {
	RunID, TreeID, BranchID string
	ScheduledEventID        int64
	ExecutionGeneration     int64
	StickyGeneration        int64
	ActivationID            string
	WorkerIdentity          string
	WorkerBuildID           string
	ProgramVersion          string
	Candidate               UpgradeCandidate
	Timeout                 time.Duration
	Now                     time.Time
}

// WorkflowTaskStartResult 是一次领取的结果。
type WorkflowTaskStartResult struct {
	// Started 为 true 时激活与 WorkflowTaskStarted 随事务提交。
	Started bool
	// Choice 是事务内按 ChooseWorkflowTaskProgram 确定的 Program 版本。ConfigError 非空表示 Run 已暂停；
	// Version 与 WorkflowTaskStart.ProgramVersion 不同表示调用方准备的版本已过时。两种情况都不开始激活。
	Choice ProgramVersionChoice
}

// StartWorkflowTask 在事务 tx 里开始 Workflow Task 的一次激活并追加它的 WorkflowTaskStarted 事件。
// 先按 ChooseWorkflowTaskProgram 在锁下确定 Program 版本：Run 已暂停（或本次选版暂停了它）、或确定的版本与
// start.ProgramVersion 不同时不开始激活，除选版暂停之外事务不做修改。激活被拒绝时（WFT 已关闭、代数已变、另一个
// 激活正在进行）Started 为 false，事务不做修改。激活与 Started 事件一起提交：不会留下没有 start-to-close 截止时间
// 的激活。Started 事件带事务内 execution state 的 History 快照、按存储上限得出的 continue-as-new 建议与这次领取的
// 超时，execution state 由它得出截止时间。
func StartWorkflowTask(tx ExecutionTx, start WorkflowTaskStart) (WorkflowTaskStartResult, error) {
	choice, err := ChooseWorkflowTaskProgram(tx, start.RunID, start.ScheduledEventID, start.Candidate)
	if err != nil {
		return WorkflowTaskStartResult{}, err
	}
	result := WorkflowTaskStartResult{Choice: choice}
	if choice.ConfigError != "" || choice.Version != start.ProgramVersion {
		return result, nil
	}
	begun, err := tx.BeginWorkflowActivation(start.RunID, start.ScheduledEventID, start.ExecutionGeneration, start.StickyGeneration, start.ActivationID)
	if err != nil || !begun {
		return result, err
	}
	state, err := tx.GetWorkflowExecutionStateForUpdate(start.RunID)
	if err != nil {
		return WorkflowTaskStartResult{}, err
	}
	metrics := history.HistoryMetrics{EventCount: state.HistoryEventCount, SizeBytes: state.HistorySizeBytes}
	started := history.NewWorkflowTaskStartedEvent(start.ScheduledEventID, start.WorkerIdentity, start.WorkerBuildID, 0)
	started.Timestamp = start.Now
	started.WorkflowTaskStarted.ProgramVersion = start.ProgramVersion
	started.WorkflowTaskStarted.TimeoutMs = start.Timeout.Milliseconds()
	if err := history.PopulateWorkflowTaskStartedHistoryInfo(&started, state.NextEventID, metrics, tx.HistoryLimits()); err != nil {
		return WorkflowTaskStartResult{}, err
	}
	if _, _, err := tx.AppendIdempotent(start.RunID, start.TreeID, start.BranchID, started); err != nil {
		return WorkflowTaskStartResult{}, err
	}
	result.Started = true
	return result, nil
}

// WorkflowTaskTimeout 是一次 Workflow Task 超时：Worker 上报的，或 Server 计时到期的（ServerTimer 为 true）。
type WorkflowTaskTimeout struct {
	RunID, TreeID, BranchID string
	ScheduledEventID        int64
	Message                 string
	Now                     time.Time
	ServerTimer             bool
}

// TimeoutWorkflowTask 记 WorkflowTaskTimedOut 关闭仍 open 的 WFT。Server 计时到期时，先在持锁读到的
// execution state 上确认该 WFT 已 started 且超过 start-to-close 超时：定时器可能比它所依据的状态活得久，
// WFT 已被 Worker 关闭、或被再次领取推迟了截止时间时，事务不做任何修改。
func TimeoutWorkflowTask(tx ExecutionTx, timeout WorkflowTaskTimeout) (WorkflowTaskCloseResult, error) {
	state, err := tx.GetWorkflowExecutionStateForUpdate(timeout.RunID)
	if err != nil || state.IsTerminal() || !state.WorkflowTaskOpen(timeout.ScheduledEventID) {
		return WorkflowTaskCloseResult{}, err
	}
	if timeout.ServerTimer && !state.WorkflowTaskTimedOutAt(timeout.ScheduledEventID, timeout.Now) {
		return WorkflowTaskCloseResult{}, nil
	}
	event := history.NewWorkflowTaskTimedOutEvent(timeout.ScheduledEventID, timeout.Message)
	event.Timestamp = timeout.Now
	eventID, inserted, err := tx.AppendWorkflowTaskClosed(timeout.RunID, timeout.TreeID, timeout.BranchID, event)
	if err != nil {
		return WorkflowTaskCloseResult{}, err
	}
	return WorkflowTaskCloseResult{EventID: eventID, Inserted: inserted}, nil
}

// WorkflowTaskPrepareFailure 是准备一次 Workflow Task 投递失败后的处理方式。
type WorkflowTaskPrepareFailure int

const (
	// PrepareFailureRetry：失败是暂时的（存储、超时、并发冲突、当前 Worker 的能力不够）或无法判定。投递不确认，
	// 由队列重投（租约到期或已被 Nack），下一次领取重新准备。
	PrepareFailureRetry WorkflowTaskPrepareFailure = iota
	// PrepareFailurePauseRun：失败由 Run 自己的配置或数据决定，每次准备都会重现。Run 以 ConfigError 暂停并确认这份
	// 投递；修复后由 set-policy 恢复，恢复时调度新的 Workflow Task。
	PrepareFailurePauseRun
	// PrepareFailureDropTask：Run 已不存在。确认丢弃这份投递。
	PrepareFailureDropTask
)

// ClassifyWorkflowTaskPrepareError 给准备 Workflow Task（领取 lease、读取 Run 与 execution state、选版、读取 Program、
// 写入 WorkflowTaskStarted）时的错误分类。确定性失败只认下面列出的错误，其余一律按暂时性失败重试：
//   - Run 不存在（ErrRunNotFound）：丢弃任务；
//   - 选中的 Program 版本读不到（definition.ErrWorkflowConfigNotFound）、
//     Run 的 branch token 损坏（ErrInvalidBranchToken）：暂停 Run；
//   - Worker 不支持 Program 要求的能力（*program.UnsupportedWLangCapabilityError）取决于领取的 Worker，按暂时性处理。
func ClassifyWorkflowTaskPrepareError(err error) WorkflowTaskPrepareFailure {
	var unsupported *program.UnsupportedWLangCapabilityError
	switch {
	case err == nil, errors.As(err, &unsupported):
		return PrepareFailureRetry
	case errors.Is(err, ErrRunNotFound):
		return PrepareFailureDropTask
	case errors.Is(err, definition.ErrWorkflowConfigNotFound), errors.Is(err, ErrInvalidBranchToken):
		return PrepareFailurePauseRun
	}
	return PrepareFailureRetry
}
