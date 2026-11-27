package execution

import (
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

const (
	DefaultWorkflowTaskFailureBackoffBase = time.Second
	DefaultWorkflowTaskFailureBackoffCap  = time.Minute
)

// WorkflowTaskCloseResult 是一次 Workflow Task 关闭事件的写入结果，Inserted 为 false 表示该 WFT 已被关闭。
type WorkflowTaskCloseResult struct {
	EventID  int64
	Inserted bool
}

// RecordWorkflowTaskFailure 以 failed（WorkflowTaskFailed）关闭 Workflow Task。失败的 WFT 若在 AutoUpgrade 候选版本上
// started，Run 的 UpgradeError 诊断在同一事务写入。inserted 为 false 表示该 WFT 已被关闭，事务不做修改；
// 返回的 state 包含本次关闭，调用方据它取退避档位。
func RecordWorkflowTaskFailure(tx ExecutionTx, run *Run, treeID, branchID string, failed history.Event) (inserted bool, state *WorkflowExecutionState, err error) {
	attrs := failed.WorkflowTaskFailed
	if attrs == nil {
		return false, nil, fmt.Errorf("workflow task failed event required for run %s", run.ID)
	}
	if _, inserted, err = tx.AppendWorkflowTaskClosed(run.ID, treeID, branchID, failed); err != nil || !inserted {
		return false, nil, err
	}
	if state, err = tx.GetWorkflowExecutionStateForUpdate(run.ID); err != nil {
		return false, nil, err
	}
	if run.RecordUpgradeFailure(state.WorkflowTaskProgramVersion(attrs.ScheduledEventID), attrs.ScheduledEventID, attrs.Cause, attrs.Reason) {
		if err := tx.UpdateRun(run); err != nil {
			return false, nil, err
		}
	}
	return true, state, nil
}

// CloseWorkflowTaskAtHistoryLimit keeps the task completion, the terminal
// failure and the Run's failed status in one transaction.
func CloseWorkflowTaskAtHistoryLimit(tx ExecutionTx, run *Run, treeID, branchID string, closeEvent history.Event, message, failureType string) (WorkflowTaskCompletionResult, error) {
	failed, err := history.NewWorkflowFailedFailureEvent(protocol.Failure{Message: message, Type: failureType}, "")
	if err != nil {
		return WorkflowTaskCompletionResult{}, err
	}
	result, err := tx.AppendWorkflowTaskCompleted(run.ID, treeID, branchID, closeEvent, []history.Event{failed})
	if err != nil || !result.ClosedInserted {
		return result, err
	}
	return result, closeRun(tx, run, history.StatusFailed, nil)
}

func isWorkflowTaskLifecycleEvent(typ history.EventType) bool {
	return typ == history.EventWorkflowTaskScheduled || typ == history.EventWorkflowTaskStarted || history.IsWorkflowTaskCloseEventType(typ)
}

// WorkflowTaskFailedCauseUnhandledEvents 表示一批含终态命令（Complete、Fail、Cancel、ContinueAsNew）的完成
// 遇到了 Worker 没有看到的新事实（对标 Temporal 的 UNHANDLED_COMMAND）。关闭 Workflow 会丢掉这些事实，本次完成
// 不提交，立即重做；它不计入连续失败，也不产生退避。非终态的完成照常提交，新事实留给下一个 Workflow Task。
const WorkflowTaskFailedCauseUnhandledEvents = "unhandled_events"

func isWorkflowActivationEvent(typ history.EventType) bool {
	switch typ {
	case history.EventWorkflowStarted, history.EventActivityCompleted, history.EventActivityFailed, history.EventActivityCancelled, history.EventTimerFired,
		history.EventMarkerRecorded, history.EventWorkflowSignaled,
		history.EventWorkflowUpdateAccepted, history.EventWorkflowUpdateCompleted,
		history.EventChildWorkflowCompleted, history.EventChildWorkflowFailed, history.EventChildWorkflowCancelled,
		history.EventChildWorkflowTerminated, history.EventWorkflowCancellationRequested:
		return true
	default:
		return false
	}
}

func WorkflowTaskFailureBackoff(streak int, base, cap time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultWorkflowTaskFailureBackoffBase
	}
	if cap <= 0 {
		cap = DefaultWorkflowTaskFailureBackoffCap
	}
	if streak < 1 {
		streak = 1
	}
	if base >= cap {
		return cap
	}
	delay := base
	for i := 1; i < streak; i++ {
		if delay >= cap/2 {
			return cap
		}
		delay *= 2
	}
	if delay > cap {
		return cap
	}
	return delay
}

// WorkflowTaskFailedCauseRunResumed 表示暂停的 Run 被 set-policy 恢复：暂停时打开的 Workflow Task
// 被关闭，恢复从一个新的 Workflow Task 开始，选版在新的任务边界重新运行。
const WorkflowTaskFailedCauseRunResumed = "run_resumed"

// WorkflowTaskFailedCauseResetWorkflow 是 Reset 写在新 Run 开头的标记：被 reset 的 Workflow Task
// 由新 Run 重做。
const WorkflowTaskFailedCauseResetWorkflow = "reset_workflow"

// isRedoWorkflowTaskFailure 报告 cause 是否表示「这个 Workflow Task 没有出错，只是要重做」。
// 这类关闭不计入连续失败、不产生退避。
func isRedoWorkflowTaskFailure(cause string) bool {
	return cause == WorkflowTaskFailedCauseUnhandledEvents || cause == WorkflowTaskFailedCauseRunResumed ||
		cause == WorkflowTaskFailedCauseResetWorkflow
}
