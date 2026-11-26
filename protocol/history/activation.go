package history

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
)

// Worker 向 Server 声明的 Workflow 能力，以及执行器 ABI 版本。
const (
	WorkflowCapabilityIncrementalActivationV1 = "incremental-activation-v1"
	WorkflowCapabilityStickyExecutionV1       = "sticky-execution-v1"
	ExecutorABIVersionV1                      = "workflow-executor/v1"
)

// WorkflowActivation 是 Server 交给 Worker 的一次增量激活：从 Worker 游标之后到 ThroughEventID 的事件。
type WorkflowActivation struct {
	ActivationID        string
	ExecutionGeneration int64
	Events              []Event
	ThroughEventID      int64
}

// ActivationResult 是 Worker 执行一次激活的产出。
type ActivationResult struct {
	ActivationID            string
	ExecutionGeneration     int64
	Commands                []Command
	CommandDigest           string
	AppliedThroughEventID   int64
	ActivatedThroughEventID int64
}

// WorkflowTaskCompletionAck 是 Server 对一次激活完成上报的确认。
type WorkflowTaskCompletionAck struct {
	Accepted                bool
	CommittedThroughEventID int64
	CommittedEvents         []Event
	ActivationID            string
	CommandDigest           string
	ExecutionGeneration     int64
	StickyGeneration        int64
	Terminal                bool
}

type WorkflowActivationRequest struct {
	Task                protocol.Task
	AfterEventID        int64
	ExecutionGeneration int64
	ActivationID        string
}

type WorkflowTaskCompletionRequest struct {
	Task       protocol.Task
	Activation ActivationResult
	Commands   []Command
}

// ColdRestoreRequiredError 表示 Server 无法提供增量激活，Worker 需要从完整历史恢复。
type ColdRestoreRequiredError struct {
	Reason string
}

func (e *ColdRestoreRequiredError) Error() string {
	if e.Reason == "" {
		return "cold workflow restore required"
	}
	return "cold workflow restore required: " + e.Reason
}

// Is 把它归为请求方可处理的失败：Worker 改用完整历史恢复后重试。
func (e *ColdRestoreRequiredError) Is(target error) bool { return target == protocol.ErrInvalidRequest }

func IsColdRestoreRequired(err error) bool {
	var target *ColdRestoreRequiredError
	return errors.As(err, &target)
}

// WorkflowActivationID 返回 task 的激活 ID；task 未携带时由 Run、调度事件与 task token 推导。
func WorkflowActivationID(task protocol.Task) string {
	if task.ActivationID != "" {
		return task.ActivationID
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", task.RunID, task.ScheduledEventID, task.TaskToken)))
	return "activation:" + hex.EncodeToString(sum[:16])
}

// WorkflowTaskFailedCauseNonDeterministic 是重放与历史不一致时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseNonDeterministic = "non_deterministic_error"

// WorkflowTaskFailedCauseSessionInvalid 是 Worker 的执行 session 校验失败（session 状态、activation 增量或 history
// 形状）时 WorkflowTaskFailed 的 cause：Worker 丢弃 session，下一次从 Event History 恢复。
const WorkflowTaskFailedCauseSessionInvalid = "execution_session_invalid"

// WorkflowTaskFailedCauseWorkflowPanic 是 Workflow 执行 panic（解释器 panic，或 Workflow 代码的越界、nil 解引用、
// 显式 panic）时 WorkflowTaskFailed 的 cause：Run 保持 Running 并按 WFT 失败退避重试，修复代码后继续（Temporal
// Go SDK 缺省的 BlockWorkflow panic 策略，WORKFLOW_WORKER_UNHANDLED_FAILURE）。
const WorkflowTaskFailedCauseWorkflowPanic = "workflow_panic"

// IsWorkerWorkflowTaskFailureCause 报告 cause 是否是 Worker 可以上报的 Workflow Task 失败原因。其余原因（如
// unhandled_events、run_resumed、reset_workflow）只由 Server 写入，Worker 上报时被拒绝。
func IsWorkerWorkflowTaskFailureCause(cause string) bool {
	return cause == WorkflowTaskFailedCauseNonDeterministic || cause == WorkflowTaskFailedCauseSessionInvalid ||
		cause == WorkflowTaskFailedCauseWorkflowPanic
}
