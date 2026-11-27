package history

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

type CommandType string

// Command 是 Executor replay 编排代码时产出的"意图"，由 Server 处理后落地成对应 Event。
// Command 不持久化、每轮 replay 重新产生；Event 才是落库的事实。括号内是它落地的 Event。
const (
	// ── 1. 发起异步动作（产生一个待推进的开放项）────────────────
	// 这类 Command 落地后会写一个 scheduled/started 事件并触发副作用（投队列/起 timer/建子 Run），
	// 之后等外部结果回来再触发下一轮 WFT。
	CmdScheduleActivity   CommandType = "ScheduleActivity"   // 调度一个 activity（→ ActivityScheduled + 投 activity 队列）
	CmdScheduleTimer      CommandType = "ScheduleTimer"      // 启动一个 durable 定时器（→ TimerScheduled）
	CmdStartChildWorkflow CommandType = "StartChildWorkflow" // 启动子 workflow（→ ChildWorkflowStarted + 建子 Run）

	// ── 取消已发起的异步动作（作用域被取消时产生）────────────────
	CmdRequestCancelActivity      CommandType = "RequestCancelActivity"      // 请求取消 activity（→ ActivityCancelRequested；未开始的立即 ActivityCancelled）
	CmdCancelTimer                CommandType = "CancelTimer"                // 取消 durable 定时器（→ TimerCancelled）
	CmdRequestCancelChildWorkflow CommandType = "RequestCancelChildWorkflow" // 请求取消子 workflow（→ ChildWorkflowCancelRequested + 子 Run 的取消请求）

	// ── 2. 写入确定性/外部交互记录（不阻塞，记完即触发下一轮）──────
	CmdRecordMarker           CommandType = "RecordMarker"           // 固化非确定性值，replay 复用（→ MarkerRecorded）
	CmdCompleteWorkflowUpdate CommandType = "CompleteWorkflowUpdate" // 编排处理完 Update，记录结果（→ WorkflowUpdateCompleted）
	CmdUpsertSearchAttributes CommandType = "UpsertSearchAttributes" // GetVersion 写版本索引，Input 为属性表（→ SearchAttributesUpserted）

	// ── 3. 结束当前 Run（终态，关闭 Run）──────────────────────
	CmdCompleteWorkflow CommandType = "CompleteWorkflow" // 编排正常完成（→ WorkflowCompleted，终态）
	CmdFailWorkflow     CommandType = "FailWorkflow"     // 编排失败（→ WorkflowFailed，终态）
	CmdCancelWorkflow   CommandType = "CancelWorkflow"   // 编排返回取消错误（→ WorkflowCancelled，终态）
	CmdContinueAsNew    CommandType = "ContinueAsNew"    // 截断历史、开下一代 Run（→ WorkflowContinuedAsNew，终态）
)

// ClosesWorkflow 报告命令是否结束当前 Run（第 3 类）。一批命令里它排在最后（对标 Temporal 的
// ValidateCommandSequence：关闭命令必须是最后一条）。
func (t CommandType) ClosesWorkflow() bool {
	switch t {
	case CmdCompleteWorkflow, CmdFailWorkflow, CmdCancelWorkflow, CmdContinueAsNew:
		return true
	}
	return false
}

// Command 是宽结构体：各种命令共用同一个 struct，不同 Type 只填各自相关的字段。
// 下方注释标了每个字段服务于哪种命令。
type Command struct {
	TargetWorkerIdentity string
	Type                 CommandType // 命令类型，决定下面哪些字段有效

	// ScheduleActivity / RequestCancelActivity 用
	ActivityID             string                      // activity 的稳定步骤 ID（去重键）
	ActivityType           string                      // activity 名（路由到对应 worker）
	TaskQueue              string                      // 目标队列
	ScheduleToStartTimeout time.Duration               // 入队到开始执行的超时
	StartToCloseTimeout    time.Duration               // 开始到完成的超时
	ScheduleToCloseTimeout time.Duration               // 入队到完成的总超时
	HeartbeatTimeout       time.Duration               // 心跳超时
	RetryPolicy            *protocol.ParsedRetryPolicy // 重试策略

	TimerID string        // ScheduleTimer / CancelTimer 用：定时器 ID
	Delay   time.Duration // ScheduleTimer 用：延迟时长

	MarkerName string // RecordMarker 用：marker 名（如 "SideEffect"/"GetVersion"）
	MarkerID   string // RecordMarker 用：marker 实例 ID

	Name     string // CompleteWorkflowUpdate 用：update 名
	UpdateID string // CompleteWorkflowUpdate 用：update 实例 ID
	// UpdateOutcome 是 update 完成的唯一结果（成功=Success，失败=Failure）；缺失是协议错误。
	UpdateOutcome *protocol.UpdateOutcome

	WorkflowType string // StartChildWorkflow / ContinueAsNew 用：目标 Workflow Type
	ChildID      string // StartChildWorkflow / RequestCancelChildWorkflow 用：子工作流的稳定 ID
	// ParentClosePolicy 是 StartChildWorkflow 的父关闭策略；空值表示 Terminate。
	ParentClosePolicy protocol.ParentClosePolicy

	Input  map[string]any // 输入载荷（activity input / child input / marker value 等）
	Output map[string]any // 输出载荷（CompleteWorkflow 的结果）
	Error  string         // FailWorkflow 用：失败原因；普通 error 只有它，应用错误另带 WorkflowFailure
	// WorkflowFailure 是 FailWorkflow 的结构化失败；Error 镜像其 Message，普通 error 的 Type 为空。
	WorkflowFailure *protocol.Failure
	// Details 是 CancelWorkflow 的取消错误 details。
	Details protocol.Payloads
}

func ToRetryPolicy(policy *protocol.ParsedRetryPolicy) *RetryPolicy {
	if policy == nil {
		return nil
	}
	return &RetryPolicy{
		MaxAttempts:            policy.MaxAttempts,
		InitialIntervalMs:      policy.InitialInterval.Milliseconds(),
		BackoffCoefficient:     policy.BackoffCoefficient,
		MaxIntervalMs:          policy.MaxInterval.Milliseconds(),
		NonRetryableErrors:     policy.NonRetryableErrors,
		NonRetryableErrorTypes: policy.NonRetryableErrorTypes,
	}
}
