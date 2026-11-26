package protocol

// Failure 是应用错误的唯一结构化表示。Activity 失败、Workflow 终态、Update outcome
// 与客户端读取共用同一形状和同一 JSON 编码：
//
//	{"message":"card has expired","type":"CardExpired",
//	 "details":[{"encoding":"json/plain-v1","data":"ImNhcmQtMSI="}],
//	 "cause":{"message":"inner","type":"InnerType","nonRetryable":true}}
//
// Message 参与既有的 message 子串重试策略；Type 参与 NonRetryableErrorTypes 的完全
// 相等匹配；NonRetryable 是显式开关，优先于两条列表策略；Details 是已编码载荷；
// Cause 是逐层独立的嵌套失败。TimeoutType 非空表示这是一次超时，值是超时种类。
type Failure struct {
	Message      string      `json:"message"`
	Type         string      `json:"type,omitempty"`
	TimeoutType  TimeoutType `json:"timeoutType,omitempty"`
	NonRetryable bool        `json:"nonRetryable,omitempty"`
	Details      Payloads    `json:"details,omitempty"`
	Cause        *Failure    `json:"cause,omitempty"`
}

// Validate 校验 details 载荷。
func (f Failure) Validate() error {
	_, err := f.Normalize()
	return err
}

// Normalize 返回标准化后的 Failure：details 逐项 canonical，cause 递归标准化。
// 载荷大小沿用既有 payload 上限与事件大小预算，不另设截断规则。
func (f Failure) Normalize() (Failure, error) {
	details, err := NormalizePayloads(f.Details)
	if err != nil {
		return Failure{}, err
	}
	out := Failure{
		Message:      f.Message,
		Type:         f.Type,
		TimeoutType:  f.TimeoutType,
		NonRetryable: f.NonRetryable,
		Details:      details,
	}
	if f.Cause != nil {
		cause, err := f.Cause.Normalize()
		if err != nil {
			return Failure{}, err
		}
		out.Cause = &cause
	}
	return out, nil
}

// CanceledFailureType 是取消错误的 Failure.Type：Workflow 以取消错误结束、Activity 确认取消时使用，
// 对标 Temporal 的 CanceledFailureInfo。
const CanceledFailureType = "CanceledError"

// ResetWorkflowFailureType 是 Workflow Reset 打断已开始的 Activity 时写入的 Failure.Type，不可重试，
// 对标 Temporal 的 ResetWorkflowFailureInfo。
const ResetWorkflowFailureType = "ResetWorkflowError"

// TimeoutType 是超时的种类，取值与 Temporal 的 TimeoutType 同名。
type TimeoutType string

const (
	TimeoutTypeScheduleToStart TimeoutType = "ScheduleToStart"
	TimeoutTypeStartToClose    TimeoutType = "StartToClose"
	TimeoutTypeScheduleToClose TimeoutType = "ScheduleToClose"
	TimeoutTypeHeartbeat       TimeoutType = "Heartbeat"
	// TimeoutTypeWorkflowExecution 与 TimeoutTypeWorkflowRun 是 Workflow 的执行超时与单个 Run 的超时，
	// 记在 WorkflowFailed 事件的 timeoutType 上。
	TimeoutTypeWorkflowExecution TimeoutType = "Execution"
	TimeoutTypeWorkflowRun       TimeoutType = "Run"
)
