package protocol

import "time"

// ActivityTaskBeginResult 是开始一次 Activity attempt 的结果。Execute 为 true 时 StartedTime 是
// Server 记下的 attempt 开始时间。
type ActivityTaskBeginResult struct {
	Execute     bool      `json:"execute"`
	Reason      string    `json:"reason,omitempty"`
	StartedTime time.Time `json:"started_time,omitzero"`
}

// ActivityCancellationResult 告诉执行中的 attempt 是否应该停下。Reason 为 ActivityCancelRequestedReason 时
// Workflow 请求取消了这个 Activity，Activity 结束时应确认取消（返回取消错误或 context.Canceled）；其余原因
// （Run 已关闭、attempt 已过期或 Activity 已结束）表示结果不再有接收方。
type ActivityCancellationResult struct {
	CancelRequested bool   `json:"cancel_requested"`
	Reason          string `json:"reason,omitempty"`
}

// ActivityCancelRequestedReason 是 Workflow 请求取消 Activity 时 ActivityCancellationResult 的 Reason。
const ActivityCancelRequestedReason = "activity_cancel_requested"

type ActivityHeartbeatResult = ActivityCancellationResult
