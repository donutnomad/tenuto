package protocol

import "unicode/utf8"

// 标识的最大字符数，取存储它的最窄一列的宽度（MySQL VARCHAR(n) 按字符计）。超长的标识在入口以 ErrInvalidRequest
// 拒绝；Workflow 命令里的标识在 Workflow Task 提交时校验，超长时这次 Workflow Task 失败、Run 显示 Blocked。
const (
	// MaxNamespaceLength 受 tenuto_activity_worker_versions.namespace 限制。
	MaxNamespaceLength = 64
	// MaxWorkflowIDLength 同时约束子 Workflow 的 ID（子 Run 的 workflow_id 与父 Run 记录的 child_id）。
	MaxWorkflowIDLength   = 191
	MaxWorkflowTypeLength = 191
	MaxActivityIDLength   = 191
	// MaxActivityTypeLength 受 tenuto_activity_worker_versions.activity_type 限制。
	MaxActivityTypeLength = 128
	// MaxTaskQueueLength 受 tenuto_activity_worker_versions.task_queue 限制。
	MaxTaskQueueLength      = 64
	MaxTimerIDLength        = 191
	MaxUpdateIDLength       = 191
	// MaxMarkerNameLength 与 MaxMarkerIDLength 让 MarkerRecorded 的去重键（"marker_recorded:<name>:<id>"）不超过
	// tenuto_events.dedupe_key 的 400 字符。
	MaxMarkerNameLength = 191
	MaxMarkerIDLength   = 191
	// MaxRequestIDLength 是启动与 Signal 请求幂等键的最大字节数（不是字符数），WorkflowSignaled 的去重键
	// （"workflow_signaled:<request id>"）因此不超过 tenuto_events.dedupe_key 的宽度。
	MaxRequestIDLength = 128
	MaxWorkerIdentityLength = 128
	MaxBuildIDLength        = 64
	// MaxScheduleDescriptionLength 与 MaxCronExpressionLength 是 tenuto_schedules 对应列的宽度。
	MaxScheduleDescriptionLength = 191
	MaxCronExpressionLength      = 191
	MaxTimezoneLength            = 64
	// MaxBatchReasonLength 是 tenuto_batch_operations.reason 的宽度。
	MaxBatchReasonLength = 1024
)

// CheckLength 在 value 超过 limit 个字符时返回 ErrInvalidRequest，field 是错误文本里的字段名。
// 多个字段用 errors.Join 合并检查：全部合规时结果为 nil。
func CheckLength(field, value string, limit int) error {
	if n := utf8.RuneCountInString(value); n > limit {
		return InvalidRequestf("%s has %d characters, the limit is %d", field, n, limit)
	}
	return nil
}
