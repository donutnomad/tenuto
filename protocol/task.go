package protocol

import "time"

type TaskType string

const (
	TaskTypeWorkflow TaskType = "workflow"
	TaskTypeActivity TaskType = "activity"
)

type TaskToken string

type WorkflowTaskRoute string

const (
	WorkflowTaskRouteNormal WorkflowTaskRoute = "normal"
	WorkflowTaskRouteSticky WorkflowTaskRoute = "sticky"
)

type Task struct {
	TargetWorkerIdentity string   `json:"target_worker_identity,omitempty"`
	Type                 TaskType `json:"type"`
	Namespace            string   `json:"namespace,omitempty"`
	WorkflowID           string   `json:"workflow_id,omitempty"`
	RunID                string   `json:"run_id"`
	WorkflowType         string   `json:"workflow_type,omitempty"`
	ProgramVersion       string   `json:"program_version,omitempty"`
	ActivityID           string   `json:"activity_id,omitempty"`
	ScheduledEventID     int64    `json:"scheduled_event_id"`
	Attempt              int      `json:"attempt,omitempty"`
	Stamp                int64    `json:"stamp,omitempty"`
	ActivityType         string   `json:"activity_type,omitempty"`
	TaskQueue            string   `json:"task_queue,omitempty"`
	WorkerIdentity       string   `json:"worker_identity,omitempty"`
	WorkerBuildID        string   `json:"worker_build_id,omitempty"`
	// RequestID 是 Activity Worker 为一份投递生成的请求标识，随 BeginActivityTask 发送：同一 RequestID 的重发
	// 得到原来的开始结果。它和 WorkerIdentity 一样只在请求里，不进 task token。
	RequestID                     string            `json:"request_id,omitempty"`
	ArtifactFingerprint           string            `json:"artifact_fingerprint,omitempty"`
	ExecutorABIVersion            string            `json:"executor_abi_version,omitempty"`
	ExecutionGeneration           int64             `json:"execution_generation,omitempty"`
	StickyGeneration              int64             `json:"sticky_generation,omitempty"`
	ActivationID                  string            `json:"activation_id,omitempty"`
	WorkflowTaskRoute             WorkflowTaskRoute `json:"workflow_task_route,omitempty"`
	StickyTarget                  string            `json:"sticky_target,omitempty"`
	StickyScheduleToStartDeadline time.Time         `json:"sticky_schedule_to_start_deadline,omitzero"`
	EnqueuedAt                    time.Time         `json:"enqueued_at,omitzero"`
	// ScheduledTime 是 Activity 首次被调度的时间（ActivityScheduled 事件的时间）。
	ScheduledTime time.Time      `json:"scheduled_time,omitzero"`
	Input         map[string]any `json:"input,omitempty"`
	TaskToken     TaskToken      `json:"task_token,omitempty"`
	// 四个超时以纳秒计。
	ScheduleToStartTimeout time.Duration     `json:"schedule_to_start_timeout,omitempty"`
	StartToCloseTimeout    time.Duration     `json:"start_to_close_timeout,omitempty"`
	ScheduleToCloseTimeout time.Duration     `json:"schedule_to_close_timeout,omitempty"`
	HeartbeatTimeout       time.Duration     `json:"heartbeat_timeout,omitempty"`
	LastHeartbeatDetails   map[string]any    `json:"last_heartbeat_details,omitempty"`
	TraceContext           map[string]string `json:"trace_context,omitempty"`
}

// TaskLease 是队列交给 Worker 的一次任务租约。Token 是队列的租约凭据，Ack/Nack 时原样带回；
// Server 签发的 task token 在 Task.TaskToken 上。
type TaskLease struct {
	Task  Task   `json:"task"`
	Token string `json:"token"`
}
