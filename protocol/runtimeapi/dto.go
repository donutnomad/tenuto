package runtimeapi

import (
	"encoding/json"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// RuntimeOKResponse 是无业务返回值端点的统一应答 {"ok":true}。
type RuntimeOKResponse struct {
	OK bool `json:"ok"`
}

// ---- workflow ----

type WorkflowRuntimeRegisterRequest struct {
	Namespace string `json:"namespace,omitempty"`
	// Workflow 是 program.WorkflowConfig 的 JSON。
	Workflow json.RawMessage `json:"workflow"`
	// ActivateIfNew 是启动期自动登记语义：新内容首次出现（或尚无 latest）时激活，
	// 同 hash 重复登记保持 latest。缺省 false 保持「只登记」。
	ActivateIfNew bool `json:"activate_if_new,omitempty"`
}

// WorkflowRuntimeStartRequest 按 Workflow Type 启动 Run。WorkflowID 标识这次执行，
// 留空时服务端生成 UUIDv7 文本并在应答里返回。
type WorkflowRuntimeStartRequest struct {
	Namespace    string         `json:"namespace,omitempty"`
	WorkflowType string         `json:"workflow_type"`
	WorkflowID   string         `json:"workflow_id,omitempty"`
	Input        map[string]any `json:"input,omitempty"`
	// RequestID 是启动的幂等键（至多 128 字节）：Workflow ID 的 current Run 由同一个 RequestID 启动时应答这个 Run。
	RequestID string `json:"request_id,omitempty"`
}

type WorkflowRuntimeStartVersionRequest struct {
	Ref        WorkflowConfigRef `json:"ref"`
	WorkflowID string            `json:"workflow_id,omitempty"`
	Input      map[string]any    `json:"input,omitempty"`
}

type WorkflowRuntimeRunRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	RunID      string `json:"run_id"`
	// ProgramVersion 用于读取 WFT 本次选中的内容快照；空值表示 Run current。
	ProgramVersion string `json:"program_version,omitempty"`
}

// WorkflowRuntimeWaitClosedRequest 是 wait-closed 长轮询：等待 Run 结束。TimeoutMillis 为 0 时按
// DefaultRuntimePollTimeout，超过 MaxRuntimePollTimeout 时按上界；到期时 Run 仍在运行则应答 204 且无 body。
type WorkflowRuntimeWaitClosedRequest struct {
	Namespace     string `json:"namespace,omitempty"`
	WorkflowID    string `json:"workflow_id,omitempty"`
	RunID         string `json:"run_id"`
	TimeoutMillis int64  `json:"timeout_millis,omitempty"`
}

// WorkflowRuntimeWaitClosedResponse 是 Run 结束时 wait-closed 的应答：结束后的 Run 与它的结束事件
// （WorkflowCompleted、WorkflowFailed、WorkflowCancelled、WorkflowTerminated 或 WorkflowContinuedAsNew）。
type WorkflowRuntimeWaitClosedResponse struct {
	Run        *Run          `json:"run"`
	CloseEvent history.Event `json:"close_event"`
}

type WorkflowRuntimeHistoryPageRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	HistoryPageRequest
}

type WorkflowRuntimeListRunsRequest struct {
	Filter RunListFilter `json:"filter"`
}

type WorkflowRuntimeRunStatsRequest struct {
	Namespace string `json:"namespace,omitempty"`
}

type WorkflowRuntimeListConfigsRequest struct {
	Namespace string `json:"namespace,omitempty"`
}

type WorkflowRuntimeListWorkersRequest struct {
	Filter WorkerListFilter `json:"filter"`
}

type WorkflowRuntimeRegisterActivityWorkerRequest struct {
	Worker protocol.ActivityWorkerVersion `json:"worker"`
}

type WorkflowRuntimeHeartbeatActivityWorkerRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Identity  string `json:"identity"`
	BuildID   string `json:"build_id"`
}

type WorkflowRuntimeDeregisterActivityWorkerRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Identity  string `json:"identity"`
	BuildID   string `json:"build_id"`
}

type WorkflowRuntimeSearchAttributesRequest struct {
	Namespace        string            `json:"namespace,omitempty"`
	WorkflowID       string            `json:"workflow_id,omitempty"`
	RunID            string            `json:"run_id"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
}

type WorkflowRuntimeTerminateRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	RunID      string `json:"run_id"`
	Reason     string `json:"reason,omitempty"`
}

type WorkflowRuntimeCancelRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	RunID      string `json:"run_id"`
	Reason     string `json:"reason,omitempty"`
}

// WorkflowRuntimeInteractionRequest 是 signal 与 query 的请求体。
type WorkflowRuntimeInteractionRequest struct {
	Namespace  string         `json:"namespace,omitempty"`
	WorkflowID string         `json:"workflow_id,omitempty"`
	RunID      string         `json:"run_id"`
	Name       string         `json:"name"`
	Input      map[string]any `json:"input,omitempty"`
	// RequestID 是 Signal 的幂等键（至多 128 字节）：同一个 Run 上同一个 RequestID 只记一条 WorkflowSignaled。Query 不用它。
	RequestID string `json:"request_id,omitempty"`
}

// WorkflowRuntimeUpdateRequest 是 update 的请求体：Args 按 handler 业务参数的顺序排列。
type WorkflowRuntimeUpdateRequest struct {
	Namespace  string            `json:"namespace,omitempty"`
	WorkflowID string            `json:"workflow_id,omitempty"`
	RunID      string            `json:"run_id"`
	Name       string            `json:"name"`
	UpdateID   string            `json:"update_id,omitempty"`  // 空则内部生成
	WaitStage  string            `json:"wait_stage,omitempty"` // 空=Accepted，Completed 则长轮询
	Args       protocol.Payloads `json:"args"`
}

type WorkflowRuntimeUpdateWithStartRequest struct {
	Namespace    string            `json:"namespace,omitempty"`
	WorkflowType string            `json:"workflow_type"`
	WorkflowID   string            `json:"workflow_id,omitempty"` // 空则服务端生成
	StartInput   map[string]any    `json:"start_input,omitempty"`
	UpdateName   string            `json:"update_name"`
	UpdateID     string            `json:"update_id,omitempty"` // 空则内部生成
	UpdateArgs   protocol.Payloads `json:"update_args"`
	WaitStage    string            `json:"wait_stage,omitempty"` // 空=Accepted，Completed 则长轮询
}

// WorkflowRuntimeUpdateWithStartResponse 携带接收 Update 的 Run（新启动的或复用的）、它的 Workflow ID 和 Update 结果。
type WorkflowRuntimeUpdateWithStartResponse struct {
	RunID      string                        `json:"run_id"`
	WorkflowID string                        `json:"workflow_id"`
	Result     protocol.WorkflowUpdateResult `json:"result"`
}

type RuntimeCapabilities struct {
	WorkflowCapabilities []string `json:"workflow_capabilities"`
}

type WorkflowRuntimeSignalWithStartRequest struct {
	Namespace    string         `json:"namespace,omitempty"`
	WorkflowType string         `json:"workflow_type"`
	WorkflowID   string         `json:"workflow_id,omitempty"` // 空则服务端生成
	StartInput   map[string]any `json:"start_input,omitempty"`
	SignalName   string         `json:"signal_name"`
	SignalInput  map[string]any `json:"signal_input,omitempty"`
	// RequestID 是 Signal 的幂等键，语义同 WorkflowRuntimeInteractionRequest.RequestID。
	RequestID string `json:"request_id,omitempty"`
}

// WorkflowRuntimeSignalWithStartResponse 携带接收 Signal 的 Run、它的 Workflow ID 和 Signal 的事件 ID。
type WorkflowRuntimeSignalWithStartResponse struct {
	RunID      string `json:"run_id"`
	WorkflowID string `json:"workflow_id"`
	EventID    int64  `json:"event_id"`
}

type WorkflowRuntimeScheduleRequest struct {
	Namespace  string `json:"namespace,omitempty"`
	ScheduleID string `json:"schedule_id"`
}

type WorkflowRuntimeUpdateScheduleRequest struct {
	Namespace  string         `json:"namespace,omitempty"`
	ScheduleID string         `json:"schedule_id"`
	Update     ScheduleUpdate `json:"update"`
}

type WorkflowRuntimeListSchedulesRequest struct {
	Filter ScheduleListFilter `json:"filter"`
}

type WorkflowRuntimeTriggerScheduleResponse struct {
	RunID string `json:"run_id"`
}

type WorkflowRuntimeStartBatchRequest struct {
	Req BatchStartRequest `json:"req"`
}

// WorkflowRuntimeBatchRequest 按 ID 寻址一个批量操作（get-batch、cancel-batch）。
type WorkflowRuntimeBatchRequest struct {
	Namespace   string `json:"namespace,omitempty"`
	OperationID string `json:"operation_id"`
}

type WorkflowRuntimeListBatchesRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Offset    int    `json:"offset,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type WorkflowRuntimeResetRequest struct {
	Namespace           string   `json:"namespace,omitempty"`
	RunID               string   `json:"run_id"`
	Reason              string   `json:"reason,omitempty"`
	ResetType           string   `json:"reset_type,omitempty"`
	ResetEventID        int64    `json:"reset_event_id,omitempty"`
	ReapplyExcludeTypes []string `json:"reapply_exclude_types,omitempty"`
}

// WorkflowRuntimeResetActivityRequest 按 ActivityID 寻址 Run 里一个未关闭的 Activity。
type WorkflowRuntimeResetActivityRequest struct {
	Namespace      string `json:"namespace,omitempty"`
	RunID          string `json:"run_id"`
	ActivityID     string `json:"activity_id"`
	ResetHeartbeat bool   `json:"reset_heartbeat,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

type WorkflowRuntimeListTaskQueuesRequest struct {
	Namespace string `json:"namespace,omitempty"`
}

type WorkflowRuntimeResetResponse struct {
	RunID string `json:"run_id"`
}

type WorkflowRuntimeStartResponse struct {
	RunID      string `json:"run_id"`
	WorkflowID string `json:"workflow_id"`
}

type WorkflowRuntimeSignalResponse struct {
	EventID int64 `json:"event_id"`
}

type WorkflowRuntimeSetRunPolicyRequest struct {
	Namespace string `json:"namespace"`
	RunID     string `json:"run_id"`
	Policy    string `json:"policy"`
}

type WorkflowRuntimeSetWorkerTargetRequest struct {
	Namespace            string `json:"namespace"`
	WorkflowID           string `json:"workflow_id"`
	TargetWorkerIdentity string `json:"target_worker_identity"`
}

// ---- workflow task ----

type WorkflowTaskRuntimeLeaseRequest struct {
	Lease protocol.TaskLease `json:"lease"`
}

type WorkflowTaskRuntimePollRequest struct {
	TimeoutMillis int64 `json:"timeout_millis,omitempty"`
}

// WorkflowTaskRuntimeRunRequest 读取 Workflow Task 所属 Run 的配置、History 或状态。Run 取自 TaskToken：
// 只有持有这个 Workflow Task 租约的 Worker 能读取。
type WorkflowTaskRuntimeRunRequest struct {
	TaskToken      protocol.TaskToken `json:"task_token"`
	ProgramVersion string             `json:"program_version,omitempty"`
}

type WorkflowTaskRuntimeCompleteRequest struct {
	TaskToken      protocol.TaskToken `json:"task_token,omitempty"`
	Commands       []history.Command  `json:"commands,omitempty"`
	WorkerIdentity string             `json:"worker_identity,omitempty"`
}

type WorkflowTaskRuntimeTimedOutRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Error     string             `json:"error,omitempty"`
}

type WorkflowTaskRuntimeFailedRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Cause     string             `json:"cause,omitempty"`
	Reason    string             `json:"reason,omitempty"`
}

type WorkflowTaskRuntimeActivationRequest struct {
	TaskToken           protocol.TaskToken `json:"task_token"`
	AfterEventID        int64              `json:"after_event_id"`
	ExecutionGeneration int64              `json:"execution_generation"`
	ActivationID        string             `json:"activation_id"`
}

type WorkflowTaskRuntimeCompleteActivationRequest struct {
	TaskToken  protocol.TaskToken       `json:"task_token"`
	Activation history.ActivationResult `json:"activation"`
	Commands   []history.Command        `json:"commands"`
}

type WorkflowTaskRuntimeStatusResponse struct {
	Status history.Status `json:"status"`
}

// ---- activity ----

type ActivityRuntimePollRequest struct {
	TaskQueue     string `json:"task_queue"`
	TimeoutMillis int64  `json:"timeout_millis,omitempty"`
}

type ActivityRuntimeLeaseRequest struct {
	Lease protocol.TaskLease `json:"lease"`
}

type ActivityRuntimeTaskRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Task      protocol.Task      `json:"task,omitempty"`
}

type ActivityRuntimeHeartbeatRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Task      protocol.Task      `json:"task,omitempty"`
	Details   map[string]any     `json:"details,omitempty"`
}

// ActivityRuntimeCompleteRequest 是异步完成请求：failure 为 nil 表示成功，非 nil 表示失败。
type ActivityRuntimeCompleteRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Task      protocol.Task      `json:"task,omitempty"`
	Output    map[string]any     `json:"output,omitempty"`
	Failure   *protocol.Failure  `json:"failure,omitempty"`
}

// ActivityRuntimeFailRequest 是失败请求：失败载荷统一用 failure。
type ActivityRuntimeFailRequest struct {
	TaskToken protocol.TaskToken `json:"task_token,omitempty"`
	Task      protocol.Task      `json:"task,omitempty"`
	Failure   *protocol.Failure  `json:"failure,omitempty"`
}
