export type RunStatus =
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'terminated'
  | 'continued_as_new'

// BlockedInfo 是 run 被 BlockWorkflow 卡住的派生描述（后端历史尾部形态的纯函数，非新 RunStatus）。
export interface BlockedInfo {
  blocked: boolean
  consecutive_failures?: number
  cause?: string
  reason?: string
  since?: string
}

// Run 对应 server/workflow/run/run.go 的 Run JSON；[]byte 字段在 JSON 里是 base64 字符串。
export interface Run {
  id: string
  namespace: string
  workflow_id: string
  workflow_type?: string
  workflow_config_version: string
  // AutoUpgrade/Pinned 审计字段：current 用版本字段表达，original 保留启动来源。
  original_workflow_config_version?: string
  policy?: string
  policy_source?: string
  target_worker_identity?: string
  upgrade_error?: string
  config_error?: string
  parent_run_id?: string
  parent_child_id?: string
  parent_scheduled_event_id?: number
  first_run_id: string
  continued_run_id?: string
  reset_run_id?: string
  branch_token?: string
  status: RunStatus
  input?: Record<string, unknown>
  output?: Record<string, unknown>
  search_attributes?: Record<string, string>
  created_at: string
  deadline_at?: string
  run_deadline_at?: string
  attempt?: number
  initiator?: string
  is_current?: boolean
  // blocked 是派生标记（get-run 响应附带），status 仍为 running；非持久化字段。
  blocked?: BlockedInfo
  // terminal_failure 是失败 Run 的结构化终态失败（get-run 响应附带）。
  terminal_failure?: Failure
}

export interface WaitingState {
  type?: string
  activity_id?: string
  child_id?: string
  activity_type?: string
  task_queue?: string
  timer_id?: string
  child_run_id?: string
  scheduled_event_id?: number
}

export interface RunListItem {
  run_id: string
  namespace: string
  workflow_id: string
  workflow_type: string
  status: RunStatus
  workflow_config_version: string
  parent_run_id?: string
  created_at: string
  // closed_at 是 Run 进入终态的时间（RFC3339）；Run 未结束时缺省。
  closed_at?: string
  search_attributes: Record<string, string> | null
  current_waiting?: WaitingState
  reset_run_id?: string
  is_reset?: boolean
  failure_reason: string
  history_length: number
  // blocked 是派生标记（list 响应附带），仅 blocked 的 run 携带；status 仍为 running。
  blocked?: BlockedInfo
}

export interface RunListResult {
  items: RunListItem[]
  total: number
}

export interface WorkflowConfigSummary {
  target_worker_identity?: string
  target_worker_status?: string
  workflow_id: string
  namespace: string
  name?: string
  latest_version: string
  policy?: string
  workflow_version?: string
  version_count: number
  activity_count: number
  active: boolean
}

export interface WorkflowConfigListResult {
  items: WorkflowConfigSummary[]
  total: number
}

export interface WorkerListItem {
  namespace?: string
  app?: string
  identity?: string
  build_id?: string
  task_queues?: string[]
  activity_types?: string[]
  default?: boolean
  weight?: number
  last_heartbeat_at?: string
  expires_at?: string
  sticky_cache?: StickyCacheStats
}

// StickyCacheStats 对应 protocol.ExecutionCacheSnapshot；字段全部可选，缺失时按 0 展示。
export interface StickyCacheStats {
  entries?: number
  estimated_bytes?: number
  hits?: number
  misses?: number
  admissions?: number
  rejected?: number
  evictions?: Record<string, number>
}

export interface WorkerListResult {
  items: WorkerListItem[]
  total: number
}

export interface RetryPolicy {
  maxAttempts?: number
  initialIntervalMs?: number
  backoffCoefficient: number
  maxIntervalMs?: number
  nonRetryableErrors?: string[]
  nonRetryableErrorTypes?: string[]
}

// Payload / Failure / UpdateOutcome 对应 protocol 包；Payload.data 是 base64 字符串。
export interface Payload {
  encoding: string
  data: string
}

export interface Failure {
  message: string
  type?: string
  timeoutType?: string
  nonRetryable?: boolean
  details?: Payload[]
  cause?: Failure
}

export interface UpdateOutcome {
  success?: Payload
  failure?: Failure
}

// Event 对应 server/workflow/run/event.go 的 Event JSON：每条事件只携带一个事件字段。
// Go 侧带 omitempty 的字段在零值时不出现，这里一律标为可选。
export interface Event {
  id: number
  timestamp: string

  workflowStarted?: {
    workflowId?: string
    input?: Record<string, unknown>
    firstRunId?: string
    continuedRunId?: string
    name?: string
    executionTimeoutMs?: number
    attempt?: number
    initiator?: string
    searchAttributes: Record<string, string> | null
  }
  workflowCompleted?: { output?: Record<string, unknown> }
  workflowFailed?: {
    error?: string
    failure?: Failure
    timeoutType?: string
    newExecutionRunId?: string
    retryState?: string
  }
  workflowCancelled?: { details?: Payload[] }
  workflowTerminated?: { reason?: string }
  workflowContinuedAsNew?: {
    name?: string
    nextRunId?: string
    workflowConfigVersion?: string
    input?: Record<string, unknown>
  }

  workflowTaskScheduled?: { timeoutMs?: number; attempt?: number; traceContext?: Record<string, string> }
  workflowTaskStarted?: {
    scheduledEventId?: number
    workerIdentity?: string
    workerBuildId?: string
    programVersion?: string
    historyEventCount?: number
    historySizeBytes?: number
    continueAsNewReasons?: string[]
  }
  workflowTaskCompleted?: { scheduledEventId?: number; workerIdentity?: string }
  workflowTaskTimedOut?: { scheduledEventId?: number; error?: string }
  workflowTaskFailed?: {
    scheduledEventId?: number
    startedEventId?: number
    cause?: string
    baseRunId?: string
    resetRunId?: string
    reason?: string
    traceContext?: Record<string, string>
  }

  activityScheduled?: {
    activityId?: string
    activityType?: string
    taskQueue?: string
    input?: Record<string, unknown>
    traceContext?: Record<string, string>
    scheduleToStartTimeoutMs?: number
    startToCloseTimeoutMs?: number
    scheduleToCloseTimeoutMs?: number
    heartbeatTimeoutMs?: number
    retryPolicy?: RetryPolicy
  }
  activityStarted?: {
    activityId?: string
    scheduledEventId?: number
    attempt?: number
    stamp?: number
    activityType?: string
    taskQueue?: string
    workerIdentity?: string
    workerBuildId?: string
    lastError?: string
  }
  activityAttemptFailed?: {
    activityId?: string
    scheduledEventId?: number
    attempt?: number
    stamp?: number
    error?: string
    failure?: Failure
  }
  activityTimedOut?: {
    activityId?: string
    scheduledEventId?: number
    startedEventId?: number
    attempt?: number
    stamp?: number
    activityType?: string
    taskQueue?: string
    timeoutType?: 'ScheduleToStart' | 'StartToClose' | 'ScheduleToClose' | 'Heartbeat'
    retryState?: string
    failure?: Failure
    // 超时时 Activity 状态里最后一次心跳的详情；心跳本身不写 History。
    lastHeartbeatDetails?: Record<string, unknown>
  }
  activityCompleted?: {
    activityId?: string
    scheduledEventId?: number
    startedEventId?: number
    attempt?: number
    activityType?: string
    taskQueue?: string
    identity?: string
    output?: Record<string, unknown>
  }
  activityFailed?: {
    activityId?: string
    scheduledEventId?: number
    startedEventId?: number
    attempt?: number
    activityType?: string
    taskQueue?: string
    identity?: string
    error?: string
    retryState?: string
    failure?: Failure
  }
  activityCancelRequested?: { activityId?: string; scheduledEventId?: number }
  activityCancelled?: {
    activityId?: string
    scheduledEventId?: number
    startedEventId?: number
    cancelRequestedEventId?: number
    attempt?: number
    activityType?: string
    taskQueue?: string
    identity?: string
    details?: Payload[]
  }
  activityReset?: {
    activityId?: string
    scheduledEventId?: number
    attempt?: number
    identity?: string
    reason?: string
    resetHeartbeat?: boolean
  }

  timerScheduled?: {
    timerId?: string
    delayInput?: Record<string, unknown>
    delayMs?: number
    traceContext?: Record<string, string>
  }
  timerFired?: { timerId?: string; scheduledEventId?: number }
  timerCancelled?: { timerId?: string; scheduledEventId?: number }

  childWorkflowStarted?: {
    name?: string
    childId?: string
    childRunId?: string
    workflowConfigVersion?: string
    input?: Record<string, unknown>
    parentClosePolicy?: 'terminate' | 'request_cancel' | 'abandon'
  }
  childWorkflowCompleted?: {
    childId?: string
    childRunId?: string
    scheduledEventId?: number
    output?: Record<string, unknown>
  }
  childWorkflowFailed?: {
    childId?: string
    childRunId?: string
    scheduledEventId?: number
    error?: string
    failure?: Failure
  }
  childWorkflowCancelled?: { childId?: string; childRunId?: string; scheduledEventId?: number }
  childWorkflowTerminated?: { childId?: string; childRunId?: string; scheduledEventId?: number; reason?: string }
  childWorkflowCancelRequested?: { childId?: string; childRunId?: string; scheduledEventId?: number }

  workflowSignaled?: { name?: string; input?: Record<string, unknown> }
  workflowUpdateAccepted?: { name?: string; updateId?: string; args?: Payload[] }
  workflowUpdateCompleted?: { name?: string; updateId?: string; outcome?: UpdateOutcome }

  workflowCancellationRequested?: { cause?: string }
  markerRecorded?: { markerName?: string; markerId?: string; value?: Record<string, unknown> }
  searchAttributesUpserted?: { attributes?: Record<string, unknown> }
}

// EventType 是事件字段名（camelCase），也是 Console 里事件类型的标识。
export type EventType = Exclude<keyof Event, "id" | "timestamp">

export interface HistoryPage {
  events: Event[]
  next_page_token?: string
  snapshot_max_event_id: number
  history_event_count: number
  history_size_bytes: number
}

export interface ActivityState {
  targetWorkerIdentity?: string
  runId: string
  scheduledEventId: number
  activityId: string
  activityType: string
  taskQueue: string
  attempt: number
  stamp: number
  startedEventId: number
  activityReset: boolean
  cancelRequested?: boolean
  status: 'scheduled' | 'started' | 'completed' | 'failed' | 'timed_out' | 'cancelled'
  startedTime?: string
  workerIdentity?: string
  workerBuildId?: string
  lastHeartbeat?: Record<string, unknown>
  lastHeartbeatAt?: string
  retryLastFailure?: string
  nextAttemptAt?: string
  scheduledTime: string
  input?: Record<string, unknown>
  scheduleToStartTimeoutMs?: number
  startToCloseTimeoutMs?: number
  scheduleToCloseTimeoutMs?: number
  heartbeatTimeoutMs?: number
  retryPolicy?: RetryPolicy
}

export interface Step {
  seq: number
  stepID: string
  activityType: string
  status: 'scheduled' | 'running' | 'completed' | 'failed' | 'timed_out' | 'cancelled' | 'terminated'
  // currentAttempt、nextAttemptAt 与 lastFailure 取自 activity-states：运行中的 Activity 以它为准。
  currentAttempt?: number
  nextAttemptAt?: string
  lastFailure?: string
  activityReset?: boolean
  taskQueue: string
  input: Record<string, unknown> | null
  output: Record<string, unknown> | null
  error: string | null
  scheduledAt: string
  startedAt: string | null
  completedAt: string | null
  duration: number | null
  workerIdentity: string
  workerBuildID: string
  attempts: Attempt[]
  heartbeat: Record<string, unknown> | null
  childRunID: string | null
  scheduledEventID: number
  startedEventID?: number
  completedEventID?: number
  scheduleToStartTimeoutMs?: number
  startToCloseTimeoutMs?: number
  scheduleToCloseTimeoutMs?: number
  heartbeatTimeoutMs?: number
  retryPolicy?: RetryPolicy
}

export interface Attempt {
  attempt: number
  startedAt?: string
  startedEventID?: number
  completedAt?: string
  completedEventID?: number
  error?: string
  errorEventID?: number
  status: 'started' | 'failed' | 'completed' | 'timed_out'
}

// WorkflowConfig 对应 program.WorkflowConfig：登记的 Workflow 是一段 WLang 程序与它引用的 Activity。
export interface WorkflowConfig {
  target_worker_identity?: string
  type: string
  name: string
  policy?: string
  version?: string
  program: Record<string, unknown>
  activities?: Record<string, ActivityRef>
  execution_timeout?: string
  run_timeout?: string
}

export interface ActivityRef {
  symbol: string
  type: string
  task_queue?: string
  build_id?: string
  version?: string
  input?: string
  output?: string
}

// --- Schedules ---

export interface Schedule {
  id: string
  namespace: string
  workflow_type: string
  description?: string
  cron_expression: string
  timezone?: string
  input?: Record<string, unknown>
  paused: boolean
  overlap_policy?: string
  last_run_at?: string
  next_run_at?: string
  last_run_id?: string
  run_count: number
  created_at: string
  updated_at: string
}

export interface ScheduleListResult {
  items: Schedule[]
  total: number
}

export interface CreateScheduleRequest {
  namespace?: string
  workflow_type: string
  description?: string
  cron_expression: string
  timezone?: string
  input?: Record<string, unknown>
  overlap_policy?: string
  paused?: boolean
}

// --- Batch Operations ---

export type BatchOperationType = 'cancel' | 'terminate' | 'reset'

// BatchStatus 对应 server/biz/domain/batch 的 StatusEnums：批量操作本身的状态，与它处理的 Run 的状态无关。
// pending 已受理、等待后台驱动；running 正在分页处理；completed 全部选中的 Run 已处理；
// failed 有失败且没有一个成功；cancelled 被用户取消，已处理的计数保留。
export type BatchStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'

export type ResetType = 'first_workflow_task' | 'last_workflow_task'

// BatchRunFilter 是批量操作的 Run 过滤条件（RunListFilter 的子集）。
// 未给 statuses 时 Server 只匹配非终态 Run，除非 include_terminal 为 true。
export interface BatchRunFilter {
  run_ids?: string[]
  workflow_id?: string
  statuses?: RunStatus[]
  include_terminal?: boolean
}

export interface StartBatchOperationRequest {
  namespace?: string
  type: BatchOperationType
  filter: BatchRunFilter
  reason?: string
  reset_type?: ResetType
}

// BatchOperation 是批量操作记录。succeeded_count 对 cancel 表示「已请求取消」的 Run 数（Run 是否结束取决于
// Workflow），对 terminate 与 reset 表示已终止、已 Reset 的 Run 数；skipped_count 是已结束（或已被本批 Reset）
// 而跳过的 Run 数。total_count 在处理第一页时取当时的匹配数，结束时改为实际处理的 Run 数。
export interface BatchOperation {
  id: string
  namespace: string
  type: BatchOperationType
  status: BatchStatus
  filter: BatchRunFilter & { namespace?: string }
  reset_type?: ResetType
  reason?: string
  total_count: number
  succeeded_count: number
  failed_count: number
  skipped_count: number
  // errors 是逐 Run 错误的样本，最多 20 条；更多的失败只计入 failed_count。
  errors?: { run_id: string; message: string }[]
  created_at: string
  started_at?: string
  finished_at?: string
  updated_at: string
}

export interface BatchOperationListResult {
  items: BatchOperation[]
  total: number
}

// --- Task Queues ---

export interface TaskQueueInfo {
  name: string
  namespace: string
  active_workers: number
  worker_ids?: string[]
  activity_types?: string[]
}

export interface TaskQueueListResult {
  items: TaskQueueInfo[]
  total: number
}

// --- Namespaces ---

export interface NamespaceInfo {
  name: string
  run_count: number
  workflow_count: number
  worker_count: number
  schedule_count: number
}

export interface NamespaceListResult {
  items: NamespaceInfo[]
  total: number
}

// --- Query ---

// WorkflowQueryResult 对应 execution.WorkflowQueryResult：Server 以 200 返回被拒的 Query，
// query_rejected / query_error 描述失败，query_result 是 Handler 的返回值。
export interface WorkflowQueryResult {
  query_name?: string
  query_result?: Record<string, unknown>
  query_rejected?: boolean
  query_error?: string
}
