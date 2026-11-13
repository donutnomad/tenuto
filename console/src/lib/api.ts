import type {
  ActivityState,
  BatchOperation,
  BatchOperationListResult,
  BlockedInfo,
  CreateScheduleRequest,
  Event,
  Failure,
  HistoryPage,
  NamespaceListResult,
  Run,
  RunListResult,
  Schedule,
  ScheduleListResult,
  StartBatchOperationRequest,
  TaskQueueListResult,
  WorkflowConfig,
  WorkflowConfigListResult,
  WorkflowQueryResult,
  WorkerListResult,
} from './types'

const API_BASE = process.env.NEXT_PUBLIC_API_URL || ''

// ApiError 携带 HTTP 状态码；message 是服务端返回的错误文本。
export class ApiError extends Error {
  constructor(readonly status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

// errorMessage 把 catch 到的值转成给用户看的文本。
export function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback
}

// SERVER_URL 是请求最终到达的 Tenuto Server 地址：直连时是 NEXT_PUBLIC_API_URL，
// 经 Console 代理时是 next.config 里代理的目标地址。只用于连接失败的提示。
const SERVER_URL = API_BASE || process.env.TENUTO_CONSOLE_SERVER_URL || ''

// ServerUnreachableError 表示请求没有到达 Tenuto Server：网络失败，或代理返回了 Server 之外的 5xx。
export class ServerUnreachableError extends Error {
  constructor(readonly serverURL: string = SERVER_URL) {
    super(serverURL ? `Cannot connect to Tenuto Server at ${serverURL}.` : 'Cannot connect to Tenuto Server.')
    this.name = 'ServerUnreachableError'
  }
}

// fromTenutoServer 判断一个失败应答是否由 Tenuto Server 写出：Server 的错误应答总是 text/plain，
// 成功应答是 JSON。Console 代理连不上 Server 时返回的 500 没有 Content-Type，网关的 502/503 是 HTML。
function fromTenutoServer(res: Response): boolean {
  const type = res.headers.get('content-type') ?? ''
  return type.startsWith('text/plain') || type.startsWith('application/json')
}

// UNAUTHENTICATED_MESSAGE 是 Server 应答 401 时给用户看的文本：Server 开启了 WORKFLOW_HTTP_AUTH_TOKEN，
// Console 代理没有带 token 或带的 token 不对。NEXT_PUBLIC_API_URL 直连时浏览器不带 token。
export const UNAUTHENTICATED_MESSAGE = API_BASE
  ? 'Tenuto Server rejected the request (HTTP 401): it requires a token, and NEXT_PUBLIC_API_URL sends requests without one. Remove NEXT_PUBLIC_API_URL and set TENUTO_SERVER_AUTH_TOKEN on the Console server.'
  : "Tenuto Server rejected the Console's credentials (HTTP 401). Set TENUTO_SERVER_AUTH_TOKEN on the Console server to one of the Server's WORKFLOW_HTTP_AUTH_TOKEN values and restart the Console."

async function post<T>(path: string, body: Record<string, unknown> = {}, signal?: AbortSignal): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${API_BASE}/runtime/workflow${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal,
    })
  } catch (error) {
    if (signal?.aborted) throw error
    throw new ServerUnreachableError()
  }
  if (res.status >= 500 && !fromTenutoServer(res)) {
    throw new ServerUnreachableError()
  }
  if (res.status === 401) {
    throw new ApiError(401, UNAUTHENTICATED_MESSAGE)
  }
  if (!res.ok) {
    const text = (await res.text()).trim()
    throw new ApiError(res.status, text || `HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

export const api = {
  listRuns: (filter: {
    namespace?: string
    workflow_id?: string
    statuses?: string[]
    search_attributes?: Record<string, string>
    include_terminal?: boolean
    blocked_only?: boolean
    offset?: number
    limit?: number
    order_desc?: boolean
  }) => post<RunListResult>('/list', { filter }),

  // 后端 get-run 返回 { run, blocked, terminal_failure } 包装（后两者是响应层派生字段）。
  // 合并回单个 Run 便于调用方读取。
  getRun: (namespace: string, runID: string, signal?: AbortSignal): Promise<Run> =>
    post<{ run: Run; blocked: BlockedInfo; terminal_failure?: Failure }>('/get-run', { namespace, run_id: runID }, signal).then(
      (resp) => ({ ...resp.run, blocked: resp.blocked, terminal_failure: resp.terminal_failure }),
    ),

  getHistory: (namespace: string, runID: string, signal?: AbortSignal) =>
    post<Event[]>('/history', { namespace, run_id: runID }, signal),

  // setRunPolicy 是运行中策略 override 与暂停恢复的显式入口。
  setRunPolicy: (namespace: string, runID: string, policy: 'auto_upgrade' | 'pinned'): Promise<Run> =>
    post<Run>('/set-policy', { namespace, run_id: runID, policy }),

  getHistoryPage: (
    namespace: string,
    runID: string,
    options: { pageToken?: string; pageSize?: number; direction?: 'ascending' | 'descending' } = {},
    signal?: AbortSignal,
  ) => post<HistoryPage>('/history-page', {
    namespace,
    run_id: runID,
    page_size: options.pageSize ?? 200,
    direction: options.direction ?? 'descending',
    page_token: options.pageToken,
  }, signal),

  getConfig: (namespace: string, runID: string, signal?: AbortSignal) =>
    post<WorkflowConfig>('/get-config', { namespace, run_id: runID }, signal),

  cancelWorkflow: (namespace: string, runID: string) => post<void>('/cancel', { namespace, run_id: runID }),

  terminateWorkflow: (namespace: string, runID: string, reason: string) =>
    post<void>('/terminate', { namespace, run_id: runID, reason }),

  signalWorkflow: (namespace: string, runID: string, name: string, input: Record<string, unknown>) =>
    post<{ event_id: number }>('/signal', { namespace, run_id: runID, name, input }),

  listWorkflowConfigs: (namespace: string) =>
    post<WorkflowConfigListResult>('/list-configs', { namespace }),

  setWorkerTarget: (namespace: string, workflowID: string, identity: string) =>
    post("/set-worker-target", { namespace, workflow_id: workflowID, target_worker_identity: identity }),

  listWorkers: (namespace: string) =>
    post<WorkerListResult>('/list-workers', { filter: { namespace } }),

  // Schedules
  createSchedule: (req: CreateScheduleRequest) =>
    post<Schedule>('/create-schedule', { ...req }),

  deleteSchedule: (namespace: string, scheduleID: string) =>
    post<void>('/delete-schedule', { namespace, schedule_id: scheduleID }),

  pauseSchedule: (namespace: string, scheduleID: string) =>
    post<void>('/pause-schedule', { namespace, schedule_id: scheduleID }),

  resumeSchedule: (namespace: string, scheduleID: string) =>
    post<void>('/resume-schedule', { namespace, schedule_id: scheduleID }),

  listSchedules: (filter: { namespace?: string; workflow_type?: string; paused?: boolean; offset?: number; limit?: number }) =>
    post<ScheduleListResult>('/list-schedules', { filter }),

  triggerSchedule: (namespace: string, scheduleID: string) =>
    post<{ run_id: string }>('/trigger-schedule', { namespace, schedule_id: scheduleID }),

  // Batch Operations
  // start-batch 只受理（HTTP 202）并返回 pending 记录；Server 在后台分页处理，用 get-batch 或 list-batches 查询进度。
  startBatchOperation: (req: StartBatchOperationRequest) =>
    post<BatchOperation>('/start-batch', { req }),

  getBatchOperation: (namespace: string, operationID: string) =>
    post<BatchOperation>('/get-batch', { namespace, operation_id: operationID }),

  listBatchOperations: (namespace: string, page: { offset?: number; limit?: number } = {}) =>
    post<BatchOperationListResult>('/list-batches', { namespace, ...page }),

  // cancel-batch 让后台驱动在下一页之前停下；已处理的 Run 不回滚。
  cancelBatchOperation: (namespace: string, operationID: string) =>
    post<BatchOperation>('/cancel-batch', { namespace, operation_id: operationID }),

  // Reset
  resetWorkflow: (namespace: string, runID: string, options?: {
    reason?: string
    reset_type?: string
    reset_event_id?: number
    reapply_exclude_types?: string[]
  }) =>
    post<{ run_id: string }>('/reset', {
      namespace,
      run_id: runID,
      reason: options?.reason,
      reset_type: options?.reset_type,
      reset_event_id: options?.reset_event_id,
      reapply_exclude_types: options?.reapply_exclude_types,
    }),

  resetActivity: (namespace: string, runID: string, activityID: string, opts?: { resetHeartbeat?: boolean; reason?: string }) =>
    post<void>('/reset-activity', {
      namespace,
      run_id: runID,
      activity_id: activityID,
      reset_heartbeat: opts?.resetHeartbeat,
      reason: opts?.reason,
    }),

  // Query
  queryWorkflow: (namespace: string, runID: string, name: string, input: Record<string, unknown>) =>
    post<WorkflowQueryResult>('/query', { namespace, run_id: runID, name, input }),

  // Task Queues
  listTaskQueues: (namespace: string) =>
    post<TaskQueueListResult>('/list-task-queues', { namespace }),

  // Namespaces
  listNamespaces: () =>
    post<NamespaceListResult>('/list-namespaces', {}),

  // Start Workflow
  // workflowID 标识这次执行；留空时服务端生成并在应答里返回。
  startWorkflow: (namespace: string, workflowType: string, workflowID: string, input: Record<string, unknown>) =>
    post<{ run_id: string; workflow_id: string }>('/start', {
      namespace,
      workflow_type: workflowType,
      ...(workflowID ? { workflow_id: workflowID } : {}),
      input,
    }),

  // Activity States
  getActivityStates: (namespace: string, runID: string) =>
    post<ActivityState[]>('/activity-states', { namespace, run_id: runID }),
}
