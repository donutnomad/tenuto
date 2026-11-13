import type {
  BatchOperation,
  BatchOperationType,
  BatchRunFilter,
  BatchStatus,
  ResetType,
  RunStatus,
  StartBatchOperationRequest,
} from "./types"

export const DEFAULT_RESET_TYPE: ResetType = "last_workflow_task"

// selectedRunsBatchRequest 构造 Runs 页面勾选 Run 的批量请求。显式 run_ids 带上
// include_terminal：用户勾选的失败或已终止 Run 也在目标之内（Reset 主要用于它们）。
export function selectedRunsBatchRequest(
  namespace: string,
  type: BatchOperationType,
  runIDs: string[],
  reason: string,
  resetType: ResetType,
): StartBatchOperationRequest {
  return {
    namespace,
    type,
    filter: { run_ids: runIDs, include_terminal: true },
    reason: reason.trim() || `Batch ${type} from Tenuto Console`,
    ...(type === "reset" ? { reset_type: resetType } : {}),
  }
}

// batchFilter 构造 Batch Operations 页面的过滤条件；空字段不出现在请求里。
export function batchFilter(workflowID: string, statuses: RunStatus[]): BatchRunFilter {
  const filter: BatchRunFilter = {}
  const trimmed = workflowID.trim()
  if (trimmed) filter.workflow_id = trimmed
  if (statuses.length > 0) filter.statuses = [...statuses]
  return filter
}

// isEmptyBatchFilter 为 true 时批量操作覆盖 Namespace 内全部非终态 Run。
export function isEmptyBatchFilter(filter: BatchRunFilter): boolean {
  return !filter.workflow_id && !filter.statuses?.length && !filter.run_ids?.length
}

// batchRequest 构造 Batch Operations 页面的请求；只有 reset 携带 reset_type。
export function batchRequest(
  namespace: string,
  type: BatchOperationType,
  filter: BatchRunFilter,
  reason: string,
  resetType: ResetType,
): StartBatchOperationRequest {
  return {
    namespace,
    type,
    filter,
    ...(reason.trim() ? { reason: reason.trim() } : {}),
    ...(type === "reset" ? { reset_type: resetType } : {}),
  }
}

export interface BatchOutcome {
  kind: "success" | "error"
  message: string
}

const runs = (n: number) => `${n} run${n === 1 ? "" : "s"}`

// batchAccepted 是 start-batch 受理后的提示：处理在 Server 后台进行，进度在 Batch Operations 页面查看。
export function batchAccepted(op: Pick<BatchOperation, "type">): BatchOutcome {
  return { kind: "success", message: `Batch ${op.type} accepted; it runs in the background.` }
}

// isActiveBatch 为 true 时批量操作仍在等待或处理中，页面需要轮询它的进度。
export function isActiveBatch(status: string): boolean {
  return status === "pending" || status === "running"
}

// succeededLabel 说明 succeeded_count 对这种批量操作的含义：cancel 只请求取消，Run 是否结束取决于 Workflow。
export function succeededLabel(type: BatchOperationType): string {
  switch (type) {
    case "cancel":
      return "Cancellation requested"
    case "terminate":
      return "Terminated"
    case "reset":
      return "Reset"
  }
}

type BatchCounts = Pick<BatchOperation, "type" | "status" | "total_count" | "succeeded_count" | "failed_count" | "skipped_count">

export interface BatchProgress {
  processed: number
  // total 为 0 表示总数尚未知道（还没有处理第一页）或没有匹配的 Run。
  total: number
  percent: number
}

// batchProgress 计算已处理（成功、失败、跳过）的 Run 数与百分比。
export function batchProgress(op: BatchCounts): BatchProgress {
  const processed = op.succeeded_count + op.failed_count + op.skipped_count
  const total = Math.max(op.total_count, processed)
  return { processed, total, percent: total === 0 ? 0 : Math.round((processed / total) * 100) }
}

// batchSummary 用一句话描述批量操作当前的结果。
export function batchSummary(op: BatchCounts): string {
  const { processed, total } = batchProgress(op)
  const detail = [
    `${succeededLabel(op.type).toLowerCase()}: ${op.succeeded_count}`,
    `failed: ${op.failed_count}`,
    `skipped: ${op.skipped_count}`,
  ].join(", ")
  switch (op.status) {
    case "pending":
      return "Waiting for the server to start processing."
    case "running":
      return total > 0 ? `Processing: ${processed} of ${runs(total)} done (${detail}).` : "Processing the first page of runs."
    case "cancelled":
      return `Cancelled after ${runs(processed)} (${detail}); the remaining runs were not processed.`
    case "failed":
      return `Every processed run failed (${detail}).`
    default:
      return processed === 0 ? "No runs matched; nothing was changed." : `Finished ${runs(processed)} (${detail}).`
  }
}

export interface BatchStatusStyle {
  badge: string
  dot: string
}

const BATCH_STATUS_STYLES: Record<BatchStatus, BatchStatusStyle> = {
  pending: { badge: "bg-slate-50 text-slate-700 ring-1 ring-slate-300", dot: "bg-slate-500" },
  running: { badge: "bg-blue-50 text-blue-700 ring-1 ring-blue-200", dot: "bg-blue-500" },
  completed: { badge: "bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200", dot: "bg-emerald-500" },
  failed: { badge: "bg-red-50 text-red-700 ring-1 ring-red-200", dot: "bg-red-500" },
  cancelled: { badge: "bg-amber-50 text-amber-800 ring-1 ring-amber-200", dot: "bg-amber-500" },
}

const UNKNOWN_BATCH_STATUS_STYLE: BatchStatusStyle = {
  badge: "bg-slate-50 text-slate-600 ring-1 ring-slate-200",
  dot: "bg-slate-400",
}

export function batchStatusStyle(status: string): BatchStatusStyle {
  return BATCH_STATUS_STYLES[status as BatchStatus] ?? UNKNOWN_BATCH_STATUS_STYLE
}
