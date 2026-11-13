"use client"

import { Fragment, useState, useCallback, useMemo } from "react"
import { useParams } from "next/navigation"
import useSWR from "swr"
import { api, errorMessage } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { BatchOperation, BatchOperationType, ResetType, RunStatus } from "@/lib/types"
import { timeAgo, countNoun } from "@/lib/format"
import {
  DEFAULT_RESET_TYPE,
  batchAccepted,
  batchFilter,
  batchProgress,
  batchRequest,
  batchStatusStyle,
  batchSummary,
  isActiveBatch,
  isEmptyBatchFilter,
  succeededLabel,
  type BatchOutcome,
} from "@/lib/batch-operations"
import { ActionFeedback } from "@/components/dashboard/action-feedback"
import { ResetTypeOptions } from "@/components/dashboard/reset-type-options"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import {
  Dialog,
  DialogTrigger,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogClose,
} from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
} from "@/components/ui/dropdown-menu"
import { Loader2, Plus, ChevronDown, ChevronRight, Filter, AlertTriangle } from "lucide-react"

const BATCH_REFRESH_INTERVAL = 3000

const BATCH_TYPES: BatchOperationType[] = ["cancel", "terminate", "reset"]

const ALL_STATUSES: RunStatus[] = [
  "running",
  "completed",
  "failed",
  "cancelled",
  "terminated",
  "continued_as_new",
]

function BatchStatusBadge({ status }: { status: string }) {
  const style = batchStatusStyle(status)
  return (
    <span className={`inline-flex h-5 items-center gap-1.5 rounded-full px-2 text-xs font-medium capitalize ${style.badge}`}>
      <span className={`size-1.5 rounded-full shrink-0 ${style.dot}`} />
      {status}
    </span>
  )
}

// ProgressBar 按已处理的 Run 显示进度：绿色成功、红色失败、灰色跳过；总数未知时只显示已处理数。
function ProgressBar({ op }: { op: BatchOperation }) {
  const { processed, total, percent } = batchProgress(op)
  if (total === 0) {
    return <span className="text-xs text-muted-foreground">{isActiveBatch(op.status) ? "…" : "-"}</span>
  }
  const width = (n: number) => `${(n / total) * 100}%`
  return (
    <div className="flex items-center gap-2" title={`${processed} of ${total} runs processed`}>
      <div className="h-2 w-24 overflow-hidden rounded-full bg-muted">
        <div className="flex h-full">
          <div className="bg-green-500 transition-all" style={{ width: width(op.succeeded_count) }} />
          <div className="bg-red-500 transition-all" style={{ width: width(op.failed_count) }} />
          <div className="bg-slate-400 transition-all" style={{ width: width(op.skipped_count) }} />
        </div>
      </div>
      <span className="text-[10px] text-muted-foreground">
        {processed}/{total} ({percent}%)
      </span>
    </div>
  )
}

export default function BatchOperationsPage() {
  const params = useParams<{ namespace: string }>()
  const namespace = params.namespace ?? "default"

  const [expandedID, setExpandedID] = useState<string | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [newType, setNewType] = useState<BatchOperationType>("cancel")
  const [newWorkflowID, setNewWorkflowID] = useState("")
  const [newStatuses, setNewStatuses] = useState<RunStatus[]>([])
  const [newReason, setNewReason] = useState("")
  const [newResetType, setNewResetType] = useState<ResetType>(DEFAULT_RESET_TYPE)
  // 空过滤条件覆盖 Namespace 内全部非终态 Run，需要用户单独勾选确认。
  const [confirmAll, setConfirmAll] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [result, setResult] = useState<BatchOutcome | null>(null)

  const [cancellingID, setCancellingID] = useState<string | null>(null)
  const [cancelError, setCancelError] = useState<string | null>(null)

  // 有 pending 或 running 的批量操作时轮询进度，全部结束后停止。
  const { data, isLoading, error, mutate } = useSWR(
    ["batch-operations", namespace],
    () => api.listBatchOperations(namespace),
    {
      refreshInterval: (data) => {
        const hasActive = data?.items.some((op) => isActiveBatch(op.status))
        return hasActive ? BATCH_REFRESH_INTERVAL : 0
      },
    }
  )

  const operations: BatchOperation[] = data?.items ?? []
  const filter = useMemo(() => batchFilter(newWorkflowID, newStatuses), [newWorkflowID, newStatuses])
  const filterIsEmpty = isEmptyBatchFilter(filter)

  // 匹配数预览：与 start-batch 相同的过滤语义（未选状态时只匹配非终态 Run）。
  const { data: matchData, error: matchError, isLoading: matchLoading } = useSWR(
    dialogOpen ? ["batch-match-count", namespace, filter] : null,
    () => api.listRuns({ namespace, ...filter, limit: 1 }),
  )
  const matchCount = matchData?.total

  const handleStatusToggle = useCallback((status: RunStatus) => {
    setNewStatuses((prev) =>
      prev.includes(status) ? prev.filter((s) => s !== status) : [...prev, status]
    )
  }, [])

  function resetForm() {
    setNewType("cancel")
    setNewWorkflowID("")
    setNewStatuses([])
    setNewReason("")
    setNewResetType(DEFAULT_RESET_TYPE)
    setConfirmAll(false)
    setSubmitError(null)
  }

  function handleDialogOpenChange(open: boolean) {
    if (!open && submitting) return
    if (open) resetForm()
    setDialogOpen(open)
  }

  const handleSubmit = useCallback(async () => {
    setSubmitting(true)
    setSubmitError(null)
    setResult(null)
    try {
      const op = await api.startBatchOperation(batchRequest(namespace, newType, filter, newReason, newResetType))
      setDialogOpen(false)
      setResult(batchAccepted(op))
      setExpandedID(op.id)
      void mutate()
    } catch (err) {
      setSubmitError(errorMessage(err, `Failed to start batch ${newType}`))
    } finally {
      setSubmitting(false)
    }
  }, [namespace, newType, filter, newReason, newResetType, mutate])

  const canSubmit = !submitting && matchCount !== 0 && (!filterIsEmpty || confirmAll)

  async function handleCancel(op: BatchOperation) {
    setCancellingID(op.id)
    setCancelError(null)
    try {
      await api.cancelBatchOperation(namespace, op.id)
      void mutate()
    } catch (err) {
      setCancelError(errorMessage(err, `Failed to cancel batch ${op.id}`))
    } finally {
      setCancellingID(null)
    }
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold text-foreground">
          {data ? `${data.total} Batch Operation${data.total !== 1 ? "s" : ""}` : "Batch Operations"}
        </h1>
        <Dialog open={dialogOpen} onOpenChange={handleDialogOpenChange}>
          <DialogTrigger
            render={
              <Button variant="default" size="default">
                <Plus className="size-3.5" />
                New Batch
              </Button>
            }
          />
          <DialogContent className="sm:max-w-md">
            <DialogHeader>
              <DialogTitle>Start Batch Operation</DialogTitle>
              <DialogDescription>
                Run a batch operation on matching workflow runs in namespace{" "}
                <span className="font-mono">{namespace}</span>. Without a status filter only running runs match.
                The server processes the batch in the background; runs started after it is created are not included.
              </DialogDescription>
            </DialogHeader>

            <div className="space-y-4 py-2">
              <div className="space-y-1.5">
                <label className="text-xs font-medium">Type</label>
                <div className="flex gap-2">
                  {BATCH_TYPES.map((t) => (
                    <Button
                      key={t}
                      variant={newType === t ? "default" : "outline"}
                      size="sm"
                      onClick={() => setNewType(t)}
                      className="capitalize"
                    >
                      {t}
                    </Button>
                  ))}
                </div>
              </div>

              <div className="space-y-1.5">
                <label className="text-xs font-medium">Workflow ID Filter</label>
                <Input
                  placeholder="e.g. order-processing"
                  value={newWorkflowID}
                  onChange={(e) => setNewWorkflowID((e.target as HTMLInputElement).value)}
                />
              </div>

              <div className="space-y-1.5">
                <label className="text-xs font-medium">Status Filter</label>
                <DropdownMenu>
                  <DropdownMenuTrigger
                    render={
                      <Button variant="outline" size="default" className="w-full justify-between">
                        <span>
                          {newStatuses.length > 0
                            ? `${newStatuses.length} selected`
                            : "Select statuses..."}
                        </span>
                        <Filter className="size-3.5" />
                      </Button>
                    }
                  />
                  <DropdownMenuContent>
                    {ALL_STATUSES.map((status) => (
                      <DropdownMenuCheckboxItem
                        key={status}
                        checked={newStatuses.includes(status)}
                        onCheckedChange={() => handleStatusToggle(status)}
                        className="capitalize"
                      >
                        {status.replace(/_/g, " ")}
                      </DropdownMenuCheckboxItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>

              {newType === "reset" && (
                <ResetTypeOptions value={newResetType} onChange={setNewResetType} />
              )}

              <div className="space-y-1.5">
                <label className="text-xs font-medium">Reason</label>
                <Input
                  placeholder="Optional reason..."
                  value={newReason}
                  onChange={(e) => setNewReason((e.target as HTMLInputElement).value)}
                />
              </div>

              <p className="text-sm text-muted-foreground" aria-live="polite">
                {matchLoading
                  ? "Counting matching runs…"
                  : matchError
                    ? `Could not count matching runs: ${errorMessage(matchError, "unknown error")}`
                    : matchCount === 0
                      ? "No runs match this filter."
                      : matchCount != null
                        ? `${countNoun(matchCount, "run")} currently ${matchCount === 1 ? "matches" : "match"}.`
                        : null}
              </p>

              {filterIsEmpty && (
                <div role="alert" className="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
                  <div className="flex items-center gap-1.5 font-medium">
                    <AlertTriangle className="size-3.5" />
                    No filter set
                  </div>
                  <label className="flex items-start gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={confirmAll}
                      onChange={(e) => setConfirmAll(e.target.checked)}
                      className="accent-destructive mt-0.5"
                    />
                    <span>
                      I understand this will <strong>{newType}</strong> every running workflow run in namespace{" "}
                      <span className="font-mono font-semibold">{namespace}</span>
                      {matchCount != null ? ` (${matchCount} run${matchCount === 1 ? "" : "s"})` : ""}.
                    </span>
                  </label>
                </div>
              )}

              <ActionFeedback error={submitError} />
            </div>

            <DialogFooter>
              <DialogClose render={<Button variant="outline" disabled={submitting} />}>
                Cancel
              </DialogClose>
              <Button
                variant={newType === "cancel" ? "default" : "destructive"}
                onClick={handleSubmit}
                disabled={!canSubmit}
                className="capitalize"
              >
                {submitting && <Loader2 className="size-3.5 animate-spin" />}
                Start {newType}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {result && (
        <ActionFeedback
          error={result.kind === "error" && result.message}
          success={result.kind === "success" && result.message}
          onDismiss={() => setResult(null)}
        />
      )}

      <ActionFeedback error={cancelError} onDismiss={() => setCancelError(null)} />

      {error && <LoadError what="batch operations" error={error} onRetry={() => void mutate()} />}

      {isLoading && !data && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="size-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {data && (
        <>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8" />
                <TableHead>ID</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Progress</TableHead>
                <TableHead>Succeeded</TableHead>
                <TableHead>Failed</TableHead>
                <TableHead>Skipped</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Finished</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {operations.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={11} className="h-24 text-center">
                    No batch operations found
                  </TableCell>
                </TableRow>
              ) : (
                operations.map((op) => {
                  const isExpanded = expandedID === op.id
                  const active = isActiveBatch(op.status)
                  return (
                    <Fragment key={op.id}>
                      <TableRow
                        className="cursor-pointer"
                        onClick={() => setExpandedID(isExpanded ? null : op.id)}
                      >
                        <TableCell className="w-8 px-2">
                          {isExpanded ? (
                            <ChevronDown className="size-3.5 text-muted-foreground" />
                          ) : (
                            <ChevronRight className="size-3.5 text-muted-foreground" />
                          )}
                        </TableCell>
                        <TableCell className="font-mono text-xs">
                          {op.id}
                        </TableCell>
                        <TableCell className="text-xs capitalize">
                          {op.type}
                        </TableCell>
                        <TableCell>
                          <BatchStatusBadge status={op.status} />
                        </TableCell>
                        <TableCell>
                          <ProgressBar op={op} />
                        </TableCell>
                        <TableCell className="text-xs text-green-500" title={succeededLabel(op.type)}>{op.succeeded_count}</TableCell>
                        <TableCell className="text-xs text-red-400">{op.failed_count}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{op.skipped_count}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {timeAgo(op.created_at)}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {op.finished_at ? timeAgo(op.finished_at) : "-"}
                        </TableCell>
                        <TableCell className="text-right">
                          {active && (
                            <Button
                              variant="outline"
                              size="sm"
                              disabled={cancellingID === op.id}
                              onClick={(e) => {
                                e.stopPropagation()
                                void handleCancel(op)
                              }}
                            >
                              {cancellingID === op.id && <Loader2 className="size-3.5 animate-spin" />}
                              Cancel batch
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                      {isExpanded && (
                        <TableRow>
                          <TableCell colSpan={11} className="bg-muted/30 px-8 py-3">
                            <div className="space-y-3">
                              <p className="text-xs">{batchSummary(op)}</p>
                              {op.type === "cancel" && (
                                <p className="text-xs text-muted-foreground">
                                  A batch cancel only requests cancellation; each run finishes when its workflow handles the request.
                                </p>
                              )}
                              <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs sm:grid-cols-3">
                                <div>
                                  <span className="text-muted-foreground">Operation ID:</span>{" "}
                                  <span className="font-mono">{op.id}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground">{succeededLabel(op.type)}:</span>{" "}
                                  <span>{op.succeeded_count}</span>
                                </div>
                                {op.started_at && (
                                  <div>
                                    <span className="text-muted-foreground">Started:</span>{" "}
                                    <span>{timeAgo(op.started_at)}</span>
                                  </div>
                                )}
                                {op.reset_type && (
                                  <div>
                                    <span className="text-muted-foreground">Reset type:</span>{" "}
                                    <span className="font-mono">{op.reset_type}</span>
                                  </div>
                                )}
                                {op.reason && (
                                  <div>
                                    <span className="text-muted-foreground">Reason:</span>{" "}
                                    <span>{op.reason}</span>
                                  </div>
                                )}
                                {op.filter && Object.keys(op.filter).length > 0 && (
                                  <div>
                                    <span className="text-muted-foreground">Filter:</span>{" "}
                                    <span className="font-mono">{JSON.stringify(op.filter)}</span>
                                  </div>
                                )}
                              </div>
                              {op.errors && op.errors.length > 0 ? (
                                <div className="space-y-1">
                                  <span className="text-xs font-medium text-red-400">
                                    Error samples ({op.errors.length} of {op.failed_count} failed)
                                  </span>
                                  <div className="space-y-1">
                                    {op.errors.map((e, i) => (
                                      <div key={i} className="rounded-md bg-muted p-2 text-xs text-muted-foreground">
                                        <span className="font-mono">{e.run_id}</span>: {e.message}
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              ) : (
                                <span className="text-xs text-muted-foreground">
                                  No errors
                                </span>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  )
                })
              )}
            </TableBody>
          </Table>
        </>
      )}
    </div>
  )
}
