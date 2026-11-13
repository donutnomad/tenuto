"use client"

import { Suspense, useState, useCallback } from "react"
import { useParams, useRouter, useSearchParams } from "next/navigation"
import Link from "next/link"
import useSWR from "swr"
import { api, errorMessage } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { BatchOperationType, ResetType, RunListItem, RunStatus } from "@/lib/types"
import { REFRESH_INTERVAL_LIST } from "@/lib/constants"
import { DEFAULT_RESET_TYPE, batchAccepted, selectedRunsBatchRequest, type BatchOutcome } from "@/lib/batch-operations"
import { scheduleRunsFilter } from "@/lib/schedule-runs"
import { countNoun, truncateID, formatDateTimeColumn } from "@/lib/format"
import { RunStatusBadge } from "@/components/dashboard/run-status-badge"
import { BlockedBadge } from "@/components/dashboard/blocked-badge"
import { ActionFeedback } from "@/components/dashboard/action-feedback"
import { ResetTypeOptions } from "@/components/dashboard/reset-type-options"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
} from "@/components/ui/dropdown-menu"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Filter,
  Search,
  Loader2,
  GitFork,
  ArrowUp,
  ArrowDown,
  ChevronLeft,
  ChevronRight,
  Plus,
  XCircle,
  Skull,
  RotateCcw,
  AlertTriangle,
  CalendarClock,
  X,
} from "lucide-react"

const ALL_STATUSES: RunStatus[] = [
  "running",
  "completed",
  "failed",
  "cancelled",
  "terminated",
  "continued_as_new",
]

const PAGE_SIZE_OPTIONS = [20, 50, 100] as const

const NO_SELECTION: ReadonlySet<string> = new Set()

// useSearchParams 需要 Suspense 边界，页面主体放在边界内。
export default function RunsPage() {
  return (
    <Suspense>
      <RunsPageContent />
    </Suspense>
  )
}

function RunsPageContent() {
  const params = useParams<{ namespace: string }>()
  const router = useRouter()
  const searchParams = useSearchParams()
  const namespace = params.namespace ?? "default"

  // Run Detail 的 Workflow ID 链接带 ?workflow_id=，作为初始筛选。
  const [workflowFilter, setWorkflowFilter] = useState(() => searchParams.get("workflow_id") ?? "")
  // Schedules 页面的链接带 ?schedule_id=：只列这个 Schedule 启动的 Run（搜索属性 TemporalScheduledById）。
  const [scheduleFilter, setScheduleFilter] = useState(() => searchParams.get("schedule_id") ?? "")
  const [statusFilter, setStatusFilter] = useState<RunStatus[]>([])
  // blockedOnly 是派生 predicate 过滤开关，独立于 statusFilter/ALL_STATUSES（Blocked 非 RunStatus）。
  const [blockedOnly, setBlockedOnly] = useState(false)
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(20)
  const [orderDesc, setOrderDesc] = useState(true)
  const [batchDialogType, setBatchDialogType] = useState<BatchOperationType | null>(null)
  const [batchReason, setBatchReason] = useState("")
  const [batchLoading, setBatchLoading] = useState(false)
  const [batchError, setBatchError] = useState<string | null>(null)
  const [batchResult, setBatchResult] = useState<BatchOutcome | null>(null)
  const [batchResetType, setBatchResetType] = useState<ResetType>(DEFAULT_RESET_TYPE)

  const listKey = ["runs", namespace, workflowFilter, scheduleFilter, statusFilter, blockedOnly, page, pageSize, orderDesc]
  // 勾选只属于当前这一页与这组筛选：换页、换筛选或换排序后勾选自动清空，批量操作不会作用到看不见的 Run。
  const selectionKey = JSON.stringify(listKey)
  const [selection, setSelection] = useState<{ key: string; ids: ReadonlySet<string> }>({ key: selectionKey, ids: NO_SELECTION })
  const selectedRuns = selection.key === selectionKey ? selection.ids : NO_SELECTION
  const setSelectedRuns = (update: (prev: ReadonlySet<string>) => ReadonlySet<string>) =>
    setSelection((prev) => ({ key: selectionKey, ids: update(prev.key === selectionKey ? prev.ids : NO_SELECTION) }))

  const { data, isLoading, isValidating, error, mutate } = useSWR(
    listKey,
    () =>
      api.listRuns({
        namespace,
        workflow_id: workflowFilter || undefined,
        search_attributes: scheduleRunsFilter(scheduleFilter),
        statuses: statusFilter.length > 0 ? statusFilter : undefined,
        include_terminal: true,
        blocked_only: blockedOnly || undefined,
        offset: page * pageSize,
        limit: pageSize,
        order_desc: orderDesc,
      }),
    {
      refreshInterval: (data) => {
        const hasRunning = data?.items.some((r) => r.status === "running")
        return hasRunning ? REFRESH_INTERVAL_LIST : 0
      },
    }
  )

  const handleStatusToggle = useCallback(
    (status: RunStatus) => {
      setStatusFilter((prev) => {
        setPage(0)
        return prev.includes(status)
          ? prev.filter((s) => s !== status)
          : [...prev, status]
      })
    },
    []
  )

  const totalPages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1
  const isFirstPage = page === 0
  const isLastPage = page >= totalPages - 1
  const runs: RunListItem[] = data?.items ?? []
  const hasRunningRun = runs.some((r) => r.status === "running")
  const isAutoRefreshing = hasRunningRun && isValidating
  const allSelected = runs.length > 0 && runs.every((r) => selectedRuns.has(r.run_id))
  const someSelected = selectedRuns.size > 0

  function toggleSelectAll() {
    setSelectedRuns(() => (allSelected ? NO_SELECTION : new Set(runs.map((r) => r.run_id))))
  }

  function toggleSelect(runId: string) {
    setSelectedRuns((prev) => {
      const next = new Set(prev)
      if (next.has(runId)) next.delete(runId)
      else next.add(runId)
      return next
    })
  }

  function openBatchDialog(type: BatchOperationType) {
    setBatchError(null)
    setBatchDialogType(type)
  }

  function closeBatchDialog() {
    if (batchLoading) return
    setBatchDialogType(null)
    setBatchError(null)
  }

  async function handleBatchAction() {
    if (!batchDialogType || selectedRuns.size === 0) return
    setBatchLoading(true)
    setBatchError(null)
    setBatchResult(null)
    try {
      const op = await api.startBatchOperation(
        selectedRunsBatchRequest(namespace, batchDialogType, Array.from(selectedRuns), batchReason, batchResetType),
      )
      setSelectedRuns(() => NO_SELECTION)
      setBatchDialogType(null)
      setBatchReason("")
      setBatchResult(batchAccepted(op))
      void mutate()
    } catch (err) {
      setBatchError(errorMessage(err, `Failed to start batch ${batchDialogType}`))
    } finally {
      setBatchLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold text-foreground">
            {data ? countNoun(data.total, "Run") : "Runs"}
          </h1>
          {isAutoRefreshing && (
            <span className="flex items-center gap-1.5 text-xs text-emerald-600">
              <span className="relative flex size-1.5">
                <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500 opacity-75" />
                <span className="relative inline-flex size-1.5 rounded-full bg-emerald-500" />
              </span>
              Live
            </span>
          )}
        </div>
        <Link href={`/${namespace}/runs/start`}>
          <Button variant="outline" size="sm">
            <Plus className="size-3.5" />
            Start Workflow
          </Button>
        </Link>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-40 flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Filter by Workflow ID..."
            value={workflowFilter}
            onChange={(e) => {
              setWorkflowFilter((e.target as HTMLInputElement).value)
              setPage(0)
            }}
            className="pl-8 h-8 text-sm"
          />
        </div>

        <DropdownMenu>
          <DropdownMenuTrigger
            render={
              <Button variant="outline" size="sm">
                <Filter className="size-3.5" />
                Status
                {statusFilter.length > 0 && (
                  <span className="ml-1 rounded-full bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">
                    {statusFilter.length}
                  </span>
                )}
              </Button>
            }
          />
          <DropdownMenuContent>
            {ALL_STATUSES.map((status) => (
              <DropdownMenuCheckboxItem
                key={status}
                checked={statusFilter.includes(status)}
                onCheckedChange={() => handleStatusToggle(status)}
                className="capitalize"
              >
                {status.replace(/_/g, " ")}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        {scheduleFilter && (
          <Button
            variant="secondary"
            size="sm"
            onClick={() => {
              setScheduleFilter("")
              setPage(0)
              router.replace(`/${namespace}/runs`)
            }}
            title="Show runs of every schedule and of direct starts"
            aria-label={`Remove schedule filter ${scheduleFilter}`}
          >
            <CalendarClock className="size-3.5" />
            Schedule <span className="font-mono">{scheduleFilter}</span>
            <X className="size-3.5" />
          </Button>
        )}

        <Button
          variant={blockedOnly ? "default" : "outline"}
          size="sm"
          onClick={() => { setBlockedOnly((v) => !v); setPage(0) }}
          aria-pressed={blockedOnly}
        >
          <AlertTriangle className="size-3.5" />
          Blocked
        </Button>

        {statusFilter.length > 0 && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => { setStatusFilter([]); setPage(0) }}
            className="text-sm"
          >
            Clear
          </Button>
        )}
      </div>

      {someSelected && (
        <div className="flex flex-wrap items-center gap-3 rounded-md border border-primary/20 bg-primary/5 px-4 py-2">
          <span className="text-sm text-muted-foreground">
            {selectedRuns.size} selected
          </span>
          <Button variant="outline" size="sm" onClick={() => openBatchDialog("cancel")}>
            <XCircle className="size-3.5" />
            Cancel
          </Button>
          <Button variant="outline" size="sm" onClick={() => openBatchDialog("reset")}>
            <RotateCcw className="size-3.5" />
            Reset
          </Button>
          <Button variant="destructive" size="sm" onClick={() => openBatchDialog("terminate")}>
            <Skull className="size-3.5" />
            Terminate
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setSelectedRuns(() => NO_SELECTION)}>
            Clear
          </Button>
        </div>
      )}

      {batchResult && (
        <ActionFeedback
          error={batchResult.kind === "error" && <BatchResultMessage namespace={namespace} message={batchResult.message} />}
          success={batchResult.kind === "success" && <BatchResultMessage namespace={namespace} message={batchResult.message} />}
          onDismiss={() => setBatchResult(null)}
        />
      )}

      {error && <LoadError what="runs" error={error} onRetry={() => void mutate()} />}

      {isLoading && !data && (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="size-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {data && (
        <>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8">
                  <input
                    type="checkbox"
                    checked={allSelected}
                    onChange={toggleSelectAll}
                    className="accent-primary"
                  />
                </TableHead>
                <TableHead className="w-24">Status</TableHead>
                <TableHead>Workflow ID</TableHead>
                <TableHead>Run ID</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>History</TableHead>
                <TableHead>
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 hover:text-foreground transition-colors"
                    onClick={() => { setOrderDesc((prev) => !prev); setPage(0) }}
                  >
                    Start
                    {orderDesc ? <ArrowDown className="size-3" /> : <ArrowUp className="size-3" />}
                  </button>
                </TableHead>
                <TableHead>End</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
                    No workflow runs found.
                  </TableCell>
                </TableRow>
              ) : (
                runs.map((run) => (
                  <TableRow
                    key={run.run_id}
                    className="cursor-pointer"
                    onClick={() => router.push(`/${namespace}/runs/${run.run_id}`)}
                  >
                    <TableCell className="w-8" onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={selectedRuns.has(run.run_id)}
                        onChange={() => toggleSelect(run.run_id)}
                        className="accent-primary"
                      />
                    </TableCell>
                    <TableCell>
                      <span className="inline-flex items-center gap-1.5">
                        <RunStatusBadge status={run.status} />
                        <BlockedBadge blocked={run.blocked} />
                      </span>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      <span className="inline-flex items-center gap-1.5">
                        {run.parent_run_id && (
                          <GitFork className="size-3 text-muted-foreground" />
                        )}
                        {run.is_reset && (
                          <RotateCcw
                            className="size-3 text-muted-foreground"
                            aria-label="Reset run"
                          >
                            <title>This run was produced by a reset</title>
                          </RotateCcw>
                        )}
                        <span className="text-primary hover:underline">{run.workflow_id}</span>
                      </span>
                    </TableCell>
                    <TableCell className="font-mono text-sm text-foreground">
                      {run.run_id}
                    </TableCell>
                    <TableCell className="text-sm text-foreground">
                      {run.workflow_type}
                    </TableCell>
                    <TableCell className="text-xs tabular-nums text-muted-foreground">
                      {run.history_length.toLocaleString()} events
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground tabular-nums">
                      {formatDateTimeColumn(run.created_at)}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground tabular-nums">
                      {run.closed_at ? formatDateTimeColumn(run.closed_at) : "—"}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>

          <div className="flex items-center justify-between pt-2">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <select
                value={pageSize}
                onChange={(e) => {
                  setPageSize(Number(e.target.value))
                  setPage(0)
                }}
                className="h-7 rounded-md border border-input bg-transparent px-1.5 text-sm outline-none"
              >
                {PAGE_SIZE_OPTIONS.map((size) => (
                  <option key={size} value={size}>
                    {size}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="ghost" size="icon-sm" disabled={isFirstPage} onClick={() => setPage((p) => p - 1)}>
                <ChevronLeft className="size-4" />
              </Button>
              <Button variant="ghost" size="icon-sm" disabled={isLastPage} onClick={() => setPage((p) => p + 1)}>
                <ChevronRight className="size-4" />
              </Button>
              <span className="text-sm text-muted-foreground tabular-nums">
                {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data.total)} of {data.total}
              </span>
            </div>
          </div>
        </>
      )}

      <Dialog open={batchDialogType !== null} onOpenChange={(open) => { if (!open) closeBatchDialog() }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="capitalize">
              {batchDialogType} {selectedRuns.size} Run{selectedRuns.size !== 1 ? "s" : ""}
            </DialogTitle>
            <DialogDescription>
              {batchDialogType === "reset"
                ? `Resets each selected run in namespace "${namespace}" into a new run at the chosen decision point. Running runs are terminated first.`
                : `Runs a batch ${batchDialogType} on the selected workflow runs in namespace "${namespace}".`}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            {batchDialogType === "reset" && (
              <ResetTypeOptions value={batchResetType} onChange={setBatchResetType} />
            )}
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Reason</label>
              <Input
                placeholder="Optional reason..."
                value={batchReason}
                onChange={(e) => setBatchReason((e.target as HTMLInputElement).value)}
              />
            </div>
            <p className="text-sm text-muted-foreground">
              Affected runs: {Array.from(selectedRuns).slice(0, 3).map(truncateID).join(", ")}
              {selectedRuns.size > 3 && ` and ${selectedRuns.size - 3} more`}
            </p>
            <ActionFeedback error={batchError} />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeBatchDialog} disabled={batchLoading}>
              Cancel
            </Button>
            <Button
              variant={batchDialogType === "terminate" ? "destructive" : "default"}
              onClick={handleBatchAction}
              disabled={batchLoading}
            >
              {batchLoading && <Loader2 className="size-3.5 animate-spin" />}
              Confirm {batchDialogType}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function BatchResultMessage({ namespace, message }: { namespace: string; message: string }) {
  return (
    <>
      {message}{" "}
      <Link href={`/${namespace}/batch-operations`} className="underline underline-offset-2">
        View batch operations
      </Link>
    </>
  )
}
