"use client"

import { useState, useCallback, type ReactNode } from "react"
import { useParams } from "next/navigation"
import Link from "next/link"
import useSWR from "swr"
import { api, errorMessage } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { Schedule, CreateScheduleRequest } from "@/lib/types"
import { REFRESH_INTERVAL_LIST } from "@/lib/constants"
import { timeAgo } from "@/lib/format"
import { scheduleRunsHref } from "@/lib/schedule-runs"
import { ActionFeedback } from "@/components/dashboard/action-feedback"
import { Badge } from "@/components/ui/badge"
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
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogClose,
} from "@/components/ui/dialog"
import {
  Search,
  Loader2,
  Plus,
  Play,
  Pause,
  RotateCcw,
  Trash2,
  ChevronLeft,
  ChevronRight,
} from "lucide-react"

const PAGE_SIZE_OPTIONS = [10, 20, 50] as const
const OVERLAP_POLICIES = ["SKIP", "ALLOW_ALL"] as const

type ScheduleStatus = "active" | "paused"

const EMPTY_FORM: CreateScheduleRequest = {
  namespace: "",
  workflow_type: "",
  cron_expression: "",
  description: "",
  timezone: "UTC",
  overlap_policy: "SKIP",
  input: undefined,
  paused: false,
}

export default function SchedulesPage() {
  const params = useParams<{ namespace: string }>()
  const namespace = params.namespace ?? "default"

  const [workflowFilter, setWorkflowFilter] = useState("")
  const [statusFilter, setStatusFilter] = useState<ScheduleStatus | "">("")
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(20)

  const [createOpen, setCreateOpen] = useState(false)
  const [form, setForm] = useState<CreateScheduleRequest>({ ...EMPTY_FORM, namespace })
  const [inputJson, setInputJson] = useState("")
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState("")

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  // pendingAction 是进行中的行操作（"<action>:<scheduleID>"），期间禁用该行按钮。
  const [pendingAction, setPendingAction] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [actionSuccess, setActionSuccess] = useState<ReactNode>(null)

  const trimmedWorkflowFilter = workflowFilter.trim()
  const swrKey = ["schedules", namespace, trimmedWorkflowFilter, statusFilter, page, pageSize]

  // 过滤交给 Server：workflow_type 精确匹配，paused 按状态筛选，分页与 total 对应过滤后的结果。
  const { data, isLoading, error, mutate } = useSWR(
    swrKey,
    () =>
      api.listSchedules({
        namespace,
        workflow_type: trimmedWorkflowFilter || undefined,
        paused: statusFilter === "" ? undefined : statusFilter === "paused",
        offset: page * pageSize,
        limit: pageSize,
      }),
    { refreshInterval: REFRESH_INTERVAL_LIST }
  )

  const schedules: Schedule[] = data?.items ?? []

  const totalPages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1
  const isFirstPage = page === 0
  const isLastPage = page >= totalPages - 1

  async function runRowAction(key: string, action: () => Promise<ReactNode>, failure: string) {
    setPendingAction(key)
    setActionError(null)
    setActionSuccess(null)
    try {
      setActionSuccess(await action())
      void mutate()
    } catch (err) {
      setActionError(errorMessage(err, failure))
    } finally {
      setPendingAction(null)
    }
  }

  function handleTogglePause(schedule: Schedule) {
    const resume = schedule.paused
    void runRowAction(
      `pause:${schedule.id}`,
      async () => {
        if (resume) await api.resumeSchedule(namespace, schedule.id)
        else await api.pauseSchedule(namespace, schedule.id)
        return <>Schedule <span className="font-mono">{schedule.id}</span> {resume ? "resumed" : "paused"}.</>
      },
      `Failed to ${resume ? "resume" : "pause"} schedule ${schedule.id}`,
    )
  }

  function handleTrigger(scheduleID: string) {
    void runRowAction(
      `trigger:${scheduleID}`,
      async () => {
        const { run_id: runID } = await api.triggerSchedule(namespace, scheduleID)
        return (
          <>
            Schedule <span className="font-mono">{scheduleID}</span> triggered: run{" "}
            <Link href={`/${namespace}/runs/${encodeURIComponent(runID)}`} className="font-mono underline underline-offset-2">
              {runID}
            </Link>
            .
          </>
        )
      },
      `Failed to trigger schedule ${scheduleID}`,
    )
  }

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return
    setDeleting(true)
    setDeleteError(null)
    try {
      await api.deleteSchedule(namespace, deleteTarget)
      setActionError(null)
      setActionSuccess(<>Schedule <span className="font-mono">{deleteTarget}</span> deleted.</>)
      setDeleteTarget(null)
      void mutate()
    } catch (err) {
      setDeleteError(errorMessage(err, `Failed to delete schedule ${deleteTarget}`))
    } finally {
      setDeleting(false)
    }
  }, [namespace, deleteTarget, mutate])

  const handleCreate = useCallback(async () => {
    setCreateError("")
    if (!form.workflow_type.trim()) {
      setCreateError("Workflow Type is required")
      return
    }
    if (!form.cron_expression.trim()) {
      setCreateError("Cron expression is required")
      return
    }

    let parsedInput: Record<string, unknown> | undefined
    if (inputJson.trim()) {
      try {
        parsedInput = JSON.parse(inputJson)
      } catch {
        setCreateError("Input must be valid JSON")
        return
      }
    }

    setCreating(true)
    try {
      const created = await api.createSchedule({
        ...form,
        namespace,
        input: parsedInput,
      })
      setActionError(null)
      setActionSuccess(<>Schedule <span className="font-mono">{created.id}</span> created.</>)
      setCreateOpen(false)
      setForm({ ...EMPTY_FORM, namespace })
      setInputJson("")
      mutate()
    } catch (err: unknown) {
      setCreateError(err instanceof Error ? err.message : "Failed to create schedule")
    } finally {
      setCreating(false)
    }
  }, [form, inputJson, namespace, mutate])

  const openCreateDialog = useCallback(() => {
    setForm({ ...EMPTY_FORM, namespace })
    setInputJson("")
    setCreateError("")
    setCreateOpen(true)
  }, [namespace])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold text-foreground">
          {data ? `${data.total} Schedule${data.total !== 1 ? "s" : ""}` : "Schedules"}
        </h1>
        <Button variant="outline" size="sm" onClick={openCreateDialog}>
          <Plus className="size-3.5" />
          Create Schedule
        </Button>
      </div>

      <div className="flex items-center gap-2">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Workflow type (exact match)..."
            value={workflowFilter}
            onChange={(e) => {
              setWorkflowFilter((e.target as HTMLInputElement).value)
              setPage(0)
            }}
            className="pl-8 h-8 text-xs"
          />
        </div>

        <select
          value={statusFilter}
          onChange={(e) => {
            setStatusFilter(e.target.value as ScheduleStatus | "")
            setPage(0)
          }}
          className="h-8 rounded-md border border-input bg-transparent px-2 text-xs outline-none"
        >
          <option value="">All statuses</option>
          <option value="active">Active</option>
          <option value="paused">Paused</option>
        </select>
      </div>

      <ActionFeedback
        error={actionError}
        success={actionSuccess}
        onDismiss={() => { setActionError(null); setActionSuccess(null) }}
      />

      {error && <LoadError what="schedules" error={error} onRetry={() => void mutate()} />}

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
                <TableHead>Status</TableHead>
                <TableHead>Schedule ID</TableHead>
                <TableHead>Workflow Type</TableHead>
                <TableHead>Cron Expression</TableHead>
                <TableHead>Timezone</TableHead>
                <TableHead>Overlap Policy</TableHead>
                <TableHead>Last Run</TableHead>
                <TableHead>Next Run</TableHead>
                <TableHead>Run Count</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {schedules.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="h-24 text-center">
                    No schedules found.
                  </TableCell>
                </TableRow>
              ) : (
                schedules.map((s) => (
                  <TableRow key={s.id}>
                    <TableCell>
                      {s.paused ? (
                        <Badge variant="secondary" className="text-[10px] bg-yellow-500/15 text-yellow-500">
                          Paused
                        </Badge>
                      ) : (
                        <Badge variant="secondary" className="text-[10px] bg-green-500/15 text-green-500">
                          Active
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {s.id}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {s.workflow_type}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {s.cron_expression}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {s.timezone || "UTC"}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {s.overlap_policy || "SKIP"}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {s.last_run_at ? timeAgo(s.last_run_at) : "-"}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {s.next_run_at ? timeAgo(s.next_run_at) : "-"}
                    </TableCell>
                    <TableCell className="text-xs">
                      <Link
                        href={scheduleRunsHref(namespace, s.id)}
                        className="underline underline-offset-2"
                        title="Runs started by this schedule"
                        aria-label={`Runs of schedule ${s.id}`}
                      >
                        {s.run_count ?? 0}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1">
                        <Button
                          variant="ghost"
                          size="icon-xs"
                          onClick={() => handleTogglePause(s)}
                          disabled={pendingAction !== null}
                          title={s.paused ? "Resume" : "Pause"}
                          aria-label={`${s.paused ? "Resume" : "Pause"} schedule ${s.id}`}
                        >
                          {s.paused ? (
                            <RotateCcw className="size-3" />
                          ) : (
                            <Pause className="size-3" />
                          )}
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon-xs"
                          onClick={() => handleTrigger(s.id)}
                          disabled={pendingAction !== null}
                          title="Trigger now"
                          aria-label={`Trigger schedule ${s.id} now`}
                        >
                          <Play className="size-3" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon-xs"
                          onClick={() => { setDeleteError(null); setDeleteTarget(s.id) }}
                          disabled={pendingAction !== null}
                          title="Delete"
                          aria-label={`Delete schedule ${s.id}`}
                        >
                          <Trash2 className="size-3 text-red-400" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>

          <div className="flex items-center justify-between pt-2">
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <select
                value={pageSize}
                onChange={(e) => {
                  setPageSize(Number(e.target.value))
                  setPage(0)
                }}
                className="h-7 rounded-md border border-input bg-transparent px-1.5 text-xs outline-none"
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
              <span className="text-xs text-muted-foreground tabular-nums">
                {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data.total)} of {data.total}
              </span>
            </div>
          </div>
        </>
      )}

      {/* Create Schedule Dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Create Schedule</DialogTitle>
            <DialogDescription>
              Create a new cron schedule for a workflow.
            </DialogDescription>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-1.5">
              <label className="text-xs font-medium">Workflow Type *</label>
              <Input
                placeholder="my-workflow"
                value={form.workflow_type}
                onChange={(e) =>
                  setForm((f) => ({ ...f, workflow_type: (e.target as HTMLInputElement).value }))
                }
              />
            </div>

            <div className="grid gap-1.5">
              <label className="text-xs font-medium">Cron Expression *</label>
              <Input
                placeholder="*/5 * * * *"
                className="font-mono"
                value={form.cron_expression}
                onChange={(e) =>
                  setForm((f) => ({ ...f, cron_expression: (e.target as HTMLInputElement).value }))
                }
              />
            </div>

            <div className="grid gap-1.5">
              <label className="text-xs font-medium">Description</label>
              <textarea
                placeholder="Optional description..."
                className="h-16 w-full min-w-0 rounded-lg border border-input bg-transparent px-2.5 py-1.5 text-sm outline-none placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 resize-none"
                value={form.description ?? ""}
                onChange={(e) =>
                  setForm((f) => ({ ...f, description: e.target.value }))
                }
              />
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-1.5">
                <label className="text-xs font-medium">Timezone</label>
                <Input
                  placeholder="UTC"
                  value={form.timezone ?? "UTC"}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, timezone: (e.target as HTMLInputElement).value }))
                  }
                />
              </div>

              <div className="grid gap-1.5">
                <label className="text-xs font-medium">Overlap Policy</label>
                <select
                  value={form.overlap_policy ?? "SKIP"}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, overlap_policy: e.target.value }))
                  }
                  className="h-8 rounded-lg border border-input bg-transparent px-2 text-sm outline-none"
                >
                  {OVERLAP_POLICIES.map((p) => (
                    <option key={p} value={p}>
                      {p.replace(/_/g, " ")}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="grid gap-1.5">
              <label className="text-xs font-medium">Input (JSON)</label>
              <textarea
                placeholder='{"key": "value"}'
                className="h-20 w-full min-w-0 rounded-lg border border-input bg-transparent px-2.5 py-1.5 font-mono text-xs outline-none placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 resize-none"
                value={inputJson}
                onChange={(e) => setInputJson(e.target.value)}
              />
            </div>

            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={form.paused ?? false}
                onChange={(e) =>
                  setForm((f) => ({ ...f, paused: e.target.checked }))
                }
                className="size-3.5 rounded border border-input"
              />
              Start paused
            </label>

            {createError && (
              <div className="rounded-md bg-red-500/10 px-3 py-2 text-xs text-red-400">
                {createError}
              </div>
            )}
          </div>

          <DialogFooter>
            <DialogClose
              render={<Button variant="outline" />}
            >
              Cancel
            </DialogClose>
            <Button onClick={handleCreate} disabled={creating}>
              {creating && <Loader2 className="size-3.5 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteTarget !== null} onOpenChange={(open) => { if (!open && !deleting) setDeleteTarget(null) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Schedule</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete schedule{" "}
              <span className="font-mono">{deleteTarget}</span>? This action
              cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <ActionFeedback error={deleteError} />
          <DialogFooter>
            <DialogClose
              render={<Button variant="outline" />}
            >
              Cancel
            </DialogClose>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleting}
            >
              {deleting && <Loader2 className="size-3.5 animate-spin" />}
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
