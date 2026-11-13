"use client"

import { useState, useMemo } from "react"
import { useParams, useRouter } from "next/navigation"
import Link from "next/link"
import { HISTORY_PAGE_SIZE, formatByteSize } from "@/lib/history-pagination"
import { buildStepTree } from "@/lib/build-step-tree"
import { useRunDetail } from "@/lib/use-run-detail"
import { useRunActions } from "@/lib/use-run-actions"
import {
  isTerminalRunStatus,
  runConfigErrorBanner,
  runFailureSummary,
  runUpgradeErrorBanner,
  RUN_DETAIL_EVENT_CATEGORIES,
} from "@/lib/run-detail"
import { cn } from "@/lib/utils"
import { RunStatusBadge } from "@/components/dashboard/run-status-badge"
import { LoadError, loadErrorText } from "@/components/dashboard/load-error"
import { BlockedBadge } from "@/components/dashboard/blocked-badge"
import { BlockedDetailCard } from "@/components/dashboard/blocked-detail-card"
import { RunMetadata } from "@/components/dashboard/run-metadata"
import { RunChain } from "@/components/dashboard/run-chain"
import { RelationshipsPanel } from "@/components/dashboard/relationships-panel"
import { QueryPanel } from "@/components/dashboard/query-panel"
import { JsonViewer } from "@/components/dashboard/json-viewer"
import { StepTree } from "@/components/dashboard/step-tree"
import { TimelineView } from "@/components/dashboard/timeline-view"
import { EventHistoryTable } from "@/components/dashboard/event-history-table"
import { decodePayloads, filterEventsByCategory } from "@/components/dashboard/event-history-table-logic"
import { WorkflowDebugger } from "@/components/dashboard/workflow-debugger"
import { TraceTeaser } from "@/components/dashboard/trace-teaser"
import { SignalDialog } from "@/components/dashboard/signal-dialog"
import { ResetPanel } from "@/components/dashboard/reset-panel"
import { PendingStepCard } from "@/components/dashboard/pending-step-card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Tabs,
  TabsList,
  TabsTrigger,
  TabsContent,
} from "@/components/ui/tabs"
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuItem,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu"
import {
  XCircle,
  Skull,
  Radio,
  ExternalLink,
  Loader2,
  ArrowLeft,
  ChevronDown,
  GitFork,
  ArrowUp,
  ArrowDown,
  Filter,
} from "lucide-react"

export default function RunDetailPage() {
  const params = useParams<{ namespace: string; runID: string }>()
  const router = useRouter()
  const namespace = params.namespace ?? "default"
  const runID = params.runID ?? ""

  const [signalOpen, setSignalOpen] = useState(false)
  const [historySortDesc, setHistorySortDesc] = useState(false)
  // Event History 默认倒序（最新在上，对齐 Temporal Descending 默认）；与上方 Steps/Flow 的排序独立。
  const [eventHistoryDesc, setEventHistoryDesc] = useState(true)
  const [categoryFilter, setCategoryFilter] = useState<string[]>([])

  const {
    run,
    runError,
    mutateRun,
    isAutoRefreshing,
    events,
    historyPages,
    historyProgress,
    historyError,
    isHistoryLoading,
    isHistoryValidating,
    loadOlderHistory,
    hasFullHistory,
    fullHistoryLoading,
    ensureFullHistory,
    refreshHistory,
    activityStates,
    workflowConfig,
    latestVersion,
    secondaryErrors,
    allSteps,
    pendingSteps,
    wlangProgram,
    eventsJSON,
    relationshipCount,
    runEndTime,
    taskQueue,
  } = useRunDetail(namespace, runID)

  const {
    actionLoading,
    actionError,
    actionSuccess,
    confirmAction,
    setConfirmAction,
    terminateReason,
    setTerminateReason,
    confirm,
    dismissConfirm,
    setPolicy,
    showResetPanel,
    setShowResetPanel,
    openResetPanel,
    loadFullHistory,
    resetActivityTarget,
    setResetActivityTarget,
    resetActivityLoading,
    resetActivity,
  } = useRunActions({ namespace, runID, mutateRun, refreshHistory, ensureFullHistory })

  const filteredEvents = useMemo(
    () => filterEventsByCategory(events, categoryFilter),
    [events, categoryFilter],
  )
  const eventHistoryRows = useMemo(
    () => (eventHistoryDesc ? [...filteredEvents].reverse() : filteredEvents),
    [eventHistoryDesc, filteredEvents],
  )

  // 不选分类时 filteredEvents 就是 events，直接复用 allSteps。
  const steps = useMemo(() => {
    if (filteredEvents === events) return allSteps
    return filteredEvents.length > 0 ? buildStepTree(filteredEvents, run?.status, activityStates) : []
  }, [filteredEvents, events, allSteps, run?.status, activityStates])
  const stepTreeRows = useMemo(
    () => (historySortDesc ? [...steps].reverse() : steps),
    [historySortDesc, steps],
  )

  if (runError) {
    return <LoadError what="run" error={runError} onRetry={() => void mutateRun()} />
  }

  if (!run) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const isTerminal = isTerminalRunStatus(run.status)
  const configErrorBanner = runConfigErrorBanner(run)
  const upgradeErrorBanner = runUpgradeErrorBanner(run)
  const failureSummary = runFailureSummary(run, events)

  return (
    <div className="space-y-5">
      {/* Back link */}
      <Link
        href={run.parent_run_id ? `/${namespace}/runs/${run.parent_run_id}` : `/${namespace}/runs`}
        className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
      >
        <ArrowLeft className="size-3.5" />
        {run.parent_run_id ? "Back to Parent Run" : "Back to Runs"}
      </Link>

      {/* Parent banner */}
      {run.parent_run_id && (
        <div className="flex items-center gap-2 rounded-md border border-blue-200/60 bg-blue-50/50 px-3 py-1.5 text-sm">
          <GitFork className="size-3.5 text-blue-600/80" />
          <span className="text-blue-600/80">Child of</span>
          <Link href={`/${namespace}/runs/${run.parent_run_id}`} className="font-mono text-primary hover:underline">
            {run.parent_run_id}
          </Link>
        </div>
      )}

      {/* Title row */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1.5">
          <RunStatusBadge status={run.status} />
          <BlockedBadge blocked={run.blocked} />
          <h1 className="min-w-0 break-all text-base font-semibold font-mono">{run.workflow_id}</h1>
          {run.policy && (
            <Badge variant="secondary" className="text-[11px]">
              policy: {run.policy.replace(/_/g, " ")}
              {run.policy_source ? ` · ${run.policy_source}` : ""}
            </Badge>
          )}
          {run.workflow_config_version && (
            <span className="text-xs text-muted-foreground">
              current {run.workflow_config_version.slice(0, 12)}
              {run.original_workflow_config_version && run.original_workflow_config_version !== run.workflow_config_version
                ? ` · original ${run.original_workflow_config_version.slice(0, 12)}`
                : ""}
              {latestVersion ? ` · latest ${latestVersion.slice(0, 12)}` : ""}
            </span>
          )}
          {run.target_worker_identity && (
            <span className="text-xs text-muted-foreground">Activity target: {run.target_worker_identity} (fixed for this run)</span>
          )}
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
        <div className="flex shrink-0 items-center gap-1.5">
          <Button
            size="sm"
            disabled={fullHistoryLoading}
            onClick={() => void openResetPanel()}
          >
            {fullHistoryLoading && <Loader2 className="size-3.5 animate-spin" />}
            Reset
          </Button>
          {!isTerminal && (
            <DropdownMenu>
              <DropdownMenuTrigger
                render={
                  <Button variant="outline" size="sm">
                    More Actions
                    <ChevronDown className="size-3.5" />
                  </Button>
                }
              />
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => setConfirmAction("cancel")} disabled={actionLoading !== null}>
                  <XCircle className="size-3.5" />
                  Cancel
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setConfirmAction("terminate")} disabled={actionLoading !== null} className="text-destructive">
                  <Skull className="size-3.5" />
                  Terminate
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => setSignalOpen(true)}>
                  <Radio className="size-3.5" />
                  Send Signal
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => setPolicy("pinned")} disabled={actionLoading !== null || run.policy === "pinned"}>
                  Pin current content
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setPolicy("auto_upgrade")} disabled={actionLoading !== null || run.policy === "auto_upgrade"}>
                  Use latest (AutoUpgrade)
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      </div>

      {configErrorBanner && (
        <div role="alert" className="rounded-md border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
          <div className="font-medium">{configErrorBanner.title}</div>
          <div className="mt-1">{configErrorBanner.description}</div>
          <div className="mt-1 break-all font-mono text-xs">{configErrorBanner.detail}</div>
          {!isTerminal && (
            <div className="mt-2 flex items-center gap-2">
              <Button size="sm" disabled={actionLoading !== null} onClick={() => setPolicy("auto_upgrade")}>
                Resume with latest (AutoUpgrade)
              </Button>
              <Button size="sm" variant="outline" disabled={actionLoading !== null} onClick={() => setPolicy("pinned")}>
                Pin current content
              </Button>
            </div>
          )}
        </div>
      )}

      {upgradeErrorBanner && (
        <div role="alert" className="rounded-md border border-amber-300/70 bg-amber-50/70 px-4 py-3 text-sm text-amber-900 dark:border-amber-500/40 dark:bg-amber-500/10 dark:text-amber-200">
          <div className="font-medium">{upgradeErrorBanner.title}</div>
          <div className="mt-1">{upgradeErrorBanner.description}</div>
          <div className="mt-1 break-all font-mono text-xs">{upgradeErrorBanner.detail}</div>
          {!isTerminal && (
            <div className="mt-2 flex items-center gap-2">
              <Button size="sm" variant="outline" disabled={actionLoading !== null} onClick={() => setPolicy("pinned")}>
                Pin current content
              </Button>
            </div>
          )}
        </div>
      )}

      {actionError && (
        <div role="alert" className="rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {actionError}
        </div>
      )}
      {secondaryErrors.length > 0 && (
        <div role="alert" className="rounded-md border border-amber-300/70 bg-amber-50/70 px-4 py-3 text-sm text-amber-900 dark:border-amber-500/40 dark:bg-amber-500/10 dark:text-amber-200">
          <div className="font-medium">Some run details failed to load.</div>
          {secondaryErrors.map(({ label, error }) => (
            <div key={label} className="mt-1 break-all text-xs">
              {loadErrorText(label, error)}
            </div>
          ))}
        </div>
      )}
      {historyError && (
        <div role="alert" className="flex items-center justify-between gap-3 rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
          <span className="break-all">{loadErrorText("event history", historyError)}</span>
          <Button variant="outline" size="sm" onClick={() => void refreshHistory()}>
            Retry
          </Button>
        </div>
      )}
      {actionSuccess && (
        <div role="status" className="rounded-md bg-emerald-500/10 px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
          {actionSuccess}
        </div>
      )}

      <RunMetadata
        run={run}
        namespace={namespace}
        runEndTime={runEndTime}
        taskQueue={taskQueue}
        historySize={historyPages?.length ? formatByteSize(historyProgress.bytes) : "-"}
        stateTransitions={historyProgress.total}
        executionTimeout={workflowConfig?.execution_timeout}
        runTimeout={workflowConfig?.run_timeout}
      />

      {run.blocked?.blocked && <BlockedDetailCard blocked={run.blocked} />}

      {failureSummary && (
        <div role="alert" className="rounded-md border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
          <div className="flex flex-wrap items-center gap-2 font-medium">
            Failure
            {failureSummary.failure.type && <Badge variant="outline">{failureSummary.failure.type}</Badge>}
            {failureSummary.timeoutType && <Badge variant="outline">timeout: {failureSummary.timeoutType}</Badge>}
            {failureSummary.failure.nonRetryable && <Badge variant="outline">non-retryable</Badge>}
          </div>
          <div className="mt-1 break-all font-mono text-xs">{failureSummary.failure.message}</div>
          {failureSummary.causes.map((cause, index) => (
            <div key={index} className="mt-1 break-all font-mono text-xs">
              caused by{cause.type ? ` ${cause.type}` : ""}: {cause.message}
            </div>
          ))}
          {failureSummary.failure.details && failureSummary.failure.details.length > 0 && (
            <div className="mt-2">
              <JsonViewer data={decodePayloads(failureSummary.failure.details)} title="Details" />
            </div>
          )}
        </div>
      )}

      {/* Input / Result side by side */}
      {(run.input || run.output) && (
        <div className={cn("grid gap-4", run.input && run.output ? "grid-cols-1 md:grid-cols-2" : "grid-cols-1")}>
          {run.input && <JsonViewer data={run.input} title="Input" defaultOpen />}
          {run.output && <JsonViewer data={run.output} title="Result" defaultOpen />}
        </div>
      )}

      {run.search_attributes && Object.keys(run.search_attributes).length > 0 && (
        <div className="space-y-2">
          <h3 className="text-sm font-medium text-muted-foreground">Search Attributes</h3>
          <div className="flex flex-wrap gap-1.5">
            {Object.entries(run.search_attributes).map(([key, val]) => (
              <Badge key={key} variant="secondary">{key}: {val}</Badge>
            ))}
          </div>
        </div>
      )}

      <ResetPanel
        namespace={namespace}
        runID={runID}
        events={events}
        open={showResetPanel}
        onOpenChange={setShowResetPanel}
        onReset={(newRunID) => {
          setShowResetPanel(false)
          router.push(`/${namespace}/runs/${newRunID}`)
        }}
      />

      {/* Tabs — Temporal-style underline */}
      <Tabs
        defaultValue="history"
        onValueChange={(value) => {
          if (value === "debugger" || value === "relationships") void loadFullHistory()
        }}
      >
        {/* 窄屏下标签栏横向滚动，不撑宽页面。 */}
        <div className="overflow-x-auto border-b border-border">
          <TabsList variant="line" className="gap-0">
            <TabsTrigger value="history" className="px-4 py-2.5 text-[13px]">
              Timeline
            </TabsTrigger>
            <TabsTrigger value="event-history" className="px-4 py-2.5 text-[13px]">
              Event Histories
              {historyProgress.total > 0 && (
                <span className="ml-1.5 rounded-full bg-primary/10 px-1.5 text-xs font-semibold text-primary">
                  {historyProgress.total}
                </span>
              )}
            </TabsTrigger>
            <TabsTrigger value="pending" className="px-4 py-2.5 text-[13px]">
              Pending
              {pendingSteps.length > 0 && (
                <span className="ml-1.5 rounded-full bg-amber-500/10 px-1.5 text-xs font-semibold text-amber-600">
                  {pendingSteps.length}
                </span>
              )}
            </TabsTrigger>
            <TabsTrigger value="relationships" className="px-4 py-2.5 text-[13px]">
              Relationships
              {relationshipCount > 0 && (
                <span className="ml-1.5 rounded-full bg-purple-500/10 px-1.5 text-xs font-semibold text-purple-600">
                  {relationshipCount}
                </span>
              )}
            </TabsTrigger>
            <TabsTrigger value="query" className="px-4 py-2.5 text-[13px]">Query</TabsTrigger>
            {wlangProgram && <TabsTrigger value="debugger" className="px-4 py-2.5 text-[13px]">Replay</TabsTrigger>}
            {wlangProgram && <TabsTrigger value="trace-debugger" className="px-4 py-2.5 text-[13px]">Trace</TabsTrigger>}
          </TabsList>
        </div>

        <TabsContent value="history">
          <div className="space-y-5 pt-4">
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => setHistorySortDesc((p) => !p)}>
                {historySortDesc ? <ArrowDown className="size-3.5" /> : <ArrowUp className="size-3.5" />}
                {historySortDesc ? "Newest First" : "Oldest First"}
              </Button>

              <CategoryFilter selected={categoryFilter} onChange={setCategoryFilter} />
            </div>

            <RunChain
              viewingRunID={run.id}
              firstRunID={run.first_run_id}
              previousRunID={run.continued_run_id || undefined}
              nextRunID={events.findLast((event) => event.workflowContinuedAsNew)?.workflowContinuedAsNew?.nextRunId}
              resetRunID={run.reset_run_id}
              isActive={run.is_current === true}
              namespace={namespace}
            />

            <div>
              <h3 className="mb-2 text-sm font-medium">
                Steps ({steps.length})
                {categoryFilter.length > 0 && allSteps.length !== steps.length && (
                  <span className="ml-1 text-xs text-muted-foreground font-normal">
                    (filtered from {allSteps.length})
                  </span>
                )}
                {historyProgress.hasMore && !hasFullHistory && (
                  <span className="ml-1 text-xs text-muted-foreground font-normal">
                    (from the latest {historyProgress.loaded} of {historyProgress.total} events; open activities come from their current state)
                  </span>
                )}
              </h3>
              <StepTree steps={stepTreeRows} namespace={namespace} />
            </div>

            {steps.length > 0 && (
              <div>
                <h3 className="mb-2 text-sm font-medium">Timeline</h3>
                <TimelineView steps={steps} />
              </div>
            )}
          </div>
        </TabsContent>

        <TabsContent value="event-history">
          <div className="space-y-3 pt-4">
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => setEventHistoryDesc((p) => !p)}>
                {eventHistoryDesc ? <ArrowDown className="size-3.5" /> : <ArrowUp className="size-3.5" />}
                {eventHistoryDesc ? "Newest First" : "Oldest First"}
              </Button>

              <CategoryFilter selected={categoryFilter} onChange={setCategoryFilter} />
            </div>

            {historyPages ? (
              <EventHistoryTable
                events={eventHistoryRows}
                namespace={namespace}
                loadedEvents={events.length}
                totalEvents={historyProgress.total}
              />
            ) : isHistoryLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="size-5 animate-spin text-muted-foreground" />
              </div>
            ) : (
              <p className="py-8 text-center text-sm text-muted-foreground">Event history is unavailable.</p>
            )}
            {historyProgress.hasMore && !hasFullHistory && (
              <div className="flex items-center justify-center gap-3 border-t border-border pt-4">
                <span className="text-xs text-muted-foreground">
                  {historyProgress.loaded} of {historyProgress.total} events loaded
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={isHistoryValidating}
                  onClick={loadOlderHistory}
                >
                  {isHistoryValidating && <Loader2 className="size-3.5 animate-spin" />}
                  Load older events ({Math.min(HISTORY_PAGE_SIZE, historyProgress.total - historyProgress.loaded)} next)
                </Button>
              </div>
            )}
          </div>
        </TabsContent>

        <TabsContent value="pending">
          <div className="space-y-3 pt-4">
            {pendingSteps.length === 0 ? (
              <p className="py-8 text-center text-sm text-muted-foreground">No pending activities</p>
            ) : (
              pendingSteps.map((step) => (
                <PendingStepCard key={step.seq} step={step} onReset={setResetActivityTarget} />
              ))
            )}
          </div>
        </TabsContent>

        <TabsContent value="relationships">
          <div className="pt-4">
            <RelationshipsPanel run={run} events={events} namespace={namespace} />
          </div>
        </TabsContent>

        <TabsContent value="query">
          <div className="pt-4">
            <QueryPanel namespace={namespace} runID={runID} programJSON={wlangProgram} />
          </div>
        </TabsContent>

        {wlangProgram && (
          <TabsContent value="debugger">
            <div className="pt-4">
              {eventsJSON ? (
                <ReplayDebugger programJSON={wlangProgram} eventsJSON={eventsJSON} namespace={namespace} />
              ) : (
                <div className="flex items-center justify-center py-16">
                  <Loader2 className="size-5 animate-spin text-muted-foreground" />
                </div>
              )}
            </div>
          </TabsContent>
        )}

        {wlangProgram && (
          <TabsContent value="trace-debugger">
            <div className="pt-4">
              <TraceTeaser
                namespace={namespace}
                runID={runID}
                workflowID={run.workflow_id}
                status={run.status}
                eventCount={historyProgress.total}
              />
            </div>
          </TabsContent>
        )}
      </Tabs>

      <div className="flex justify-end pt-2 border-t border-border/50">
        <Link href={`/${namespace}/runs/${runID}/events`}>
          <Button variant="outline" size="sm">
            <ExternalLink className="size-3.5" />
            View Raw Events
          </Button>
        </Link>
      </div>

      <SignalDialog open={signalOpen} onOpenChange={setSignalOpen} namespace={namespace} runID={runID} />

      <Dialog
        open={confirmAction !== null}
        onOpenChange={(open) => {
          if (!open) dismissConfirm()
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>
              {confirmAction === "terminate" ? "Terminate Workflow" : "Cancel Workflow"}
            </DialogTitle>
            <DialogDescription>
              {confirmAction === "terminate"
                ? "Terminate closes this run immediately and records the reason in its event history."
                : "Cancel requests a graceful workflow cancellation."}
            </DialogDescription>
          </DialogHeader>
          {confirmAction === "terminate" && (
            <div className="space-y-1.5">
              <label htmlFor="terminate-reason" className="text-sm font-medium">Reason</label>
              <textarea
                id="terminate-reason"
                value={terminateReason}
                onChange={(event) => setTerminateReason(event.target.value)}
                rows={3}
                className="w-full resize-y rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                placeholder="Why is this run being terminated?"
              />
            </div>
          )}
          {actionError && (
            <div role="alert" className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
              {actionError}
            </div>
          )}
          <DialogFooter>
            <Button
              variant="outline"
              onClick={dismissConfirm}
              disabled={actionLoading !== null}
            >
              Keep Running
            </Button>
            <Button
              variant={confirmAction === "terminate" ? "destructive" : "default"}
              onClick={() => confirmAction && confirm(confirmAction)}
              disabled={actionLoading !== null}
            >
              {actionLoading && <Loader2 className="size-3.5 animate-spin" />}
              {confirmAction === "terminate" ? "Terminate" : "Request Cancellation"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={resetActivityTarget !== null} onOpenChange={(open) => { if (!open) setResetActivityTarget(null) }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Reset Activity</DialogTitle>
            <DialogDescription>
              Reset <span className="font-mono">{resetActivityTarget?.activityType}</span> ({resetActivityTarget?.stepID}) back to attempt 1 and reschedule it immediately. This is useful when an activity is stuck retrying.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setResetActivityTarget(null)}>Cancel</Button>
            <Button onClick={resetActivity} disabled={resetActivityLoading}>
              {resetActivityLoading && <Loader2 className="size-3.5 animate-spin" />}
              Reset Activity
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

// Timeline 与 Event Histories 两个标签页共用的分类筛选。
function CategoryFilter({
  selected,
  onChange,
}: {
  selected: string[]
  onChange: (categories: string[]) => void
}) {
  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger
          render={
            <Button variant="outline" size="sm">
              <Filter className="size-3.5" />
              Category
              {selected.length > 0 && (
                <span className="ml-1 rounded-full bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">
                  {selected.length}
                </span>
              )}
            </Button>
          }
        />
        <DropdownMenuContent>
          {Object.keys(RUN_DETAIL_EVENT_CATEGORIES).map((cat) => (
            <DropdownMenuCheckboxItem
              key={cat}
              checked={selected.includes(cat)}
              onCheckedChange={() => {
                onChange(selected.includes(cat) ? selected.filter((c) => c !== cat) : [...selected, cat])
              }}
            >
              {cat}
            </DropdownMenuCheckboxItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      {selected.length > 0 && (
        <Button variant="ghost" size="sm" onClick={() => onChange([])}>
          Clear
        </Button>
      )}
    </>
  )
}

// Replay 重放打开标签页那一刻的历史快照：轮询带来的新事件不会让调试器在单步中途重新初始化。
// 标签页关闭时组件卸载，下次打开取新的快照。
function ReplayDebugger({ programJSON, eventsJSON, namespace }: { programJSON: string; eventsJSON: string; namespace: string }) {
  const [snapshot] = useState(eventsJSON)
  return <WorkflowDebugger programJSON={programJSON} eventsJSON={snapshot} namespace={namespace} />
}
