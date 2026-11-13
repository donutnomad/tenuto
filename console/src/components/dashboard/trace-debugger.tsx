"use client"

import { useState, useEffect, useMemo, useCallback, useRef } from "react"
import Link from "next/link"
import { cn } from "@/lib/utils"
import type { Event, RunStatus } from "@/lib/types"
import type { TraceNavigationSnapshot } from "@/lib/trace-session"
import {
  activityAttemptTraceTarget,
  buildActivityInvocationIndex,
  buildActivityInvocationsByLine,
  buildActivityInvocationTraceIndex,
  type ActivityAttemptRecord,
  type ActivityAttemptTracePhase,
  type ActivityAttemptTraceTarget,
  type ActivityInvocationRecord,
  type ActivityInvocationFilter,
  type ActivityInvocationSort,
} from "@/lib/activity-invocations"
import {
  loadWasm,
  traceReplay,
  getWasmVersion,
  type Trace,
  type TraceEffect,
  type LaneInfo,
  type ReplayState,
} from "@/lib/wasm-loader"
import {
  childResumeSnapshot,
  frontierSentinelText,
  restoreActivityInspector,
  uniqueEnterableChildEffects,
} from "./trace-debugger-logic"
import { useTraceNavigation } from "./use-trace-navigation"
import { ActivityInvocationInspector } from "./activity-invocation-inspector"
import { CodePane } from "./trace-code-pane"
import { VariablesPane } from "./trace-variables-pane"
import { JsonTree } from "./json-tree"
import {
  Loader2,
  SkipBack,
  Play,
  FastForward,
  ChevronDown,
  ChevronRight as ChevronRightCollapse,
  Circle,
  CheckCircle2,
  XCircle,
  Pause,
  ExternalLink,
  Rows3,
  RotateCcw,
  CornerDownRight,
  CornerUpLeft,
} from "lucide-react"

export interface TraceAncestorFrame {
  runID: string
  workflowID: string
  line: number
}

interface TraceDebuggerProps {
  programJSON?: string
  // 同一份事件历史的两种形态：events 供 Activity 索引使用，
  // eventsJSON 只在没有 precomputedTrace、需要 wasm 重放时使用。
  events: Event[]
  eventsJSON: string
  precomputedTrace?: Trace
  namespace: string
  fullScreen?: boolean
  initialNavigation?: TraceNavigationSnapshot
  canStepOut?: boolean
  onStepIntoChild?: (
    effect: TraceEffect,
    entrySnapshot: TraceNavigationSnapshot,
    resumeSnapshot: TraceNavigationSnapshot,
  ) => void
  onStepOut?: () => void
  onStepBackBoundary?: () => void
  onStepForwardBoundary?: () => void
  onContinueBoundary?: () => void
  onRestartSession?: () => void
  ancestorFrames?: TraceAncestorFrame[]
  onSelectAncestor?: (runID: string) => void
  descendantFrames?: TraceAncestorFrame[]
  onSelectDescendant?: (runID: string) => void
  readOnlyInspection?: boolean
  runStatus?: RunStatus
  controlsBusy?: boolean
  boundaryStepLabel?: string
  onReturnToActive?: () => void
  onNavigationChange?: (snapshot: TraceNavigationSnapshot) => void
  onPendingChildEffectsChange?: (count: number) => void
  hydrationKey?: string
}

// ActivityItem 从 workflow-debugger.tsx 复制而来（纯展示、无副作用）。
const STATUS_ICON_MAP = {
  scheduled: <Circle className="size-3.5 text-gray-400" />,
  started: <Loader2 className="size-3.5 text-blue-500 animate-spin" />,
  completed: <CheckCircle2 className="size-3.5 text-green-500" />,
  failed: <XCircle className="size-3.5 text-red-500" />,
} as const

const STATUS_LABEL_COLOR = {
  scheduled: "text-gray-500",
  started: "text-blue-600",
  completed: "text-green-600",
  failed: "text-red-600",
} as const

function ActivityItem({
  activity,
  namespace,
}: {
  activity: ReplayState["activities"][0]
  namespace: string
}) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div className="border border-border rounded-md overflow-hidden">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted/30 transition-colors"
      >
        {expanded ? (
          <ChevronDown className="size-3 shrink-0 text-muted-foreground" />
        ) : (
          <ChevronRightCollapse className="size-3 shrink-0 text-muted-foreground" />
        )}
        {STATUS_ICON_MAP[activity.status]}
        <span className="font-mono text-muted-foreground shrink-0">{activity.activityID}</span>
        <span className="font-mono flex-1 truncate">{activity.type}</span>
        <span className={cn("shrink-0 capitalize text-sm", STATUS_LABEL_COLOR[activity.status])}>
          {activity.status}
        </span>
      </button>
      {expanded && (
        <div className="border-t border-border bg-muted/20 px-3 py-2 space-y-2 text-xs font-mono">
          {activity.input && (
            <div>
              <span className="text-muted-foreground font-sans font-medium">Input</span>
              <div className="mt-1">
                <JsonTree data={activity.input} depth={1} />
              </div>
            </div>
          )}
          {activity.output && (
            <div>
              <span className="text-muted-foreground font-sans font-medium">Output</span>
              <div className="mt-1">
                <JsonTree data={activity.output} depth={1} />
              </div>
            </div>
          )}
          {activity.error && (
            <div>
              <span className="font-sans font-medium text-red-600">Error</span>
              <pre className="mt-1 whitespace-pre-wrap text-red-600">{activity.error}</pre>
            </div>
          )}
          {activity.attempt != null && (
            <div className="text-muted-foreground">
              <span className="font-sans">Attempt:</span> {activity.attempt}
            </div>
          )}
          {activity.workerIdentity && (
            <div className="text-muted-foreground">
              <span className="font-sans">Worker:</span> {activity.workerIdentity}
            </div>
          )}
          {activity.childRunID && (
            <div className="flex items-center gap-2 pt-1 border-t border-border mt-1">
              <span className="font-sans font-medium text-muted-foreground">Child Run:</span>
              <Link
                href={`/${namespace}/runs/${activity.childRunID}`}
                className="inline-flex items-center gap-1 text-primary hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {activity.childRunID}
                <ExternalLink className="size-3" />
              </Link>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// LaneBadge：Delve lane 列表里每个 lane 的 end_kind 徽标。
function LaneBadge({ endKind }: { endKind: string }) {
  if (endKind === "pending-frontier") {
    return (
      <span className="inline-flex items-center gap-1 rounded bg-[#4d3b18] px-1.5 py-0.5 text-[10px] font-semibold text-[#e2b86b]">
        <Pause className="size-2.5 fill-[#e2b86b] text-[#e2b86b]" />
        执行前沿
      </span>
    )
  }
  if (endKind === "error") {
    return <span className="size-2 rounded-full bg-red-500" title="error" />
  }
  return <span className="size-2 rounded-full bg-green-500" title="completed" />
}

function laneLabel(lane: LaneInfo): string {
  return lane.lane_id === "main" ? "main" : lane.lane_id
}

export function TraceDebugger({
  programJSON,
  events,
  eventsJSON,
  precomputedTrace,
  namespace,
  fullScreen = false,
  initialNavigation,
  canStepOut = false,
  onStepIntoChild,
  onStepOut,
  onStepBackBoundary,
  onStepForwardBoundary,
  onContinueBoundary,
  onRestartSession,
  ancestorFrames = [],
  onSelectAncestor,
  descendantFrames = [],
  onSelectDescendant,
  readOnlyInspection = false,
  runStatus,
  controlsBusy = false,
  boundaryStepLabel,
  onReturnToActive,
  onNavigationChange,
  onPendingChildEffectsChange,
  hydrationKey = "default",
}: TraceDebuggerProps) {
  const [trace, setTrace] = useState<Trace | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [wasmVersion, setWasmVersion] = useState("")

  const {
    focusLane,
    focusLaneInfo,
    focusSteps,
    lanesSteps,
    cursor,
    current,
    currentFrames,
    selectedFrame,
    selectedFrameIndex,
    setSelectedFrameIndex,
    currentLine,
    variables,
    executedLines,
    breakpoints,
    nextBreakpoint,
    terminalStop,
    terminalStatus,
    atTraceEnd,
    laneMenuOpen,
    setLaneMenuOpen,
    childMenuOpen,
    setChildMenuOpen,
    hydrate: hydrateNavigation,
    reset: resetNavigation,
    focusStep,
    stepForward,
    stepBack,
    continueRun,
    switchLane,
    toggleBreakpoint,
    snapshot: navigationStateSnapshot,
  } = useTraceNavigation({ trace, runStatus, onStepBackBoundary, onStepForwardBoundary, onContinueBoundary })

  const [selectedActivity, setSelectedActivity] = useState<{ line: number; eventID: number } | null>(null)
  const [selectedAttempt, setSelectedAttempt] = useState<ActivityAttemptTraceTarget | null>(null)
  const [activityQuery, setActivityQuery] = useState("")
  const [activityFilter, setActivityFilter] = useState<ActivityInvocationFilter>("all")
  const [activitySort, setActivitySort] = useState<ActivityInvocationSort>("sequence")

  const hydratedNavigationKey = useRef<string | null>(null)

  const activityIndex = useMemo(() => {
    try {
      return buildActivityInvocationIndex(events)
    } catch {
      return new Map<number, ActivityInvocationRecord>()
    }
  }, [events])

  const closeActivityInspector = useCallback(() => {
    setSelectedActivity(null)
    setSelectedAttempt(null)
    setActivityQuery("")
    setActivityFilter("all")
    setActivitySort("sequence")
  }, [])

  useEffect(() => {
    let cancelled = false
    async function init() {
      try {
        let t = precomputedTrace
        if (!t) {
          if (!programJSON) throw new Error("wlang program is required")
          await loadWasm()
          if (cancelled) return
          setWasmVersion(getWasmVersion())
          t = traceReplay(programJSON, eventsJSON)
        }
        if (cancelled) return
        setTrace(t)
        if (hydratedNavigationKey.current !== hydrationKey) {
          hydratedNavigationKey.current = hydrationKey
          hydrateNavigation(t, initialNavigation)
          const inspector = restoreActivityInspector(
            initialNavigation?.activityInspector,
            activityIndex,
            buildActivityInvocationTraceIndex(t.steps),
          )
          if (inspector) {
            setSelectedActivity(inspector.selectedActivity)
            setSelectedAttempt(inspector.selectedAttempt)
            setActivityQuery(inspector.query)
            setActivityFilter(inspector.filter)
            setActivitySort(inspector.sort)
          } else {
            closeActivityInspector()
          }
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    init()
    return () => {
      cancelled = true
    }
  }, [programJSON, eventsJSON, precomputedTrace, initialNavigation, hydrationKey, hydrateNavigation, activityIndex, closeActivityInspector])

  const activityEffectsByLine = useMemo(() => {
    return buildActivityInvocationsByLine(trace?.steps ?? [], activityIndex)
  }, [trace, activityIndex])
  const activityTraceIndex = useMemo(
    () => buildActivityInvocationTraceIndex(trace?.steps ?? []),
    [trace],
  )
  const selectedActivityInvocations = selectedActivity
    ? activityEffectsByLine.get(selectedActivity.line) ?? []
    : []

  const childEffects = useMemo(
    () => uniqueEnterableChildEffects(current?.effects ?? []),
    [current],
  )
  const readyChildEffects = useMemo(
    () => childEffects.filter((effect) => effect.child_run_id),
    [childEffects],
  )

  const selectActivityInvocation = useCallback((line: number, eventID: number) => {
    setSelectedActivity({ line, eventID })
    setSelectedAttempt(null)
    const anchor = activityTraceIndex.get(eventID)
    if (anchor) focusStep(anchor.laneID, anchor.stepSeq)
  }, [activityTraceIndex, focusStep])

  const selectActivityAttempt = useCallback((
    scheduledEventID: number,
    attempt: ActivityAttemptRecord,
    phase?: ActivityAttemptTracePhase,
  ) => {
    const target = activityAttemptTraceTarget(attempt, activityTraceIndex.get(scheduledEventID), phase)
    if (!target) return
    if (focusStep(target.laneID, target.seq)) setSelectedAttempt(target)
  }, [activityTraceIndex, focusStep])

  const restartTrace = useCallback(() => {
    resetNavigation()
    closeActivityInspector()
    onRestartSession?.()
  }, [resetNavigation, closeActivityInspector, onRestartSession])

  const navigationSnapshot = useCallback((): TraceNavigationSnapshot => ({
    ...navigationStateSnapshot(),
    activityInspector: selectedActivity ? {
      scheduledEventID: selectedActivity.eventID,
      attemptEventID: selectedAttempt?.eventID,
      attemptPhase: selectedAttempt?.phase,
      query: activityQuery,
      filter: activityFilter,
      sort: activitySort,
    } : undefined,
  }), [navigationStateSnapshot, selectedActivity, selectedAttempt, activityQuery, activityFilter, activitySort])

  useEffect(() => {
    if (!trace || !onNavigationChange) return
    onNavigationChange(navigationSnapshot())
  }, [trace, onNavigationChange, navigationSnapshot])

  useEffect(() => {
    onPendingChildEffectsChange?.(childEffects.filter((effect) => !effect.child_run_id).length)
    return () => onPendingChildEffectsChange?.(0)
  }, [childEffects, onPendingChildEffectsChange])

  const enterChild = useCallback((effect: TraceEffect) => {
    if (!effect.child_run_id || !onStepIntoChild) return
    const entrySnapshot = navigationSnapshot()
    const resumeSnapshot = childResumeSnapshot(entrySnapshot, effect, trace?.steps ?? [], lanesSteps)
    setChildMenuOpen(false)
    onStepIntoChild(effect, entrySnapshot, resumeSnapshot)
  }, [onStepIntoChild, navigationSnapshot, trace, lanesSteps, setChildMenuOpen])

  const stepIntoChild = useCallback(() => {
    if (childEffects.length === 0 || !onStepIntoChild) return
    if (childEffects.length === 1 && childEffects[0].child_run_id) {
      enterChild(childEffects[0])
      return
    }
    setChildMenuOpen((open) => !open)
  }, [childEffects, enterChild, onStepIntoChild, setChildMenuOpen])

  if (loading) {
    return (
      <div className={cn("flex items-center justify-center gap-2 text-sm text-muted-foreground", fullScreen ? "h-full bg-[#1e1f22]" : "h-96")}>
        <Loader2 className="size-5 animate-spin" />
        Loading trace...
      </div>
    )
  }

  if (error) {
    return (
      <div className={cn("rounded-md border px-4 py-3 text-sm", fullScreen ? "border-red-900/60 bg-[#2b1d20] text-red-300" : "border-red-300 bg-red-50 text-red-700")}>
        Failed to load trace debugger: {error}
      </div>
    )
  }

  if (!trace) {
    return (
      <div className={cn("rounded-md border px-4 py-3 text-sm text-muted-foreground", fullScreen ? "border-[#373737] bg-[#1e1f22]" : "border-border bg-muted/20")}>
        No trace available.
      </div>
    )
  }

  const activities = trace.activities ?? []
  // 聚焦 lane 是 pending-frontier 且 cursor 已到末尾时，才在末尾后显示前沿哨兵。
  const atLaneEnd = focusSteps.length > 0 && cursor === focusSteps.length - 1
  const sentinel =
    focusLaneInfo && atLaneEnd ? frontierSentinelText(focusLaneInfo) : null
  const boundaryHint = onStepForwardBoundary
    ? boundaryStepLabel ?? "再次 Step 返回 Parent resume site"
    : null

  return (
    <div className={cn(fullScreen && "flex h-full min-h-0 flex-col bg-[#1e1f22]")}>
      <div className={cn("flex flex-col overflow-hidden border border-border bg-background", fullScreen ? "min-h-0 flex-1 rounded-none border-0" : "rounded-lg")}>
      {/* trace 级错误（编译/运行错误，仍可能带部分轨迹） */}
      {trace.error && (
        <div className="border-b border-red-200 bg-red-50 px-4 py-2 text-sm text-red-700">
          <strong>Trace Error:</strong> {trace.error}
        </div>
      )}

      <div className="order-2 flex items-center gap-3 border-y border-[#393b40] bg-[#2B2B2B] px-3 py-1.5 text-[#bcbec4]" aria-busy={controlsBusy}>
        <div className="flex items-center gap-0.5">
          <button
            type="button"
            onClick={restartTrace}
            disabled={readOnlyInspection}
            className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
            title="重新开始 (Restart)"
          >
            <RotateCcw className="size-4" />
          </button>
          <button
            type="button"
            onClick={stepBack}
            disabled={readOnlyInspection || controlsBusy || (cursor <= 0 && !onStepBackBoundary)}
            className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
            title={cursor <= 0 && onStepBackBoundary ? "返回父 Workflow 调用点 (Back)" : "上一步 (Back)"}
          >
            <SkipBack className="size-4" />
          </button>
          <button
            type="button"
            onClick={stepForward}
            disabled={readOnlyInspection || controlsBusy || focusSteps.length === 0 || (
              (terminalStop && !onStepForwardBoundary)
              || (atTraceEnd && !terminalStatus)
            )}
            className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
            title={terminalStop
              ? (boundaryStepLabel ?? "返回父 Workflow 恢复点 (Step)")
              : atTraceEnd && terminalStatus
                ? "进入 Workflow 终态 (Step)"
                : "单步 (Step)"}
          >
            <Play className="size-4" />
          </button>
          <button
            type="button"
            onClick={continueRun}
            disabled={readOnlyInspection || controlsBusy || focusSteps.length === 0 || (
              (terminalStop && !onContinueBoundary)
              || (
                cursor >= focusSteps.length - 1
                && nextBreakpoint === null
                && !(terminalStatus && (onContinueBoundary || runStatus !== "completed"))
              )
            )}
            className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
            title={terminalStatus && onContinueBoundary && nextBreakpoint === null
              ? "继续到父 Workflow (Continue)"
              : "运行到下一断点 (Continue)"}
          >
            <FastForward className="size-4" />
          </button>
          <div className="relative">
            <button
              type="button"
              onClick={stepIntoChild}
              disabled={readOnlyInspection || controlsBusy || childEffects.length === 0 || !onStepIntoChild}
              className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
              title={childEffects.length > 1
                ? `选择要步入的子 Workflow (${childEffects.length})`
                : readyChildEffects.length === 1
                  ? `步入子 Workflow: ${readyChildEffects[0].child_workflow_id ?? readyChildEffects[0].child_run_id}`
                  : childEffects.length === 1
                    ? "子 Workflow 正在等待创建 Run"
                    : "当前步骤没有可步入的子 Workflow"}
              aria-expanded={childMenuOpen}
            >
              <CornerDownRight className="size-4" />
            </button>
            {childMenuOpen && (
              <div className="absolute bottom-full left-0 z-50 mb-1 w-80 overflow-hidden rounded border border-[#4e5157] bg-[#303033] py-1 shadow-2xl">
                <div className="border-b border-[#43454a] px-2 py-1 text-[10px] font-semibold text-[#7a7e85]">
                  Child Workflow invocations
                </div>
                {childEffects.map((effect) => {
                  const ready = Boolean(effect.child_run_id)
                  return (
                    <button
                      key={effect.invocation_id}
                      type="button"
                      onClick={() => ready && enterChild(effect)}
                      disabled={!ready}
                      className="flex w-full items-center gap-2 px-2 py-1.5 text-left text-[11px] hover:bg-[#393b40] disabled:cursor-wait disabled:opacity-60"
                    >
                      {ready ? (
                        <CheckCircle2 className="size-3.5 shrink-0 text-[#6aab73]" />
                      ) : (
                        <Loader2 className="size-3.5 shrink-0 animate-spin text-[#e2b86b]" />
                      )}
                      <span className="min-w-0 flex-1">
                        <span className="block truncate font-mono text-[#dfe1e5]">
                          {effect.child_workflow_id ?? effect.child_id ?? effect.activity_id}
                        </span>
                        <span className="block truncate font-mono text-[10px] text-[#7a7e85]">
                          {effect.child_id ?? effect.invocation_id} · {effect.child_run_id ?? "waiting for child run"}
                        </span>
                      </span>
                      <span className={cn(
                        "rounded px-1.5 py-0.5 text-[9px]",
                        ready ? "bg-[#294436] text-[#6aab73]" : "bg-[#4d3b18] text-[#e2b86b]",
                      )}>
                        {ready ? effect.status : "waiting"}
                      </span>
                    </button>
                  )
                })}
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={onStepOut}
            disabled={readOnlyInspection || !canStepOut || !onStepOut}
            className="rounded p-1.5 transition-colors hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
            title="跳出到父 Workflow (Step Out)"
          >
            <CornerUpLeft className="size-4" />
          </button>
        </div>

        {readOnlyInspection && (
          <button
            type="button"
            onClick={onReturnToActive}
            className="inline-flex h-7 items-center gap-1.5 rounded border border-[#4e5157] bg-[#303033] px-2 text-[11px] text-[#dfe1e5] hover:bg-[#393b40]"
            title="返回当前执行 Workflow"
          >
            <CornerDownRight className="size-3.5 text-[#79c0ff]" />
            返回 Active
          </button>
        )}

        <div className="text-xs tabular-nums text-[#a8adbd]">
          {focusSteps.length > 0 ? cursor + 1 : 0} / {focusSteps.length}
        </div>

        <div className="flex items-center gap-1.5 text-xs text-[#a8adbd]">
          <span>Focus:</span>
          <span className="font-mono font-medium text-[#dfe1e5]">
            {focusLaneInfo ? laneLabel(focusLaneInfo) : focusLane}
          </span>
          {selectedFrame && (
            <span className="font-mono text-[#7a7e85]">
              / {selectedFrame.name}:{selectedFrame.line + 1}
            </span>
          )}
        </div>

        <div className="ml-auto flex items-center gap-3">
          {readOnlyInspection && <span className="rounded bg-[#3b3422] px-2 py-0.5 text-[10px] font-semibold text-[#e2b86b]">inspecting</span>}
          {controlsBusy && <span className="inline-flex items-center gap-1 rounded bg-[#26384d] px-2 py-0.5 text-[10px] font-semibold text-[#79c0ff]"><Loader2 className="size-3 animate-spin" />loading child</span>}
          <span
            className={cn(
              "rounded px-2 py-0.5 text-[10px] font-semibold",
              trace.run_kind === "running"
                ? "bg-[#4d3b18] text-[#e2b86b]"
                : "bg-[#294436] text-[#6aab73]",
            )}
          >
            {trace.run_kind}
          </span>
          {wasmVersion && <span className="text-[10px] text-[#7a7e85]">wasm: {wasmVersion}</span>}
        </div>
      </div>


      <CodePane
        pseudocode={trace.pseudocode}
        currentLine={currentLine}
        focusLane={focusLane}
        terminalStop={terminalStop}
        executedLines={executedLines}
        breakpoints={breakpoints}
        activitiesByLine={activityEffectsByLine}
        sentinel={sentinel}
        runStatus={runStatus}
        runKind={trace.run_kind}
        boundaryHint={boundaryHint}
        fullScreen={fullScreen}
        onToggleBreakpoint={toggleBreakpoint}
        onSelectActivity={selectActivityInvocation}
      />

      <div className={cn("order-3 grid min-h-[320px] bg-[#2B2B2B] text-[#bcbec4] lg:grid-cols-[280px_minmax(0,1fr)]", fullScreen ? "h-[min(360px,42vh)]" : "h-[min(420px,42vh)]")}>
        <div className="h-full overflow-y-auto border-b border-[#393b40] p-2 lg:border-b-0 lg:border-r">
          <div className="relative mb-2">
            <button
              type="button"
              onClick={() => setLaneMenuOpen((open) => !open)}
              className="flex h-7 w-full items-center gap-2 rounded border border-[#4e5157] bg-[#303033] px-2 text-left text-[11px] hover:bg-[#393b40]"
              aria-expanded={laneMenuOpen}
            >
              {focusLaneInfo && <LaneBadge endKind={focusLaneInfo.end_kind} />}
              <span className="min-w-0 flex-1 truncate font-mono text-[#dfe1e5]">
                {focusLaneInfo ? laneLabel(focusLaneInfo) : focusLane}
              </span>
              <ChevronDown className={cn("size-3.5 shrink-0 text-[#a8adbd] transition-transform", laneMenuOpen && "rotate-180")} />
            </button>

            {laneMenuOpen && (
              <div className="absolute inset-x-0 top-full z-40 mt-1 overflow-hidden rounded border border-[#4e5157] bg-[#303033] py-1 shadow-xl">
                {trace.lanes.map((lane) => {
                  const active = lane.lane_id === focusLane
                  return (
                    <button
                      key={lane.lane_id}
                      type="button"
                      onClick={() => switchLane(lane.lane_id)}
                      className={cn(
                        "flex h-7 w-full items-center gap-2 px-2 text-left text-[11px]",
                        active ? "bg-[#2e436e] text-[#dfe1e5]" : "hover:bg-[#393b40]",
                      )}
                    >
                      <LaneBadge endKind={lane.end_kind} />
                      <span className="min-w-0 flex-1 truncate font-mono">{laneLabel(lane)}</span>
                      <span className="shrink-0 text-[10px] text-[#7a7e85]">{lane.step_count} 步</span>
                    </button>
                  )
                })}
              </div>
            )}
          </div>

          <div className="mb-1 px-1 text-[10px] font-semibold text-[#7a7e85]">Frames</div>
          {descendantFrames.length > 0 && (
            <div className="mb-1 space-y-0.5 border-b border-[#393b40] pb-2">
              {descendantFrames.map((frame, index) => (
                <button
                  key={frame.runID}
                  type="button"
                  onClick={() => onSelectDescendant?.(frame.runID)}
                  className={cn(
                    "flex h-6 w-full items-center gap-2 rounded px-2 text-left font-mono text-[11px] hover:bg-[#34363b]",
                    index === 0 ? "text-[#79c0ff]" : "text-[#bcbec4]",
                  )}
                  title={`查看执行中的 Workflow: ${frame.workflowID}`}
                >
                  <CornerDownRight className="size-3.5 shrink-0" />
                  <span className="min-w-0 flex-1 truncate">{frame.workflowID}</span>
                  <span className="shrink-0 text-[10px] text-[#7a7e85]">line {frame.line + 1}</span>
                </button>
              ))}
            </div>
          )}
          <div className="space-y-0.5">
            {currentFrames.map((frame, index) => (
              <button
                key={`${frame.path}-${index}`}
                type="button"
                onClick={() => setSelectedFrameIndex(index)}
                className={cn(
                  "flex h-6 w-full items-center gap-2 rounded px-2 text-left font-mono text-[11px]",
                  selectedFrameIndex === index
                    ? "bg-[#2e436e] text-[#dfe1e5]"
                    : "text-[#bcbec4] hover:bg-[#34363b]",
                )}
              >
                <Rows3 className="size-3.5 shrink-0 text-[#a8adbd]" />
                <span className="min-w-0 flex-1 truncate">{frame.name}</span>
                <span className="shrink-0 text-[10px] text-[#7a7e85]">line {frame.line + 1}</span>
              </button>
            ))}
          </div>
          {ancestorFrames.length > 0 && (
            <>
              <div className="mb-1 mt-3 border-t border-[#393b40] px-1 pt-2 text-[10px] font-semibold text-[#7a7e85]">Workflow callers</div>
              <div className="space-y-0.5">
                {ancestorFrames.map((frame) => (
                  <button
                    key={frame.runID}
                    type="button"
                    onClick={() => onSelectAncestor?.(frame.runID)}
                    className="flex h-6 w-full items-center gap-2 rounded px-2 text-left font-mono text-[11px] text-[#bcbec4] hover:bg-[#34363b]"
                    title={`查看父 Workflow: ${frame.workflowID}`}
                  >
                    <CornerUpLeft className="size-3.5 shrink-0 text-[#a8adbd]" />
                    <span className="min-w-0 flex-1 truncate">{frame.workflowID}</span>
                    <span className="shrink-0 text-[10px] text-[#7a7e85]">line {frame.line + 1}</span>
                  </button>
                ))}
              </div>
            </>
          )}
        </div>

        <div className="h-full overflow-y-auto p-2">
          {selectedActivity && selectedActivityInvocations.length > 0 && (
            <ActivityInvocationInspector
              invocations={selectedActivityInvocations}
              anchors={activityTraceIndex}
              selectedEventID={selectedActivity.eventID}
              currentSeq={current?.seq ?? -1}
              selectedAttempt={selectedAttempt}
              query={activityQuery}
              filter={activityFilter}
              sort={activitySort}
              onSelect={(eventID) => selectActivityInvocation(selectedActivity.line, eventID)}
              onSelectAttempt={(attempt, phase) => selectActivityAttempt(selectedActivity.eventID, attempt, phase)}
              onQueryChange={setActivityQuery}
              onFilterChange={setActivityFilter}
              onSortChange={setActivitySort}
              onClose={closeActivityInspector}
            />
          )}
          <VariablesPane variables={variables} />
        </div>
      </div>
      </div>

      {/* 面板是浅色底，显式用 text-foreground，不继承全屏调试台的浅色文字。 */}
      <div className={cn("rounded-lg border border-border bg-background p-3 text-foreground", fullScreen ? "mt-0 max-h-44 shrink-0 overflow-y-auto rounded-none border-x-0 border-b-0" : "mt-4")}>
        <div className="mb-2 text-sm font-semibold text-foreground">
          Activities ({activities.length})
        </div>
        <div className="space-y-1.5">
          {activities.length === 0 ? (
            <span className="text-sm text-muted-foreground italic">No activities</span>
          ) : (
            activities.map((activity, i) => (
              <ActivityItem key={`${activity.activityID}-${i}`} activity={activity} namespace={namespace} />
            ))
          )}
        </div>
      </div>
    </div>
  )
}
