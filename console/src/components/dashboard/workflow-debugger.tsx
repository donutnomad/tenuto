"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import Link from "next/link"
import { cn } from "@/lib/utils"
import { JsonTree } from "./json-tree"
import {
  loadWasm,
  initReplay,
  stepTo,
  getWasmVersion,
  type ReplayState,
} from "@/lib/wasm-loader"
import {
  Loader2,
  ChevronLeft,
  ChevronRight,
  SkipBack,
  SkipForward,
  ChevronDown,
  ChevronRight as ChevronRightCollapse,
  Circle,
  CheckCircle2,
  XCircle,
  Play,
  ExternalLink,
} from "lucide-react"

interface WorkflowDebuggerProps {
  programJSON: string
  eventsJSON: string
  namespace: string
}

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
        <span className="font-mono text-muted-foreground shrink-0">
          {activity.activityID}
        </span>
        <span className="font-mono flex-1 truncate">{activity.type}</span>
        <span
          className={cn(
            "shrink-0 capitalize text-sm",
            STATUS_LABEL_COLOR[activity.status],
          )}
        >
          {activity.status}
        </span>
      </button>
      {expanded && (
        <div className="border-t border-border bg-muted/20 px-3 py-2 space-y-2 text-xs font-mono">
          {activity.input && (
            <div>
              <span className="text-muted-foreground font-sans font-medium">
                Input
              </span>
              <div className="mt-1">
                <JsonTree data={activity.input} depth={1} />
              </div>
            </div>
          )}
          {activity.output && (
            <div>
              <span className="text-muted-foreground font-sans font-medium">
                Output
              </span>
              <div className="mt-1">
                <JsonTree data={activity.output} depth={1} />
              </div>
            </div>
          )}
          {activity.error && (
            <div>
              <span className="font-sans font-medium text-red-600">Error</span>
              <pre className="mt-1 whitespace-pre-wrap text-red-600">
                {activity.error}
              </pre>
            </div>
          )}
          {activity.attempt != null && (
            <div className="text-muted-foreground">
              <span className="font-sans">Attempt:</span> {activity.attempt}
            </div>
          )}
          {activity.workerIdentity && (
            <div className="text-muted-foreground">
              <span className="font-sans">Worker:</span>{" "}
              {activity.workerIdentity}
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

export function WorkflowDebugger({
  programJSON,
  eventsJSON,
  namespace,
}: WorkflowDebuggerProps) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [wasmVersion, setWasmVersion] = useState("")
  const [handle, setHandle] = useState<number | null>(null)
  const [state, setState] = useState<ReplayState | null>(null)
  const [eventIndex, setEventIndex] = useState(0)
  const [totalEvents, setTotalEvents] = useState(0)

  const currentLineRef = useRef<HTMLDivElement>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const [lineEventMap, setLineEventMap] = useState<Map<number, number>>(new Map())

  useEffect(() => {
    let cancelled = false

    async function init() {
      try {
        await loadWasm()
        if (cancelled) return

        setWasmVersion(getWasmVersion())
        const h = initReplay(programJSON, eventsJSON)
        const finalState = stepTo(h, Number.MAX_SAFE_INTEGER)
        const total = finalState.totalEvents

        if (cancelled) return

        // Pre-compute: for each line, the first event index that lands on it
        const lineMap = new Map<number, number>()
        for (let ei = 1; ei <= total; ei++) {
          const s = stepTo(h, ei)
          if (s.currentLine >= 0 && !lineMap.has(s.currentLine)) {
            lineMap.set(s.currentLine, ei)
          }
        }

        if (cancelled) return

        setHandle(h)
        setTotalEvents(total)
        setEventIndex(total)
        setState(finalState)
        setLineEventMap(lineMap)
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err))
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    init()
    return () => {
      cancelled = true
    }
  }, [programJSON, eventsJSON])

  useEffect(() => {
    currentLineRef.current?.scrollIntoView({
      behavior: "smooth",
      block: "center",
    })
  }, [state?.currentLine])

  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [])

  const navigateTo = useCallback(
    (index: number) => {
      if (handle == null) return
      const clamped = Math.max(0, Math.min(index, totalEvents))
      setEventIndex(clamped)

      if (debounceRef.current) clearTimeout(debounceRef.current)
      debounceRef.current = setTimeout(() => {
        const newState = stepTo(handle, clamped)
        setState(newState)
      }, 50)
    },
    [handle, totalEvents],
  )

  const jumpToNextLine = useCallback(
    (direction: 1 | -1) => {
      if (handle == null || !state) return
      const currentLine = state.currentLine

      if (direction === 1) {
        let target = eventIndex + 1
        while (target <= totalEvents) {
          const s = stepTo(handle, target)
          if (s.currentLine !== currentLine) {
            setEventIndex(target)
            setState(s)
            return
          }
          target++
        }
      } else {
        let target = eventIndex - 1
        let foundLine = -1
        let foundTarget = -1
        while (target >= 1) {
          const s = stepTo(handle, target)
          if (s.currentLine !== currentLine) {
            foundLine = s.currentLine
            foundTarget = target
            break
          }
          target--
        }
        if (foundTarget < 1) return
        // Continue backward to find the start of this line's region
        let start = foundTarget
        while (start - 1 >= 1) {
          const s = stepTo(handle, start - 1)
          if (s.currentLine !== foundLine) break
          start--
        }
        const finalState = stepTo(handle, start)
        setEventIndex(start)
        setState(finalState)
      }
    },
    [handle, state, eventIndex, totalEvents],
  )

  const jumpToLine = useCallback(
    (line: number) => {
      if (handle == null) return
      const ei = lineEventMap.get(line)
      if (ei == null) return
      const s = stepTo(handle, ei)
      setEventIndex(ei)
      setState(s)
    },
    [handle, lineEventMap],
  )

  const [variablesOpen, setVariablesOpen] = useState(true)
  const [activitiesOpen, setActivitiesOpen] = useState(true)

  const condMap = new Map<number, boolean>()
  if (state?.condResults) {
    for (const cr of state.condResults) {
      condMap.set(cr.line, cr.result)
    }
  }

  // Parse events for display in progress bar
  const parsedEvents = (() => {
    try { return JSON.parse(eventsJSON) as Array<Record<string, unknown>> } catch { return [] }
  })()

  function getEventSummary(event: Record<string, unknown>): string {
    const keys = Object.keys(event).filter(k => k !== 'id' && k !== 'timestamp')
    const type = keys[0] || 'unknown'
    const data = event[type] as Record<string, unknown> | undefined
    let detail = ''
    if (data?.activityId) detail = ` ${data.activityId}`
    else if (data?.activityType) detail = ` ${data.activityType}`
    else if (data?.error) detail = ` ${String(data.error).substring(0, 40)}`
    return type.replace(/([A-Z])/g, ' $1').trim() + detail
  }

  if (loading) {
    return (
      <div className="flex h-96 items-center justify-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-5 animate-spin" />
        Loading debugger...
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-md border border-red-300 bg-red-50 px-4 py-3 text-sm text-red-700">
        Failed to load debugger: {error}
      </div>
    )
  }

  const lines = state?.pseudocode?.split("\n") ?? []
  const currentLine = state?.currentLine ?? -1
  const variables = state?.variables ?? {}
  const activities = state?.activities ?? []

  return (
    <div className="flex flex-col rounded-lg border border-border bg-background overflow-hidden">
      <div className="grid grid-cols-[3fr_2fr] flex-1 min-h-0" style={{ height: "60vh" }}>
        {/* Pseudocode */}
        <div className="overflow-y-auto border-r border-border bg-white">
          <div className="py-2">
            {lines.map((line, i) => {
              const isCurrent = i === currentLine
              const isExecuted = currentLine >= 0 && i < currentLine
              const hasBreakpoint = lineEventMap.has(i)

              return (
                <div
                  key={i}
                  ref={isCurrent ? currentLineRef : undefined}
                  className={cn(
                    "group flex items-center px-2 font-mono text-xs leading-6",
                    isCurrent && "bg-yellow-100 border-l-4 border-yellow-400",
                    !isCurrent && isExecuted && "bg-green-50",
                    !isCurrent && !isExecuted && "border-l-4 border-transparent",
                  )}
                >
                  <span className="w-5 shrink-0 text-center select-none">
                    {isCurrent ? (
                      <ChevronRightCollapse className="inline size-3 text-yellow-600" />
                    ) : hasBreakpoint ? (
                      <button
                        type="button"
                        onClick={() => jumpToLine(i)}
                        className="inline-flex items-center justify-center rounded hover:bg-blue-100 transition-colors"
                        title={`Jump to line ${i + 1}`}
                      >
                        <Play className="size-3 fill-blue-500 text-blue-500 hover:fill-blue-700 hover:text-blue-700" />
                      </button>
                    ) : (
                      ""
                    )}
                  </span>
                  <span className="w-8 shrink-0 text-right text-gray-400 select-none pr-2">
                    {i + 1}
                  </span>
                  <span className="whitespace-pre flex-1">{line}</span>
                  {condMap.has(i) && (
                    <span className={cn(
                      "shrink-0 ml-2 px-1.5 py-0.5 rounded text-[10px] font-bold font-sans",
                      condMap.get(i) ? "bg-green-100 text-green-700" : "bg-red-100 text-red-700"
                    )}>
                      {condMap.get(i) ? "TRUE" : "FALSE"}
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        </div>

        {/* Variables & Activities */}
        <div className="overflow-y-auto p-3 space-y-3 bg-muted/10">
          {/* Variables */}
          <div>
            <button
              type="button"
              onClick={() => setVariablesOpen(!variablesOpen)}
              className="flex items-center gap-1 text-sm font-semibold text-muted-foreground hover:text-foreground mb-1"
            >
              {variablesOpen ? (
                <ChevronDown className="size-3.5" />
              ) : (
                <ChevronRightCollapse className="size-3.5" />
              )}
              Variables
            </button>
            {variablesOpen && (
              <div className="rounded-md border border-border bg-white p-3 font-mono text-xs space-y-1">
                {Object.keys(variables).length === 0 ? (
                  <span className="text-muted-foreground italic">
                    No variables
                  </span>
                ) : (
                  Object.entries(variables).map(([key, value]) => (
                    <div key={key} className="leading-relaxed">
                      <span className="font-semibold text-gray-700">
                        {key}
                      </span>
                      <span className="text-gray-400"> = </span>
                      <JsonTree data={value} depth={0} />
                    </div>
                  ))
                )}
              </div>
            )}
          </div>

          {/* Activities */}
          <div>
            <button
              type="button"
              onClick={() => setActivitiesOpen(!activitiesOpen)}
              className="flex items-center gap-1 text-sm font-semibold text-muted-foreground hover:text-foreground mb-1"
            >
              {activitiesOpen ? (
                <ChevronDown className="size-3.5" />
              ) : (
                <ChevronRightCollapse className="size-3.5" />
              )}
              Activities ({activities.length})
            </button>
            {activitiesOpen && (
              <div className="space-y-1.5">
                {activities.length === 0 ? (
                  <span className="text-sm text-muted-foreground italic">
                    No activities
                  </span>
                ) : (
                  activities.map((activity, i) => (
                    <ActivityItem key={`${activity.activityID}-${i}`} activity={activity} namespace={namespace} />
                  ))
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Runtime error */}
      {state?.error && (
        <div className="border-t border-red-200 bg-red-50 px-4 py-2 text-sm text-red-700">
          <strong>Runtime Error:</strong> {state.error}
        </div>
      )}

      {/* Progress bar + event detail */}
      <div className="border-t border-border px-4 py-3 bg-muted/20">
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={() => jumpToNextLine(-1)}
            disabled={eventIndex <= 0}
            className="rounded p-1 hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            title="Skip to previous line change"
          >
            <SkipBack className="size-4" />
          </button>
          <button
            type="button"
            onClick={() => navigateTo(eventIndex - 1)}
            disabled={eventIndex <= 0}
            className="rounded p-1 hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            title="Previous event"
          >
            <ChevronLeft className="size-4" />
          </button>

          <input
            type="range"
            min={0}
            max={totalEvents}
            value={eventIndex}
            onChange={(e) => navigateTo(Number(e.target.value))}
            className="flex-1 h-1.5 cursor-pointer accent-blue-600"
          />

          <button
            type="button"
            onClick={() => navigateTo(eventIndex + 1)}
            disabled={eventIndex >= totalEvents}
            className="rounded p-1 hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            title="Next event"
          >
            <ChevronRight className="size-4" />
          </button>
          <button
            type="button"
            onClick={() => jumpToNextLine(1)}
            disabled={eventIndex >= totalEvents}
            className="rounded p-1 hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            title="Skip to next line change"
          >
            <SkipForward className="size-4" />
          </button>
        </div>

        <div className="flex items-center justify-center gap-3 mt-1 text-sm text-muted-foreground">
          <span>Event {eventIndex} / {totalEvents}</span>
          {wasmVersion && <span className="opacity-50">wasm: {wasmVersion}</span>}
        </div>

        {/* Event list around current position */}
        <div className="mt-2 max-h-32 overflow-y-auto rounded border border-border bg-white text-xs font-mono">
          {parsedEvents.slice(Math.max(0, eventIndex - 3), eventIndex + 2).map((event, i) => {
            const absIndex = Math.max(0, eventIndex - 3) + i
            const isCurrent = absIndex === eventIndex - 1
            return (
              <div
                key={absIndex}
                className={cn(
                  "flex items-center gap-2 px-3 py-1 cursor-pointer hover:bg-gray-50",
                  isCurrent && "bg-blue-50 font-semibold",
                  absIndex >= eventIndex && "text-gray-400",
                )}
                onClick={() => navigateTo(absIndex + 1)}
              >
                <span className="w-6 text-right text-gray-400 shrink-0">{absIndex + 1}</span>
                <span className="truncate">{getEventSummary(event)}</span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
