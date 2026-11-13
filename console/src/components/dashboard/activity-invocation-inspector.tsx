"use client"

import { useEffect, useRef, useState } from "react"
import { cn } from "@/lib/utils"
import {
  activityAttemptTraceTarget,
  activityInvocationCursorPhase,
  activityInvocationDurationMs,
  activityInvocationSummary,
  activityInvocationVirtualWindow,
  nextExceptionalActivityInvocation,
  selectActivityInvocations,
  type ActivityAttemptRecord,
  type ActivityAttemptTracePhase,
  type ActivityAttemptTraceTarget,
  type ActivityInvocationRecord,
  type ActivityInvocationFilter,
  type ActivityInvocationSort,
  type ActivityInvocationTraceAnchor,
} from "@/lib/activity-invocations"
import { DebuggerVariableNode } from "./trace-variables-pane"
import {
  AlertTriangle,
  ChevronDown,
  ChevronLeft,
  ChevronRight as ChevronRightCollapse,
  X,
} from "lucide-react"

// Trace 调试器里一行代码上的 Activity 调用检查器：调用选择、attempts、输入输出与心跳。

function attemptDuration(attempt: ActivityAttemptRecord): string | null {
  if (!attempt.startedAt || !attempt.completedAt) return null
  const duration = new Date(attempt.completedAt).getTime() - new Date(attempt.startedAt).getTime()
  return duration < 1000 ? `${duration}ms` : `${(duration / 1000).toFixed(2)}s`
}

function invocationDuration(invocation: ActivityInvocationRecord): string | null {
  const duration = activityInvocationDurationMs(invocation)
  if (duration == null) return null
  return duration < 1000 ? `${duration}ms` : `${(duration / 1000).toFixed(2)}s`
}

function attemptBackoff(
  attempts: ActivityAttemptRecord[],
  index: number,
): string | null {
  if (index === 0) return null
  const previousCompletedAt = attempts[index - 1].completedAt
  const startedAt = attempts[index].startedAt
  if (!previousCompletedAt || !startedAt) return null
  const duration = new Date(startedAt).getTime() - new Date(previousCompletedAt).getTime()
  if (duration <= 0) return null
  return duration < 1000 ? `${duration}ms backoff` : `${(duration / 1000).toFixed(2)}s backoff`
}

function activityTimestamp(timestamp?: string): string | null {
  if (!timestamp) return null
  const date = new Date(timestamp)
  if (Number.isNaN(date.getTime())) return null
  return date.toLocaleTimeString([], { hour12: false, fractionalSecondDigits: 3 })
}

function ActivityPayload({
  title,
  data,
  emptyText,
}: {
  title: string
  data?: Record<string, unknown>
  emptyText: string
}) {
  const entries = Object.entries(data ?? {})
  return (
    <div className="min-w-0">
      <div className="border-b border-[#393b40] bg-[#2d2e31] px-2 py-1 text-[10px] font-semibold text-[#a8adbd]">
        {title}
      </div>
      <div className="min-h-12 overflow-auto py-0.5">
        {entries.length > 0 ? entries.map(([key, value]) => (
          <DebuggerVariableNode key={key} name={key} data={value} />
        )) : (
          <div className="px-2 py-2 text-[#7a7e85] italic">{emptyText}</div>
        )}
      </div>
    </div>
  )
}

const ACTIVITY_CALL_ROW_HEIGHT = 28
const ACTIVITY_CALL_VIEWPORT_HEIGHT = 224
const ACTIVITY_CALL_OVERSCAN = 3

function ActivityInvocationPicker({
  invocations,
  allInvocations,
  anchors,
  currentSeq,
  selectedEventID,
  query,
  filter,
  sort,
  matchCount,
  onSelect,
  onQueryChange,
  onFilterChange,
  onSortChange,
}: {
  invocations: ActivityInvocationRecord[]
  allInvocations: ActivityInvocationRecord[]
  anchors: ReadonlyMap<number, ActivityInvocationTraceAnchor>
  currentSeq: number
  selectedEventID: number
  query: string
  filter: ActivityInvocationFilter
  sort: ActivityInvocationSort
  matchCount: number
  onSelect: (scheduledEventID: number) => void
  onQueryChange: (query: string) => void
  onFilterChange: (filter: ActivityInvocationFilter) => void
  onSortChange: (sort: ActivityInvocationSort) => void
}) {
  const [open, setOpen] = useState(false)
  const [scrollTop, setScrollTop] = useState(0)
  const scrollRef = useRef<HTMLDivElement>(null)
  const selected = allInvocations.find((item) => item.scheduledEventID === selectedEventID)
  const selectedIndex = invocations.findIndex((item) => item.scheduledEventID === selectedEventID)
  const viewportHeight = Math.min(
    ACTIVITY_CALL_VIEWPORT_HEIGHT,
    Math.max(ACTIVITY_CALL_ROW_HEIGHT, invocations.length * ACTIVITY_CALL_ROW_HEIGHT),
  )
  const window = activityInvocationVirtualWindow(
    invocations.length,
    scrollTop,
    ACTIVITY_CALL_ROW_HEIGHT,
    viewportHeight,
    ACTIVITY_CALL_OVERSCAN,
  )
  const visibleInvocations = invocations.slice(window.start, window.end)
  useEffect(() => {
    if (!open || selectedIndex < 0 || !scrollRef.current) return
    const centered = Math.max(0, selectedIndex * ACTIVITY_CALL_ROW_HEIGHT - viewportHeight / 2)
    scrollRef.current.scrollTop = centered
  }, [open, selectedIndex, viewportHeight])
  return (
    <div className="relative min-w-0 flex-1">
      <button
        type="button"
        aria-label="Activity invocation"
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex h-6 w-full items-center gap-1 border border-[#4e5157] bg-[#2B2B2B] px-1.5 text-left text-[10px] text-[#dfe1e5] outline-none hover:border-[#6f737a] focus:border-[#58a6ff]"
      >
        <span className="min-w-0 flex-1 truncate">
          {selected
            ? `Call ${allInvocations.indexOf(selected) + 1} · ${activityInvocationSummary(selected)}`
            : "Select Call"}
        </span>
        {(query || filter !== "all") && (
          <span className="shrink-0 rounded bg-[#34363b] px-1 text-[9px] text-[#79c0ff]">{matchCount}</span>
        )}
        <ChevronDown className="size-3 shrink-0" />
      </button>
      {open && (
        <div className="absolute right-0 top-full z-40 w-[min(32rem,calc(100vw-2rem))] border border-[#4e5157] bg-[#252628] shadow-xl">
          <div className="border-b border-[#393b40] px-2 py-1.5">
            <div className="mb-1 flex items-center gap-2 text-[10px] font-semibold text-[#dfe1e5]">
              <span>Activity Calls</span>
              <span className="font-normal text-[#6f737a]">{matchCount} matches · {allInvocations.length} total</span>
            </div>
            <input
              type="search"
              aria-label="Search Activity Calls"
              value={query}
              onChange={(event) => onQueryChange(event.target.value)}
              placeholder="搜索 Activity Calls：输入、Event、Lane、状态"
              className="mb-1 h-7 w-full border border-[#4e5157] bg-[#2B2B2B] px-2 text-[10px] text-[#dfe1e5] outline-none placeholder:text-[#6f737a] focus:border-[#58a6ff]"
            />
            <div className="flex items-center gap-1">
              <select
                aria-label="Activity invocation filter"
                value={filter}
                onChange={(event) => onFilterChange(event.target.value as ActivityInvocationFilter)}
                className="h-6 min-w-0 flex-1 border border-[#4e5157] bg-[#2B2B2B] px-1 text-[10px] text-[#dfe1e5] outline-none focus:border-[#58a6ff]"
              >
                <option value="all">全部状态</option>
                <option value="current">当前</option>
                <option value="past">已发生</option>
                <option value="future">尚未发生</option>
                <option value="completed">completed</option>
                <option value="failed">Failed only</option>
                <option value="running">running</option>
                <option value="timed_out">timed out</option>
                <option value="cancelled">cancelled</option>
              </select>
              <select
                aria-label="Activity invocation sort"
                value={sort}
                onChange={(event) => onSortChange(event.target.value as ActivityInvocationSort)}
                className="h-6 min-w-0 flex-1 border border-[#4e5157] bg-[#2B2B2B] px-1 text-[10px] text-[#dfe1e5] outline-none focus:border-[#58a6ff]"
              >
                <option value="sequence">调用顺序</option>
                <option value="duration-desc">耗时：长 → 短</option>
                <option value="duration-asc">耗时：短 → 长</option>
              </select>
            </div>
          </div>
          <div
            ref={scrollRef}
            className="overflow-y-auto"
            role="listbox"
            aria-label="Activity calls"
            style={{ height: viewportHeight }}
            onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
          >
            <div className="relative" style={{ height: window.totalHeight }}>
              <div className="absolute left-0 right-0 top-0" style={{ transform: `translateY(${window.offsetTop}px)` }}>
                {visibleInvocations.map((item) => {
                  const originalIndex = allInvocations.findIndex((candidate) => candidate.scheduledEventID === item.scheduledEventID)
                  const phase = activityInvocationCursorPhase(anchors.get(item.scheduledEventID), currentSeq)
                  return (
                    <button
                      key={item.scheduledEventID}
                      type="button"
                      role="option"
                      aria-selected={item.scheduledEventID === selectedEventID}
                      onClick={() => {
                        onSelect(item.scheduledEventID)
                        setOpen(false)
                      }}
                      className={cn(
                        "flex w-full items-center gap-2 px-2 text-left text-[10px] hover:bg-[#393b40]",
                        item.scheduledEventID === selectedEventID && "bg-[#2f3f55]",
                      )}
                      style={{ height: ACTIVITY_CALL_ROW_HEIGHT }}
                    >
                      <span className="w-14 shrink-0 tabular-nums text-[#a8adbd]">Call {originalIndex + 1}</span>
                      <span className="min-w-0 flex-1 truncate text-[#dfe1e5]">{activityInvocationSummary(item)}</span>
                      <span className={cn(
                        "shrink-0",
                        (item.status === "failed" || item.status === "timed_out" || item.status === "cancelled") && "text-[#cf6679]",
                      )}>{item.status}</span>
                      <span className="w-12 shrink-0 text-right text-[#6f737a]">
                        {phase === "current" ? "当前" : phase === "past" ? "已发生" : "尚未"}
                      </span>
                    </button>
                  )
                })}
              </div>
            </div>
          </div>
          <div className="border-t border-[#393b40] px-2 py-1 text-right text-[9px] text-[#6f737a]">
            rendering {visibleInvocations.length} / {invocations.length}
          </div>
        </div>
      )}
    </div>
  )
}

export function ActivityInvocationInspector({
  invocations,
  anchors,
  selectedEventID,
  currentSeq,
  selectedAttempt,
  query,
  filter,
  sort,
  onSelect,
  onSelectAttempt,
  onQueryChange,
  onFilterChange,
  onSortChange,
  onClose,
}: {
  invocations: ActivityInvocationRecord[]
  anchors: ReadonlyMap<number, ActivityInvocationTraceAnchor>
  selectedEventID: number
  currentSeq: number
  selectedAttempt: ActivityAttemptTraceTarget | null
  query: string
  filter: ActivityInvocationFilter
  sort: ActivityInvocationSort
  onSelect: (scheduledEventID: number) => void
  onSelectAttempt: (attempt: ActivityAttemptRecord, phase?: ActivityAttemptTracePhase) => void
  onQueryChange: (query: string) => void
  onFilterChange: (filter: ActivityInvocationFilter) => void
  onSortChange: (sort: ActivityInvocationSort) => void
  onClose: () => void
}) {
  const selectedIndex = Math.max(0, invocations.findIndex((item) => item.scheduledEventID === selectedEventID))
  const invocation = invocations[selectedIndex]
  if (!invocation) return null
  const selectedAnchor = anchors.get(invocation.scheduledEventID)
  const selectedPhase = activityInvocationCursorPhase(selectedAnchor, currentSeq)
  const selectInvocation = (scheduledEventID: number) => onSelect(scheduledEventID)
  const filteredInvocations = selectActivityInvocations(invocations, anchors, currentSeq, query, filter, sort)
  const selectableInvocations = filteredInvocations.some((item) => item.scheduledEventID === selectedEventID)
    ? filteredInvocations
    : [invocation, ...filteredInvocations]
  const selectedViewIndex = selectableInvocations.findIndex((item) => item.scheduledEventID === selectedEventID)
  const nextException = nextExceptionalActivityInvocation(invocations, selectedEventID)
  const duration = invocationDuration(invocation)
  const selectedAttemptRecord = selectedAttempt == null
    ? null
    : invocation.attempts.find((attempt) =>
      attempt.startedEventID === selectedAttempt.eventID
        || attempt.completedEventID === selectedAttempt.eventID
        || attempt.errorEventID === selectedAttempt.eventID,
    ) ?? null
  return (
    <div className="mb-2 overflow-hidden border border-[#4e5157] bg-[#252628] font-mono text-[11px]">
      <div className="flex items-center gap-2 border-b border-[#393b40] px-2 py-1.5">
        <span className="min-w-0 flex-1 truncate text-[#dfe1e5]">
          {invocation.activityType} · Call {selectedIndex + 1}
        </span>
        <span className="rounded bg-[#34363b] px-1.5 py-0.5 text-[10px] text-[#bcbec4]">{invocation.status}</span>
        <button type="button" onClick={onClose} className="rounded p-0.5 hover:bg-[#43454a]" title="关闭 Activity Inspector">
          <X className="size-3.5" />
        </button>
      </div>
      {invocations.length > 1 && (
        <div className="border-b border-[#393b40] px-2 py-1">
          <div className="flex h-7 items-center gap-1">
            <button
              type="button"
              onClick={() => selectInvocation(selectableInvocations[selectedViewIndex - 1].scheduledEventID)}
              disabled={selectedViewIndex <= 0}
              className="rounded p-1 text-[#bcbec4] hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
              title="上一条 Activity 调用"
            >
              <ChevronLeft className="size-3.5" />
            </button>
            <ActivityInvocationPicker
              invocations={selectableInvocations}
              allInvocations={invocations}
              anchors={anchors}
              currentSeq={currentSeq}
              selectedEventID={selectedEventID}
              query={query}
              filter={filter}
              sort={sort}
              matchCount={filteredInvocations.length}
              onSelect={selectInvocation}
              onQueryChange={onQueryChange}
              onFilterChange={onFilterChange}
              onSortChange={onSortChange}
            />
            <button
              type="button"
              onClick={() => selectInvocation(selectableInvocations[selectedViewIndex + 1].scheduledEventID)}
              disabled={selectedViewIndex < 0 || selectedViewIndex === selectableInvocations.length - 1}
              className="rounded p-1 text-[#bcbec4] hover:bg-[#43454a] disabled:cursor-not-allowed disabled:opacity-30"
              title="下一条 Activity 调用"
            >
              <ChevronRightCollapse className="size-3.5" />
            </button>
            <span className="w-9 shrink-0 text-right tabular-nums text-[#7a7e85]">
              {selectedIndex + 1}/{invocations.length}
            </span>
            {nextException && (
              <button
                type="button"
                onClick={() => selectInvocation(nextException.scheduledEventID)}
                className="rounded p-1 text-[#cf6679] hover:bg-[#49343a]"
                title={`下一异常调用：Call ${invocations.indexOf(nextException) + 1}`}
              >
                <AlertTriangle className="size-3.5" />
              </button>
            )}
          </div>
        </div>
      )}
      <div className="grid grid-cols-[auto_1fr_auto_1fr] gap-x-3 gap-y-1 border-b border-[#393b40] px-2 py-1.5 text-[#a8adbd]">
        <span className="text-[#6f737a]">Event</span>
        <span>#{invocation.scheduledEventID}</span>
        <span className="text-[#6f737a]">Step</span>
        <span className="truncate">{invocation.stepID}</span>
        <span className="text-[#6f737a]">Attempts</span>
        <span>{invocation.attempts.length}</span>
        <span className="text-[#6f737a]">Duration</span>
        <span>{duration ?? "running"}</span>
        <span className="text-[#6f737a]">Iteration</span>
        <span>{selectedAnchor?.iteration ?? selectedIndex + 1}</span>
        <span className="text-[#6f737a]">Lane</span>
        <span className="truncate">{selectedAnchor?.laneID ?? "main"}</span>
        <span className="text-[#6f737a]">Horizon</span>
        <span className={cn(
          selectedPhase === "current" && "text-[#79c0ff]",
          selectedPhase === "past" && "text-[#6aab73]",
          selectedPhase === "future" && "text-[#e2b86b]",
        )}>
          {selectedPhase === "current" ? "当前调用" : selectedPhase === "past" ? "已发生" : "尚未发生"}
        </span>
        {invocation.resetEpochs.length > 0 && (
          <>
            <span className="text-[#6f737a]">Resets</span>
            <span>{invocation.resetEpochs.length}</span>
          </>
        )}
      </div>
      <div className="grid max-h-48 grid-cols-2 divide-x divide-[#393b40] border-b border-[#393b40]">
        <ActivityPayload title="Input" data={invocation.input} emptyText="No input" />
        <ActivityPayload title="Output" data={invocation.output} emptyText="No output yet" />
      </div>
      <div className="border-b border-[#393b40] bg-[#2d2e31] px-2 py-1 text-[10px] font-semibold text-[#a8adbd]">
        <span>Attempts ({invocation.attempts.length})</span>
        {selectedAttempt && (
          <span className="ml-2 font-normal text-[#79c0ff]">
            Event #{selectedAttempt.eventID} · {selectedAttempt.phase === "started" ? "Started" : "Outcome"}
          </span>
        )}
      </div>
      <div className="max-h-40 overflow-y-auto">
        {invocation.attempts.length === 0 ? (
          <div className="px-2 py-2 text-[#7a7e85]">等待 Worker 开始执行</div>
        ) : invocation.attempts.map((attempt, index) => {
          const duration = attemptDuration(attempt)
          const backoff = attemptBackoff(invocation.attempts, index)
          const startedAt = activityTimestamp(attempt.startedAt)
          const startTarget = activityAttemptTraceTarget(attempt, selectedAnchor, "started")
          const outcomeTarget = activityAttemptTraceTarget(attempt, selectedAnchor, "outcome")
          const defaultTarget = activityAttemptTraceTarget(attempt, selectedAnchor)
          const selected = selectedAttempt != null
            && (selectedAttempt.eventID === startTarget?.eventID || selectedAttempt.eventID === outcomeTarget?.eventID)
          return (
            <div
              key={`${attempt.resetEpoch}:${attempt.stamp ?? 0}:${attempt.attempt}:${index}`}
              className={cn(
                "flex min-h-8 items-center gap-2 border-b border-[#34363b] px-2 last:border-b-0",
                selected && "bg-[#2f3f55]",
              )}
            >
              <span className={cn(
                "size-1.5 shrink-0 rounded-full",
                attempt.status === "completed" && "bg-[#6aab73]",
                attempt.status === "started" && "bg-[#6897bb]",
                (attempt.status === "failed" || attempt.status === "timed_out") && "bg-[#cf6679]",
              )} />
              <button
                type="button"
                onClick={() => onSelectAttempt(attempt)}
                disabled={!defaultTarget}
                className="w-20 shrink-0 text-left text-[#dfe1e5] hover:text-white disabled:cursor-default disabled:text-[#6f737a]"
                title={defaultTarget ? `跳转到 Event #${defaultTarget.eventID}` : "当前 Attempt 没有可导航事件"}
              >
                Attempt {attempt.attempt}
              </button>
              {attempt.resetEpoch > 0 && <span className="text-[10px] text-[#cb7832]">reset {attempt.resetEpoch}</span>}
              <span className="text-[#a8adbd]">{attempt.status}</span>
              {attempt.workerIdentity && <span className="min-w-0 truncate text-[#6f737a]">{attempt.workerIdentity}</span>}
              {startedAt && <span className="shrink-0 text-[#6f737a]">{startedAt}</span>}
              {backoff && <span className="shrink-0 text-[#cb7832]">{backoff}</span>}
              <span className="ml-auto flex shrink-0 items-center gap-1">
                {startTarget && (
                  <button
                    type="button"
                    onClick={() => onSelectAttempt(attempt, "started")}
                    className={cn(
                      "rounded border border-[#4e5157] px-1 py-0.5 text-[9px] text-[#a8adbd] hover:bg-[#43454a] hover:text-[#dfe1e5]",
                      selectedAttempt?.eventID === startTarget.eventID && "border-[#79c0ff] bg-[#344b65] text-[#dfe1e5]",
                    )}
                    title={`跳转到 Started Event #${startTarget.eventID}`}
                  >
                    Start #{startTarget.eventID}
                  </button>
                )}
                {outcomeTarget && (
                  <button
                    type="button"
                    onClick={() => onSelectAttempt(attempt, "outcome")}
                    className={cn(
                      "rounded border border-[#4e5157] px-1 py-0.5 text-[9px] text-[#a8adbd] hover:bg-[#43454a] hover:text-[#dfe1e5]",
                      selectedAttempt?.eventID === outcomeTarget.eventID && "border-[#79c0ff] bg-[#344b65] text-[#dfe1e5]",
                    )}
                    title={`跳转到 Outcome Event #${outcomeTarget.eventID}`}
                  >
                    {attempt.status === "completed" ? "Done" : attempt.status === "timed_out" ? "Timeout" : "Failed"} #{outcomeTarget.eventID}
                  </button>
                )}
              </span>
              {duration && <span className="shrink-0 text-[#6f737a]">{duration}</span>}
              {attempt.error && <span className="min-w-0 truncate text-[#cf6679]" title={attempt.error}>{attempt.error}</span>}
            </div>
          )
        })}
      </div>
      {selectedAttempt && selectedAttemptRecord && (
        <div className="border-t border-[#393b40] bg-[#292a2d]">
          <div className="flex items-center gap-2 border-b border-[#393b40] px-2 py-1 text-[10px]">
            <span className="font-semibold text-[#dfe1e5]">Attempt {selectedAttemptRecord.attempt}</span>
            <span className="text-[#79c0ff]">
              {selectedAttempt.phase === "started" ? "Started" : "Outcome"} Event #{selectedAttempt.eventID}
            </span>
            <span className="ml-auto text-[#6f737a]">
              Trace seq {selectedAttempt.seq} · {selectedAttempt.laneID}
            </span>
          </div>
          <div className="grid grid-cols-[auto_1fr_auto_1fr] gap-x-3 gap-y-1 border-b border-[#393b40] px-2 py-1.5 text-[#a8adbd]">
            <span className="text-[#6f737a]">Worker</span>
            <span className="truncate">{selectedAttemptRecord.workerIdentity ?? "pending"}</span>
            <span className="text-[#6f737a]">Build</span>
            <span className="truncate">{selectedAttemptRecord.workerBuildID ?? "unknown"}</span>
            <span className="text-[#6f737a]">Started</span>
            <span>{activityTimestamp(selectedAttemptRecord.startedAt) ?? "pending"}</span>
            <span className="text-[#6f737a]">Outcome</span>
            <span>{activityTimestamp(selectedAttemptRecord.completedAt) ?? "running"}</span>
            {selectedAttemptRecord.retryState && (
              <>
                <span className="text-[#6f737a]">Retry</span>
                <span>{selectedAttemptRecord.retryState}</span>
              </>
            )}
          </div>
          <div className="grid max-h-40 grid-cols-2 divide-x divide-[#393b40]">
            <ActivityPayload title="Attempt Input" data={invocation.input} emptyText="No input" />
            <ActivityPayload title="Last Heartbeat" data={selectedAttemptRecord.lastHeartbeatDetails} emptyText="No heartbeat details" />
          </div>
          {selectedAttemptRecord.error && (
            <div className="border-t border-[#393b40] px-2 py-1.5 text-[#cf6679]">{selectedAttemptRecord.error}</div>
          )}
        </div>
      )}
      {invocation.error && <div className="border-t border-[#393b40] px-2 py-1.5 text-[#cf6679]">{invocation.error}</div>}
    </div>
  )
}
