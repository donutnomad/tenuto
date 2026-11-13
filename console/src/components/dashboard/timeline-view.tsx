"use client"

import { useMemo, useState, useRef, useCallback } from "react"
import type { Step } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useNow } from "@/lib/use-now"
import { formatAxisTick, formatDuration, formatTimestampWithAgo } from "@/lib/format"

interface TimelineViewProps {
  steps: Step[]
}

interface BarSegment {
  left: number
  width: number
  className: string
  phase: "waiting" | "running" | "scheduled"
}

interface TooltipInfo {
  step: Step
  waitMs: number | null
  runMs: number | null
  x: number
  y: number
}

function computeTimeAxis(totalMs: number): { label: string; pct: number }[] {
  if (totalMs <= 0) return []
  const ticks: { label: string; pct: number }[] = []

  let count = 5
  if (totalMs < 1000) count = 4

  const stepMs = totalMs / count
  for (let i = 0; i <= count; i++) {
    const pct = (i / count) * 100
    ticks.push({ label: formatAxisTick(i * stepMs, stepMs), pct })
  }
  return ticks
}

function Tooltip({ info }: { info: TooltipInfo }) {
  const { step, waitMs, runMs } = info

  return (
    <div
      className="pointer-events-none fixed z-50 w-64 rounded-lg border border-border bg-popover px-3 py-2.5 shadow-xl text-sm"
      style={{ left: info.x, top: info.y }}
    >
      <div className="mb-1.5 font-mono font-medium truncate">{step.activityType}</div>
      <div className="space-y-1 text-muted-foreground">
        <Row label="Status">
          <span className={cn(
            "font-medium capitalize",
            step.status === "completed" && "text-emerald-400",
            step.status === "running" && "text-blue-400",
            step.status === "failed" && "text-red-400",
            step.status === "timed_out" && "text-orange-400",
            step.status === "scheduled" && "text-zinc-400",
          )}>
            {step.status.replace(/_/g, " ")}
          </span>
        </Row>
        <Row label="Scheduled">{formatTimestampWithAgo(step.scheduledAt)}</Row>
        {step.startedAt && <Row label="Started">{formatTimestampWithAgo(step.startedAt)}</Row>}
        {step.completedAt && <Row label="Completed">{formatTimestampWithAgo(step.completedAt)}</Row>}
        <div className="my-1.5 border-t border-border/50" />
        {waitMs !== null && (
          <Row label="Wait Time">
            <span className="font-mono text-zinc-300">{formatDuration(waitMs)}</span>
          </Row>
        )}
        {runMs !== null && (
          <Row label="Run Time">
            <span className="font-mono text-zinc-300">{formatDuration(runMs)}</span>
          </Row>
        )}
        {step.duration != null && (
          <Row label="Total">
            <span className="font-mono text-zinc-300">{formatDuration(step.duration)}</span>
          </Row>
        )}
        {step.workerIdentity && <Row label="Worker">{step.workerIdentity}</Row>}
        {step.taskQueue && <Row label="Queue">{step.taskQueue}</Row>}
      </div>
    </div>
  )
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex justify-between gap-2">
      <span className="shrink-0">{label}</span>
      <span className="truncate text-right text-foreground">{children}</span>
    </div>
  )
}

function StepBar({
  step,
  workflowStart,
  totalDuration,
  onHover,
  onLeave,
  isHighlighted,
}: {
  step: Step
  workflowStart: number
  totalDuration: number
  onHover: (info: TooltipInfo) => void
  onLeave: () => void
  isHighlighted: boolean
}) {
  const barRef = useRef<HTMLDivElement>(null)

  const { segments, waitMs, runMs } = useMemo(() => {
    if (totalDuration <= 0) return { segments: [] as BarSegment[], waitMs: null, runMs: null }
    const result: BarSegment[] = []

    const scheduledMs = new Date(step.scheduledAt).getTime() - workflowStart
    const startedMs = step.startedAt
      ? new Date(step.startedAt).getTime() - workflowStart
      : null
    const completedMs = step.completedAt
      ? new Date(step.completedAt).getTime() - workflowStart
      : null

    const toPct = (ms: number) => Math.max(0, Math.min(100, (ms / totalDuration) * 100))

    let waitDuration: number | null = null
    let runDuration: number | null = null

    if (startedMs !== null) {
      waitDuration = startedMs - scheduledMs
      const waitLeft = toPct(scheduledMs)
      const waitWidth = toPct(startedMs) - waitLeft
      if (waitWidth > 0.1) {
        result.push({ left: waitLeft, width: waitWidth, className: "bg-zinc-500/50", phase: "waiting" })
      }

      if (completedMs !== null) {
        runDuration = completedMs - startedMs
        const runLeft = toPct(startedMs)
        const runWidth = toPct(completedMs) - runLeft
        const isFailed = step.status === "failed" || step.status === "timed_out"
        result.push({
          left: runLeft,
          width: Math.max(runWidth, 0.3),
          className: isFailed ? "bg-red-500" : "bg-emerald-500",
          phase: "running",
        })
      } else {
        const runLeft = toPct(startedMs)
        const runWidth = 100 - runLeft
        result.push({
          left: runLeft,
          width: Math.max(runWidth, 0.3),
          className: "bg-blue-500 animate-pulse",
          phase: "running",
        })
      }
    } else {
      const schedLeft = toPct(scheduledMs)
      result.push({
        left: schedLeft,
        width: Math.max(100 - schedLeft, 0.5),
        className: "bg-zinc-600/40 animate-pulse",
        phase: "scheduled",
      })
    }

    return { segments: result, waitMs: waitDuration, runMs: runDuration }
  }, [step, workflowStart, totalDuration])

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      onHover({ step, waitMs, runMs, x: e.clientX + 12, y: e.clientY - 8 })
    },
    [step, waitMs, runMs, onHover]
  )

  const statusColor: Record<string, string> = {
    scheduled: "text-zinc-400",
    running: "text-blue-400",
    completed: "text-emerald-400",
    failed: "text-red-400",
    timed_out: "text-orange-400",
  }

  return (
    <div
      className={cn(
        "group flex items-center gap-0 transition-colors cursor-default",
        isHighlighted ? "bg-primary/5" : "hover:bg-muted/40"
      )}
      onMouseMove={handleMouseMove}
      onMouseLeave={onLeave}
    >
      <div className="w-48 shrink-0 flex items-center gap-2 px-3 py-1.5 border-r border-border">
        <span className="text-xs text-muted-foreground font-mono w-5 text-right shrink-0">
          #{step.seq}
        </span>
        <span className="text-sm font-mono truncate flex-1" title={step.activityType}>
          {step.activityType}
        </span>
      </div>

      <div className="flex-1 flex items-center gap-2 px-3 py-1.5 min-w-0">
        <div ref={barRef} className="flex-1 relative h-5 rounded bg-muted/30">
          {segments.map((seg, i) => (
            <div
              key={i}
              className={cn(
                "absolute top-0 h-full rounded transition-opacity",
                seg.className,
                isHighlighted ? "opacity-100" : "group-hover:opacity-90"
              )}
              style={{
                left: `${seg.left}%`,
                width: `${seg.width}%`,
                minWidth: "2px",
              }}
            />
          ))}
        </div>
        <span
          className={cn(
            "w-16 shrink-0 text-right text-xs font-mono tabular-nums",
            statusColor[step.status] ?? "text-zinc-400"
          )}
        >
          {step.duration != null ? formatDuration(step.duration) : step.status}
        </span>
      </div>
    </div>
  )
}

export function TimelineView({ steps }: TimelineViewProps) {
  // 有进行中的 step 时时间轴右端跟随当前时间，每秒更新。
  const nowMs = useNow(steps.some((step) => step.status === "scheduled" || step.status === "running"))
  const [tooltip, setTooltip] = useState<TooltipInfo | null>(null)

  const handleHover = useCallback((info: TooltipInfo) => {
    setTooltip(info)
  }, [])

  const handleLeave = useCallback(() => {
    setTooltip(null)
  }, [])

  const { workflowStart, totalDuration } = useMemo(() => {
    if (steps.length === 0) return { workflowStart: 0, totalDuration: 0 }

    let earliest = Infinity
    let latest = -Infinity

    for (const step of steps) {
      const scheduled = new Date(step.scheduledAt).getTime()
      if (scheduled < earliest) earliest = scheduled

      if (step.completedAt) {
        const completed = new Date(step.completedAt).getTime()
        if (completed > latest) latest = completed
      } else if (step.startedAt) {
        if (nowMs > latest) latest = nowMs
      } else {
        if (nowMs > latest) latest = nowMs
      }
    }

    if (earliest === Infinity) return { workflowStart: 0, totalDuration: 0 }
    return {
      workflowStart: earliest,
      totalDuration: Math.max(latest - earliest, 1),
    }
  }, [nowMs, steps])

  const ticks = useMemo(
    () => computeTimeAxis(totalDuration),
    [totalDuration]
  )

  if (steps.length === 0) {
    return (
      <div className="py-8 text-center text-sm text-muted-foreground">
        No steps recorded yet.
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-border overflow-hidden shadow-sm">
      <div className="flex items-center gap-0 border-b border-border bg-muted/40">
        <div className="w-48 shrink-0 px-3 py-2 border-r border-border">
          <span className="text-xs font-semibold text-muted-foreground/70">
            Execution
          </span>
        </div>
        <div className="flex-1 px-3 py-2 min-w-0">
          <div className="relative h-4">
            {ticks.map((tick, i) => (
              <span
                key={i}
                className="absolute text-xs text-muted-foreground font-mono tabular-nums -translate-x-1/2 whitespace-nowrap"
                style={{ left: `${tick.pct}%` }}
              >
                {tick.label}
              </span>
            ))}
          </div>
        </div>
        <div className="w-16 shrink-0" />
      </div>

      <div className="relative divide-y divide-border/40">
        <div className="absolute inset-0 pointer-events-none" style={{ left: "192px", right: "80px" }}>
          {ticks.map((tick, i) => (
            <div
              key={i}
              className="absolute top-0 bottom-0 border-l border-border/20"
              style={{ left: `${tick.pct}%` }}
            />
          ))}
        </div>

        {steps.map((step) => (
          <StepBar
            key={`${step.seq}-${step.stepID}`}
            step={step}
            workflowStart={workflowStart}
            totalDuration={totalDuration}
            onHover={handleHover}
            onLeave={handleLeave}
            isHighlighted={tooltip?.step.seq === step.seq}
          />
        ))}
      </div>

      <div className="flex items-center gap-5 px-4 py-2 border-t border-border bg-muted/40">
        <LegendItem className="bg-zinc-400/60" label="Waiting" />
        <LegendItem className="bg-blue-500" label="Running" />
        <LegendItem className="bg-emerald-500" label="Completed" />
        <LegendItem className="bg-red-500" label="Failed" />
      </div>

      {tooltip && <Tooltip info={tooltip} />}
    </div>
  )
}

function LegendItem({ className, label }: { className: string; label: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className={cn("w-3 h-2 rounded-sm", className)} />
      <span className="text-xs text-muted-foreground">{label}</span>
    </div>
  )
}
