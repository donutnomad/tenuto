"use client"

import { useState, Fragment } from "react"
import Link from "next/link"
import type { Step } from "@/lib/types"
import { STATUS_COLORS, STATUS_TEXT_COLORS } from "@/lib/constants"
import { formatDuration, formatTimestampWithAgo } from "@/lib/format"
import { ActivityTimeoutFields } from "./activity-scheduled-fields"
import { cn } from "@/lib/utils"
import { JsonViewer } from "./json-viewer"
import { ChevronDown, ChevronRight, ExternalLink, RotateCcw } from "lucide-react"

function StepRow({ step, namespace }: { step: Step; namespace: string }) {
  const [expanded, setExpanded] = useState(false)
  const dotColor = STATUS_COLORS[step.status] ?? "bg-gray-400"
  const textColor = STATUS_TEXT_COLORS[step.status] ?? "text-gray-400"

  return (
    <div className="border-b border-border last:border-b-0">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-3 px-4 py-3 text-left text-sm hover:bg-muted/40 transition-colors group"
      >
        {expanded ? (
          <ChevronDown className="size-3.5 shrink-0 text-muted-foreground/60 group-hover:text-muted-foreground transition-colors" />
        ) : (
          <ChevronRight className="size-3.5 shrink-0 text-muted-foreground/60 group-hover:text-muted-foreground transition-colors" />
        )}

        <span className="w-8 shrink-0 text-sm text-muted-foreground/60 font-mono tabular-nums">
          #{step.seq}
        </span>

        <span
          className={cn(
            "inline-block size-2 shrink-0 rounded-full shadow-sm",
            dotColor
          )}
        />

        <span className="flex-1 truncate font-mono text-sm">
          {step.activityType}
        </span>

        <span className="shrink-0 text-sm text-muted-foreground font-mono">
          {step.stepID}
        </span>

        <span className={cn("shrink-0 text-xs font-medium capitalize rounded-full px-1.5 py-0.5", textColor,
          step.status === 'running' ? 'bg-blue-50' :
          step.status === 'completed' ? 'bg-emerald-50' :
          step.status === 'failed' ? 'bg-red-50' :
          step.status === 'cancelled' ? 'bg-amber-50' : 'bg-muted/50'
        )}>
          {step.status.replace(/_/g, " ")}
        </span>

        {step.activityReset && (
          <span className="shrink-0 inline-flex items-center gap-0.5 text-xs text-amber-600 bg-amber-50 rounded-full px-1.5 py-0.5">
            <RotateCcw className="size-2.5" />
            reset
          </span>
        )}

        <span className="w-20 shrink-0 text-right text-sm text-muted-foreground">
          {formatDuration(step.duration)}
        </span>
      </button>

      {expanded && (
        <div className="border-t border-border/50 bg-muted/10">
          {/* Activity Task Scheduled */}
          <EventCard
            title="Activity Task Scheduled"
            eventId={step.scheduledEventID}
            timestamp={step.scheduledAt}
          >
            <div className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 text-sm">
              <Detail label="Activity Type" value={step.activityType} />
              <Detail label="Task Queue" value={step.taskQueue || "default"} />
              <Detail label="Step ID" value={step.stepID} />
              <ActivityTimeoutFields step={step} Detail={Detail} />
            </div>
            {step.input && (
              <div className="mt-3">
                <JsonViewer data={step.input} title="Input" />
              </div>
            )}
          </EventCard>

          {/* Attempt 记录（每次 attempt 展开为 Started + 结局两张卡） */}
          {step.attempts.map((a) => (
            <Fragment key={`attempt-${a.attempt}`}>
              {a.startedEventID != null && (
                <EventCard
                  title={`Activity Task Started (Attempt ${a.attempt})`}
                  eventId={a.startedEventID}
                  timestamp={a.startedAt}
                >
                  <div className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 text-sm">
                    <Detail label="Attempt" value={String(a.attempt)} />
                  </div>
                </EventCard>
              )}
              {a.status === "failed" && (
                <EventCard
                  title={`Activity Task Failed (Attempt ${a.attempt})`}
                  eventId={a.errorEventID}
                  timestamp={a.startedAt ?? a.completedAt}
                  variant="error"
                >
                  {a.error && (
                    <div className="rounded-md bg-red-500/10 px-3 py-2 text-sm text-red-500 font-mono">
                      {a.error}
                    </div>
                  )}
                </EventCard>
              )}
              {a.status === "timed_out" && (
                <EventCard
                  title={`Activity Task Timed Out (Attempt ${a.attempt})`}
                  eventId={a.errorEventID}
                  timestamp={a.startedAt ?? a.completedAt}
                  variant="error"
                >
                  {a.error && (
                    <div className="rounded-md bg-red-500/10 px-3 py-2 text-sm text-red-500 font-mono">
                      {a.error}
                    </div>
                  )}
                </EventCard>
              )}
              {a.status === "completed" && (
                <EventCard
                  title={`Activity Task Completed (Attempt ${a.attempt})`}
                  eventId={a.completedEventID}
                  timestamp={a.completedAt}
                >
                  <div className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 text-sm">
                    <Detail label="Attempt" value={String(a.attempt)} />
                  </div>
                  {step.output && (
                    <div className="mt-3">
                      <JsonViewer data={step.output} title="Result" />
                    </div>
                  )}
                </EventCard>
              )}
            </Fragment>
          ))}

          {/* 最后一次心跳：运行中来自 activity state，超时后来自 ActivityTimedOut */}
          {step.heartbeat && (
            <EventCard title="Last Heartbeat" timestamp={step.completedAt ?? step.startedAt}>
              <JsonViewer data={step.heartbeat} title="Details" />
            </EventCard>
          )}

          {step.status === "cancelled" && (
            <EventCard
              title="Activity Task Cancelled"
              eventId={step.completedEventID}
              timestamp={step.completedAt ?? step.startedAt}
              variant="warning"
            >
              {step.error && (
                <div className="rounded-md bg-amber-500/10 px-3 py-2 text-sm text-amber-500">
                  {step.error}
                </div>
              )}
            </EventCard>
          )}

          {/* Child Workflow Link */}
          {step.childRunID && (
            <div className="flex items-center gap-2 px-6 py-3 text-sm border-t border-border/30">
              <span className="text-muted-foreground">Child Run:</span>
              <Link
                href={`/${namespace}/runs/${step.childRunID}`}
                className="inline-flex items-center gap-1 font-mono text-primary hover:underline"
              >
                {step.childRunID}
                <ExternalLink className="size-3" />
              </Link>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function EventCard({
  title,
  eventId,
  timestamp,
  variant,
  children,
}: {
  title: string
  eventId?: number | null
  timestamp?: string | null
  variant?: "error" | "warning"
  children: React.ReactNode
}) {
  const borderColor =
    variant === "error" ? "border-l-red-400" :
    variant === "warning" ? "border-l-amber-400" :
    "border-l-primary/40"

  return (
    <div className={cn("border-t border-border/30 border-l-[3px] px-6 py-4", borderColor)}>
      <div className="flex items-baseline justify-between mb-3">
        <h4 className="text-[13px] font-semibold text-foreground flex items-baseline gap-2">
          {eventId != null && (
            <span className="text-xs font-mono text-muted-foreground tabular-nums">#{eventId}</span>
          )}
          {title}
        </h4>
        {timestamp && (
          <span className="text-xs text-muted-foreground tabular-nums">
            {formatTimestampWithAgo(timestamp)}
          </span>
        )}
      </div>
      {children}
    </div>
  )
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex gap-2">
      <span className="text-muted-foreground shrink-0">{label}</span>
      <span className="font-mono truncate">{value}</span>
    </div>
  )
}

export function StepTree({ steps, namespace }: { steps: Step[]; namespace: string }) {
  if (steps.length === 0) {
    return (
      <div className="py-8 text-center text-sm text-muted-foreground">
        No steps recorded yet.
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-border overflow-hidden">
      {steps.map((step) => (
        <StepRow key={`${step.seq}-${step.stepID}`} step={step} namespace={namespace} />
      ))}
    </div>
  )
}
