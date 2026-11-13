"use client"

import { useMemo } from "react"
import type { Step } from "@/lib/types"
import { formatTimestamp, formatTimestampWithAgo, formatDuration, timeAgo } from "@/lib/format"
import { lastFailedAttempt as findLastFailedAttempt } from "@/lib/run-detail"
import { useNow } from "@/lib/use-now"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { AlertTriangle, Clock, Heart, Loader2, RotateCcw, User } from "lucide-react"

export function PendingStepCard({
  step,
  onReset,
}: {
  step: Step
  onReset?: (step: Step) => void
}) {
  const nowMs = useNow(true)
  const elapsed = useMemo(() => {
    const start = step.startedAt ?? step.scheduledAt
    if (!start) return null
    return nowMs > 0 ? nowMs - new Date(start).getTime() : null
  }, [nowMs, step.startedAt, step.scheduledAt])

  const lastFailedAttempt = findLastFailedAttempt(step)
  // activity-states 的 attempt 是当前（或退避后下一次）尝试的序号；没有时按 History 里出现过的尝试计数。
  const attempt = step.currentAttempt ?? step.attempts.length

  return (
    <div className="rounded-lg border border-border p-4 space-y-3">
      <div className="flex items-center justify-between text-sm">
        <div className="flex items-center gap-3">
          <span className={step.status === "running" ? "text-blue-500" : "text-amber-500"}>
            {step.status === "running" ? <Loader2 className="size-3.5 animate-spin" /> : <Clock className="size-3.5" />}
          </span>
          <div>
            <span className="font-mono font-medium">{step.activityType}</span>
            <span className="ml-2 text-muted-foreground">({step.stepID})</span>
          </div>
        </div>
        <div className="flex items-center gap-4 text-muted-foreground">
          {step.taskQueue && <span>queue: {step.taskQueue}</span>}
          {elapsed !== null && <span className="tabular-nums">{formatDuration(elapsed)}</span>}
          <Badge variant="secondary">{step.status}</Badge>
          {onReset && (
            <Button variant="outline" size="sm" onClick={() => onReset(step)}>
              <RotateCcw className="size-3.5" />
              Reset
            </Button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-sm sm:grid-cols-4">
        {attempt > 0 && (
          <div className="flex items-center gap-1.5">
            <AlertTriangle className="size-3 text-amber-500" />
            <span className="text-muted-foreground">Attempt:</span>
            <span>{attempt}</span>
          </div>
        )}
        {step.workerIdentity && (
          <div className="flex items-center gap-1.5">
            <User className="size-3 text-muted-foreground" />
            <span className="text-muted-foreground">Worker:</span>
            <span className="font-mono truncate">{step.workerIdentity}</span>
          </div>
        )}
        {step.scheduledAt && (
          <div className="flex gap-1.5">
            <span className="text-muted-foreground">Scheduled:</span>
            <span>{formatTimestampWithAgo(step.scheduledAt)}</span>
          </div>
        )}
        {step.startedAt && (
          <div className="flex gap-1.5">
            <span className="text-muted-foreground">Started:</span>
            <span>{formatTimestampWithAgo(step.startedAt)}</span>
          </div>
        )}
        {step.nextAttemptAt && (
          <div className="flex gap-1.5">
            <span className="text-muted-foreground">Next attempt:</span>
            <span>{formatTimestamp(step.nextAttemptAt)} ({timeAgo(step.nextAttemptAt)})</span>
          </div>
        )}
      </div>

      {step.heartbeat && (
        <div className="flex items-start gap-1.5 text-sm">
          <Heart className="size-3 mt-0.5 text-pink-400" />
          <div className="min-w-0 flex-1">
            <span className="text-muted-foreground">Last heartbeat:</span>
            <pre className="mt-1 overflow-x-auto rounded bg-muted/50 p-2 font-mono text-xs">
              {JSON.stringify(step.heartbeat, null, 2)}
            </pre>
          </div>
        </div>
      )}

      {step.lastFailure ? (
        <div className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <span className="font-medium">Last failure:</span> {step.lastFailure}
        </div>
      ) : lastFailedAttempt && (
        <div className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <span className="font-medium">Last failure (attempt {lastFailedAttempt.attempt}):</span> {lastFailedAttempt.error}
        </div>
      )}
    </div>
  )
}
