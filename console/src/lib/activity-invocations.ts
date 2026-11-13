import type { Event } from "./types"
import type { TraceStep } from "./wasm-loader"

export interface ActivityAttemptRecord {
  resetEpoch: number
  stamp?: number
  attempt: number
  status: "started" | "failed" | "completed" | "timed_out"
  startedAt?: string
  completedAt?: string
  startedEventID?: number
  completedEventID?: number
  errorEventID?: number
  workerIdentity?: string
  workerBuildID?: string
  error?: string
  retryState?: string
  // 超时的 attempt 最后一次心跳的详情，来自 ActivityTimedOut；心跳本身不写 History。
  lastHeartbeatDetails?: Record<string, unknown>
}

export interface ActivityResetEpoch {
  resetEventID: number
  timestamp: string
  resetEpoch: number
  reason?: string
}

export interface ActivityInvocationRecord {
  invocationID: string
  stepID: string
  scheduledEventID: number
  activityType: string
  status: "scheduled" | "running" | "completed" | "failed" | "timed_out" | "cancelled"
  input?: Record<string, unknown>
  output?: Record<string, unknown>
  error?: string
  attempts: ActivityAttemptRecord[]
  resetEpochs: ActivityResetEpoch[]
}

export interface ActivityInvocationTraceAnchor {
  scheduledEventID: number
  laneID: string
  line: number
  stepSeq: number
  laneSeq: number
  iteration: number
  waitSeq?: number
  resumeSeq?: number
}

export type ActivityAttemptTracePhase = "started" | "outcome"

export interface ActivityAttemptTraceTarget {
  eventID: number
  laneID: string
  seq: number
  phase: ActivityAttemptTracePhase
}

export type ActivityInvocationCursorPhase = "past" | "current" | "future"
export type ActivityInvocationFilter = "all" | "current" | "past" | "future" | "completed" | "failed" | "running" | "timed_out" | "cancelled"
export type ActivityInvocationSort = "sequence" | "duration-desc" | "duration-asc"

const ACTIVITY_SUMMARY_KEYS = [
  "wallet_address",
  "order_id",
  "reservation_id",
  "payment_id",
  "id",
  "name",
  "amount",
]

function summaryValue(value: unknown): string | null {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value)
  }
  return value === null ? "nil" : null
}

export function activityInvocationSummary(invocation: ActivityInvocationRecord): string {
  const input = invocation.input ?? {}
  const orderedKeys = [
    ...ACTIVITY_SUMMARY_KEYS.filter((key) => key in input),
    ...Object.keys(input).filter((key) => !ACTIVITY_SUMMARY_KEYS.includes(key)),
  ]
  for (const key of orderedKeys) {
    const value = summaryValue(input[key])
    if (value != null) return `${key}=${value}`
  }
  return `event ${invocation.scheduledEventID}`
}

export function activityInvocationCursorPhase(
  anchor: ActivityInvocationTraceAnchor | undefined,
  currentSeq: number,
): ActivityInvocationCursorPhase {
  if (!anchor || anchor.stepSeq > currentSeq) return "future"
  if (anchor.stepSeq === currentSeq) return "current"
  return "past"
}

export function activityInvocationDurationMs(invocation: ActivityInvocationRecord): number | null {
  const startedAt = invocation.attempts.find((attempt) => attempt.startedAt)?.startedAt
  const completedAt = [...invocation.attempts].reverse().find((attempt) => attempt.completedAt)?.completedAt
  if (!startedAt || !completedAt) return null
  const duration = new Date(completedAt).getTime() - new Date(startedAt).getTime()
  return Number.isFinite(duration) && duration >= 0 ? duration : null
}

export function selectActivityInvocations(
  invocations: readonly ActivityInvocationRecord[],
  anchors: ReadonlyMap<number, ActivityInvocationTraceAnchor>,
  currentSeq: number,
  query: string,
  filter: ActivityInvocationFilter,
  sort: ActivityInvocationSort,
): ActivityInvocationRecord[] {
  const normalizedQuery = query.trim().toLowerCase()
  const selected = invocations.filter((invocation) => {
    const anchor = anchors.get(invocation.scheduledEventID)
    const phase = activityInvocationCursorPhase(anchor, currentSeq)
    if (filter !== "all" && invocation.status !== filter && phase !== filter) return false
    if (!normalizedQuery) return true
    return [
      activityInvocationSummary(invocation),
      invocation.activityType,
      invocation.stepID,
      invocation.status,
      String(invocation.scheduledEventID),
      anchor?.laneID ?? "",
      JSON.stringify(invocation.input ?? {}),
    ].join(" ").toLowerCase().includes(normalizedQuery)
  })
  if (sort === "sequence") return selected
  const direction = sort === "duration-desc" ? -1 : 1
  return selected
    .map((invocation, index) => ({ invocation, index, duration: activityInvocationDurationMs(invocation) }))
    .sort((left, right) => {
      if (left.duration == null && right.duration == null) return left.index - right.index
      if (left.duration == null) return 1
      if (right.duration == null) return -1
      return (left.duration - right.duration) * direction || left.index - right.index
    })
    .map(({ invocation }) => invocation)
}

export function nextExceptionalActivityInvocation(
  invocations: readonly ActivityInvocationRecord[],
  selectedEventID: number,
): ActivityInvocationRecord | null {
  if (invocations.length === 0) return null
  const selectedIndex = invocations.findIndex((invocation) => invocation.scheduledEventID === selectedEventID)
  for (let offset = 1; offset <= invocations.length; offset++) {
    const index = (Math.max(selectedIndex, -1) + offset) % invocations.length
    const invocation = invocations[index]
    if (
      invocation.status === "failed"
      || invocation.status === "timed_out"
      || invocation.status === "cancelled"
      || invocation.attempts.some((attempt) => attempt.status === "failed" || attempt.status === "timed_out")
    ) return invocation
  }
  return null
}

export interface ActivityInvocationVirtualWindow {
  start: number
  end: number
  offsetTop: number
  totalHeight: number
}

export function activityInvocationVirtualWindow(
  count: number,
  scrollTop: number,
  rowHeight: number,
  viewportHeight: number,
  overscan: number,
): ActivityInvocationVirtualWindow {
  const safeCount = Math.max(0, count)
  const safeRowHeight = Math.max(1, rowHeight)
  const safeScrollTop = Math.max(0, scrollTop)
  const visibleStart = Math.floor(safeScrollTop / safeRowHeight)
  const visibleEnd = Math.ceil((safeScrollTop + Math.max(0, viewportHeight)) / safeRowHeight)
  const start = Math.max(0, visibleStart - Math.max(0, overscan))
  const end = Math.min(safeCount, visibleEnd + Math.max(0, overscan))
  return {
    start,
    end,
    offsetTop: start * safeRowHeight,
    totalHeight: safeCount * safeRowHeight,
  }
}

export function activityAttemptTraceTarget(
  attempt: ActivityAttemptRecord,
  anchor: ActivityInvocationTraceAnchor | undefined,
  requestedPhase?: ActivityAttemptTracePhase,
): ActivityAttemptTraceTarget | null {
  if (!anchor) return null
  const outcomeEventID = attempt.completedEventID ?? attempt.errorEventID
  const phase = requestedPhase ?? (outcomeEventID == null ? "started" : "outcome")
  if (phase === "started") {
    if (attempt.startedEventID == null) return null
    return {
      eventID: attempt.startedEventID,
      laneID: anchor.laneID,
      seq: anchor.waitSeq ?? anchor.stepSeq,
      phase,
    }
  }
  if (outcomeEventID == null) return null
  const resumesWorkflow = attempt.completedEventID != null || attempt.status === "timed_out"
  return {
    eventID: outcomeEventID,
    laneID: anchor.laneID,
    seq: resumesWorkflow
      ? anchor.resumeSeq ?? anchor.waitSeq ?? anchor.stepSeq
      : anchor.waitSeq ?? anchor.stepSeq,
    phase,
  }
}

interface MutableInvocation extends ActivityInvocationRecord {
  epoch: number
}

function findAttempt(
  invocation: MutableInvocation,
  attempt: number,
  stamp?: number,
): ActivityAttemptRecord {
  const existing = [...invocation.attempts].reverse().find((item) =>
    item.resetEpoch === invocation.epoch
      && item.attempt === attempt
      && (stamp == null || item.stamp == null || item.stamp === stamp),
  )
  if (existing) {
    if (stamp != null) existing.stamp = stamp
    return existing
  }
  const created: ActivityAttemptRecord = {
    resetEpoch: invocation.epoch,
    stamp,
    attempt,
    status: "started",
  }
  invocation.attempts.push(created)
  return created
}

export function buildActivityInvocationIndex(events: Event[]): Map<number, ActivityInvocationRecord> {
  const invocations = new Map<number, MutableInvocation>()

  for (const event of events) {
    if (event.workflowCancelled) {
      for (const invocation of invocations.values()) {
        if (invocation.status === "scheduled" || invocation.status === "running") {
          invocation.status = "cancelled"
        }
      }
      continue
    }
    if (event.activityScheduled) {
      invocations.set(event.id, {
        invocationID: `activity:${event.id}`,
        stepID: event.activityScheduled.activityId ?? "",
        scheduledEventID: event.id,
        activityType: event.activityScheduled.activityType ?? "",
        status: "scheduled",
        input: event.activityScheduled.input,
        attempts: [],
        resetEpochs: [],
        epoch: 0,
      })
      continue
    }

    if (event.activityReset) {
      const invocation = invocations.get(event.activityReset.scheduledEventId ?? 0)
      if (!invocation) continue
      invocation.epoch++
      invocation.status = "scheduled"
      invocation.resetEpochs.push({
        resetEventID: event.id,
        timestamp: event.timestamp,
        resetEpoch: invocation.epoch,
        reason: event.activityReset.reason,
      })
      continue
    }

    const scheduledEventID = event.activityStarted?.scheduledEventId
      ?? event.activityCompleted?.scheduledEventId
      ?? event.activityFailed?.scheduledEventId
      ?? event.activityAttemptFailed?.scheduledEventId
      ?? event.activityTimedOut?.scheduledEventId
      ?? event.activityCancelled?.scheduledEventId
    if (scheduledEventID == null) continue
    const invocation = invocations.get(scheduledEventID)
    if (!invocation) continue

    if (event.activityStarted) {
      const attempt = findAttempt(invocation, event.activityStarted.attempt ?? 0, event.activityStarted.stamp)
      attempt.status = "started"
      attempt.startedAt = event.timestamp
      attempt.startedEventID = event.id
      attempt.workerIdentity = event.activityStarted.workerIdentity
      attempt.workerBuildID = event.activityStarted.workerBuildId
      invocation.status = "running"
    }
    if (event.activityAttemptFailed) {
      const attempt = findAttempt(invocation, event.activityAttemptFailed.attempt ?? 0, event.activityAttemptFailed.stamp)
      attempt.status = "failed"
      attempt.completedAt = event.timestamp
      attempt.errorEventID = event.id
      attempt.error = event.activityAttemptFailed.error
      invocation.status = "running"
    }
    if (event.activityTimedOut) {
      const attempt = findAttempt(invocation, event.activityTimedOut.attempt ?? 0, event.activityTimedOut.stamp)
      attempt.status = "timed_out"
      attempt.completedAt = event.timestamp
      attempt.errorEventID = event.id
      attempt.error = event.activityTimedOut.timeoutType
      attempt.retryState = event.activityTimedOut.retryState
      attempt.lastHeartbeatDetails = event.activityTimedOut.lastHeartbeatDetails
      invocation.status = "timed_out"
      invocation.error = event.activityTimedOut.timeoutType
    }
    if (event.activityFailed) {
      const attempt = findAttempt(invocation, event.activityFailed.attempt ?? 0)
      attempt.status = "failed"
      attempt.completedAt = event.timestamp
      attempt.completedEventID = event.id
      attempt.errorEventID = event.id
      attempt.error = event.activityFailed.error
      attempt.retryState = event.activityFailed.retryState
      invocation.status = "failed"
      invocation.error = event.activityFailed.error
    }
    if (event.activityCancelled) {
      invocation.status = "cancelled"
      invocation.error = "cancelled"
    }
    if (event.activityCompleted) {
      const attempt = findAttempt(invocation, event.activityCompleted.attempt ?? 0)
      attempt.status = "completed"
      attempt.completedAt = event.timestamp
      attempt.completedEventID = event.id
      invocation.status = "completed"
      invocation.output = event.activityCompleted.output
    }
  }

  return new Map(Array.from(invocations, ([eventID, invocation]) => [eventID, {
    invocationID: invocation.invocationID,
    stepID: invocation.stepID,
    scheduledEventID: invocation.scheduledEventID,
    activityType: invocation.activityType,
    status: invocation.status,
    input: invocation.input,
    output: invocation.output,
    error: invocation.error,
    attempts: invocation.attempts,
    resetEpochs: invocation.resetEpochs,
  }]))
}

export function buildActivityInvocationsByLine(
  steps: readonly TraceStep[],
  activityIndex: ReadonlyMap<number, ActivityInvocationRecord>,
): Map<number, ActivityInvocationRecord[]> {
  const byLine = new Map<number, ActivityInvocationRecord[]>()
  const seen = new Set<string>()
  for (const step of steps) {
    for (const effect of step.effects ?? []) {
      const scheduledEventID = effect.scheduled_event_id
      if (effect.kind !== "activity" || effect.phase !== "scheduled" || scheduledEventID == null) continue
      const invocation = activityIndex.get(scheduledEventID)
      if (!invocation) continue
      const key = `${step.line}:${invocation.invocationID}`
      if (seen.has(key)) continue
      seen.add(key)
      const lineInvocations = byLine.get(step.line) ?? []
      lineInvocations.push(invocation)
      byLine.set(step.line, lineInvocations)
    }
  }
  for (const invocations of byLine.values()) {
    invocations.sort((left, right) => left.scheduledEventID - right.scheduledEventID)
  }
  return byLine
}

export function buildActivityInvocationTraceIndex(
  steps: readonly TraceStep[],
): Map<number, ActivityInvocationTraceAnchor> {
  const anchors = new Map<number, ActivityInvocationTraceAnchor>()
  for (const step of steps) {
    for (const effect of step.effects ?? []) {
      const scheduledEventID = effect.scheduled_event_id
      if (
        effect.kind !== "activity"
        || effect.phase !== "scheduled"
        || scheduledEventID == null
        || anchors.has(scheduledEventID)
      ) continue
      anchors.set(scheduledEventID, {
        scheduledEventID,
        laneID: step.lane_id,
        line: step.line,
        stepSeq: effect.call_seq ?? step.seq,
        laneSeq: step.lane_seq,
        iteration: 0,
        waitSeq: effect.wait_seq,
        resumeSeq: effect.resume_seq,
      })
    }
  }

  const byLine = new Map<number, ActivityInvocationTraceAnchor[]>()
  for (const anchor of anchors.values()) {
    const lineAnchors = byLine.get(anchor.line) ?? []
    lineAnchors.push(anchor)
    byLine.set(anchor.line, lineAnchors)
  }
  for (const lineAnchors of byLine.values()) {
    lineAnchors.sort((left, right) => left.stepSeq - right.stepSeq)
    lineAnchors.forEach((anchor, index) => {
      anchor.iteration = index + 1
    })
  }
  return anchors
}
