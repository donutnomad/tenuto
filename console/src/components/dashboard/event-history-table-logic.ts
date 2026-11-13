import type { Event, EventType } from "@/lib/types"
import { RUN_DETAIL_EVENT_CATEGORIES } from "@/lib/run-detail"

export type EventHistoryScope = "all" | "workflowTasks" | "activityAttempts"

export interface EventHistoryFilters {
  query: string
  scope: EventHistoryScope
  failedOnly: boolean
}

const EVENT_NAMES: Record<EventType, string> = {
  workflowStarted: "Workflow Execution Started",
  workflowCompleted: "Workflow Execution Completed",
  workflowFailed: "Workflow Execution Failed",
  workflowCancelled: "Workflow Execution Cancelled",
  workflowTerminated: "Workflow Execution Terminated",
  workflowContinuedAsNew: "Workflow Execution Continued As New",
  workflowCancellationRequested: "Workflow Execution Cancellation Requested",
  workflowTaskScheduled: "Workflow Task Scheduled",
  workflowTaskStarted: "Workflow Task Started",
  workflowTaskCompleted: "Workflow Task Completed",
  workflowTaskTimedOut: "Workflow Task Timed Out",
  workflowTaskFailed: "Workflow Task Failed",
  activityScheduled: "Activity Task Scheduled",
  activityStarted: "Activity Task Started",
  activityCompleted: "Activity Task Completed",
  activityFailed: "Activity Task Failed",
  activityAttemptFailed: "Activity Task Attempt Failed",
  activityTimedOut: "Activity Task Timed Out",
  activityReset: "Activity Task Reset",
  activityCancelRequested: "Activity Task Cancel Requested",
  activityCancelled: "Activity Task Cancelled",
  timerScheduled: "Timer Scheduled",
  timerFired: "Timer Fired",
  timerCancelled: "Timer Cancelled",
  childWorkflowStarted: "Child Workflow Execution Started",
  childWorkflowCompleted: "Child Workflow Execution Completed",
  childWorkflowFailed: "Child Workflow Execution Failed",
  childWorkflowCancelled: "Child Workflow Execution Cancelled",
  childWorkflowTerminated: "Child Workflow Execution Terminated",
  childWorkflowCancelRequested: "Child Workflow Execution Cancel Requested",
  workflowSignaled: "Workflow Execution Signaled",
  workflowUpdateAccepted: "Workflow Update Accepted",
  workflowUpdateCompleted: "Workflow Update Completed",
  markerRecorded: "Marker Recorded",
  searchAttributesUpserted: "Upsert Workflow Search Attributes",
}

export function eventTypeOf(event: Event): string {
  const keys = Object.keys(event).filter(
    (key) => key !== "id" && key !== "timestamp" && event[key as keyof Event] != null
  )
  return keys[0] ?? "unknown"
}

// 合法 reset 锚点：与后端 resolveResetNodeID 校验一致——WorkflowTask 边界事件（close 或 Started）。
// Started 与关闭事件选择同一轮：后端截至该轮 Started，并重新执行该轮。
// reset 在该 event 处 fork+截断 history，落在 WFT 中间（如 ActivityScheduled）会破坏重放一致性。
const RESETABLE_EVENT_TYPES = new Set([
  "workflowTaskStarted",
  "workflowTaskCompleted",
  "workflowTaskTimedOut",
  "workflowTaskFailed",
])

export function resetAnchorEvents(events: Event[]): Event[] {
  return events.filter((event) => RESETABLE_EVENT_TYPES.has(eventTypeOf(event)))
}

// 按 Run 详情页的分类（RUN_DETAIL_EVENT_CATEGORIES 的键）筛选事件；不选分类时返回原数组。
export function filterEventsByCategory(events: Event[], categories: string[]): Event[] {
  if (categories.length === 0) return events
  const allowed = new Set<string>(
    categories.flatMap((category) => RUN_DETAIL_EVENT_CATEGORIES[category] ?? []),
  )
  return events.filter((event) => allowed.has(eventTypeOf(event)))
}

export function eventDisplayName(eventType: string): string {
  if (eventType === "unknown") return "Unknown"
  if (eventType in EVENT_NAMES) return EVENT_NAMES[eventType as EventType]
  const spaced = eventType.replace(/([A-Z])/g, " $1")
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
}

function appendSearchableScalars(value: unknown, values: string[], seen: WeakSet<object>): void {
  if (value == null) return
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    values.push(String(value))
    return
  }
  if (typeof value !== "object" || seen.has(value)) return
  seen.add(value)

  if (Array.isArray(value)) {
    value.forEach((item) => appendSearchableScalars(item, values, seen))
    return
  }

  Object.entries(value).forEach(([key, child]) => {
    values.push(key)
    appendSearchableScalars(child, values, seen)
  })
}

export function eventSearchText(event: Event): string {
  const type = eventTypeOf(event)
  const values = [String(event.id), type, eventDisplayName(type)]
  appendSearchableScalars(event[type as keyof Event], values, new WeakSet())
  return values.join(" ").toLocaleLowerCase()
}

export function isFailedEvent(event: Event): boolean {
  const type = eventTypeOf(event).toLocaleLowerCase()
  return type.includes("failed") || type.includes("timedout")
}

export function isWorkflowTaskEvent(event: Event): boolean {
  return eventTypeOf(event).startsWith("workflowTask")
}

export function isActivityAttemptEvent(event: Event): boolean {
  const type = eventTypeOf(event)
  if (!type.startsWith("activity")) return false
  const attrs = event[type as keyof Event] as Record<string, unknown> | undefined
  return typeof attrs?.attempt === "number"
}

export function filterEventHistory(events: Event[], filters: EventHistoryFilters): Event[] {
  const query = filters.query.trim().toLocaleLowerCase()

  return events.filter((event) => {
    if (filters.failedOnly && !isFailedEvent(event)) return false
    if (filters.scope === "workflowTasks" && !isWorkflowTaskEvent(event)) return false
    if (filters.scope === "activityAttempts" && !isActivityAttemptEvent(event)) return false
    return query.length === 0 || eventSearchText(event).includes(query)
  })
}

export function isCausalEventIdField(field: string): boolean {
  return field.endsWith("EventId")
}

// command 事件：产生于某轮 WFT 的调度指令，需要回指"产生它的那轮 WFT completed"
export const COMMAND_EVENT_TYPES = new Set<string>([
  "activityScheduled",
  "activityCancelRequested",
  "timerScheduled",
  "timerCancelled",
  "markerRecorded",
  "childWorkflowStarted",
  "childWorkflowCancelRequested",
  "workflowContinuedAsNew",
  "workflowCompleted",
  "workflowFailed",
  "workflowCancelled",
])

// WFT close 事件：需要回指对应的 workflowTaskStarted
export const WFT_CLOSE_TYPES = new Set<string>(["workflowTaskCompleted", "workflowTaskTimedOut"])

export interface CausalMaps {
  // command 事件 id → 产生它的 workflowTaskCompleted 事件 id（0 表示之前没有 WFT completed）
  sourceWft: Record<number, number>
  // WFT close 事件 id → 同一 scheduledEventId 的 workflowTaskStarted 事件 id
  wftStarted: Record<number, number>
  // workflowUpdateCompleted 事件 id → 同一 updateId 的 workflowUpdateAccepted 事件 id
  updateAccepted: Record<number, number>
}

// 按 id 升序单趟扫描，用 Map 记住每个 scheduledEventId / updateId 最近一次出现的位置。
export function buildCausalMaps(events: Event[]): CausalMaps {
  const sorted = [...events].sort((a, b) => a.id - b.id)
  const maps: CausalMaps = { sourceWft: {}, wftStarted: {}, updateAccepted: {} }
  const startedByScheduledId = new Map<number | undefined, number>()
  const acceptedByUpdateId = new Map<string, number>()
  let lastWftCompletedId = 0

  for (const event of sorted) {
    const type = eventTypeOf(event)

    // command 事件回指它之前最近的 WFT completed（先赋值再更新 lastWftCompletedId）
    if (COMMAND_EVENT_TYPES.has(type)) maps.sourceWft[event.id] = lastWftCompletedId

    if (event.workflowTaskStarted) {
      startedByScheduledId.set(event.workflowTaskStarted.scheduledEventId, event.id)
    }
    const closed = event.workflowTaskCompleted ?? event.workflowTaskTimedOut
    if (closed) {
      const startedId = startedByScheduledId.get(closed.scheduledEventId)
      if (startedId != null) maps.wftStarted[event.id] = startedId
    }
    if (event.workflowTaskCompleted) lastWftCompletedId = event.id

    const acceptedUpdateId = event.workflowUpdateAccepted?.updateId
    if (acceptedUpdateId) acceptedByUpdateId.set(acceptedUpdateId, event.id)

    const completedUpdateId = event.workflowUpdateCompleted?.updateId
    const acceptedId = completedUpdateId ? acceptedByUpdateId.get(completedUpdateId) : undefined
    if (acceptedId != null) maps.updateAccepted[event.id] = acceptedId
  }

  return maps
}

// decodePayloads 把 protocol.Payload（{encoding: "json/plain-v1", data: <base64>}）换成它编码的 JSON 值，
// 递归处理对象与数组；Update 的 args/outcome、取消与失败的 details 都是这种载荷。其他编码与解不开的数据原样保留。
export function decodePayloads(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(decodePayloads)
  if (value === null || typeof value !== "object") return value
  const record = value as Record<string, unknown>
  const keys = Object.keys(record)
  if (
    record.encoding === "json/plain-v1" &&
    typeof record.data === "string" &&
    keys.every((key) => key === "encoding" || key === "data")
  ) {
    try {
      const bytes = Uint8Array.from(atob(record.data), (char) => char.charCodeAt(0))
      return JSON.parse(new TextDecoder().decode(bytes))
    } catch {
      return value
    }
  }
  return Object.fromEntries(keys.map((key) => [key, decodePayloads(record[key])]))
}
