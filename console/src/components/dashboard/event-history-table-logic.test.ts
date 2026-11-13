import { describe, expect, it } from "vitest"
import type { Event } from "@/lib/types"
import {
  buildCausalMaps,
  decodePayloads,
  eventDisplayName,
  eventSearchText,
  eventTypeOf,
  filterEventHistory,
  filterEventsByCategory,
  isActivityAttemptEvent,
  isCausalEventIdField,
  isFailedEvent,
  isWorkflowTaskEvent,
  resetAnchorEvents,
} from "./event-history-table-logic"

const events: Event[] = [
  {
    id: 8,
    timestamp: "2026-07-14T08:00:00Z",
    workflowTaskStarted: {
      scheduledEventId: 7,
      workerIdentity: "worker-east-1",
      workerBuildId: "build-blue",
    },
  },
  {
    id: 12,
    timestamp: "2026-07-14T08:00:01Z",
    activityStarted: {
      activityId: "charge-card",
      scheduledEventId: 10,
      attempt: 2,
      activityType: "ChargeCard",
      taskQueue: "payments",
      workerIdentity: "payment-worker",
      workerBuildId: "worker-build-9",
    },
  },
  {
    id: 13,
    timestamp: "2026-07-14T08:00:02Z",
    activityAttemptFailed: {
      activityId: "charge-card",
      scheduledEventId: 10,
      attempt: 2,
      error: "gateway refused transaction",
      stamp: 42,
    },
  },
  {
    id: 14,
    timestamp: "2026-07-14T08:00:03Z",
    activityTimedOut: {
      activityId: "charge-card",
      scheduledEventId: 10,
      attempt: 2,
      timeoutType: "Heartbeat",
      lastHeartbeatDetails: { checkpoint: "authorized" },
    },
  },
]

describe("event history search", () => {
  it.each([
    ["13", 13],
    ["Activity Task Attempt Failed", 13],
    ["activityAttemptFailed", 13],
    ["ChargeCard", 12],
    ["charge-card", 12],
    ["payment-worker", 12],
    ["worker-build-9", 12],
    ["gateway refused", 13],
    ["42", 13],
    ["authorized", 14],
  ])("finds %s in event %s", (query, eventID) => {
    const result = filterEventHistory(events, { query, scope: "all", failedOnly: false })
    expect(result.map((event) => event.id)).toContain(eventID)
  })

  it("indexes the raw type, display name, scalar keys, and scalar values", () => {
    const text = eventSearchText(events[1])
    expect(text).toContain("activitystarted")
    expect(text).toContain("activity task started")
    expect(text).toContain("workeridentity")
    expect(text).toContain("payment-worker")
  })

  it("formats an unknown camel-case event name", () => {
    expect(eventDisplayName("customEventRecorded")).toBe("Custom Event Recorded")
  })

  it("preserves descending input order so Event ID navigation remains order-independent", () => {
    const descending = [...events].reverse()
    expect(filterEventHistory(descending, { query: "", scope: "all", failedOnly: false }).map((event) => event.id)).toEqual([14, 13, 12, 8])
  })
})

describe("event history quick filters", () => {
  it("shows only failed and timed-out events in Failed only", () => {
    expect(isFailedEvent(events[2])).toBe(true)
    expect(isFailedEvent(events[1])).toBe(false)
    expect(filterEventHistory(events, { query: "", scope: "all", failedOnly: true }).map((event) => event.id)).toEqual([13, 14])
  })

  it("shows the workflow-task lifecycle", () => {
    expect(isWorkflowTaskEvent(events[0])).toBe(true)
    expect(filterEventHistory(events, { query: "", scope: "workflowTasks", failedOnly: false }).map((event) => event.id)).toEqual([8])
  })

  it("uses the attempt attribute for Activity Attempts, including timeouts", () => {
    expect(isActivityAttemptEvent(events[0])).toBe(false)
    expect(filterEventHistory(events, { query: "", scope: "activityAttempts", failedOnly: false }).map((event) => event.id)).toEqual([12, 13, 14])
  })

  it("combines query, scope, and Failed only", () => {
    expect(filterEventHistory(events, {
      query: "gateway",
      scope: "activityAttempts",
      failedOnly: true,
    }).map((event) => event.id)).toEqual([13])
  })
})

describe("causal event fields", () => {
  it.each(["scheduledEventId", "startedEventId", "acceptedEventId", "sourceWorkflowTaskEventId"])(
    "recognizes %s",
    (field) => expect(isCausalEventIdField(field)).toBe(true)
  )

  it("keeps run identifiers separate", () => {
    expect(isCausalEventIdField("nextRunId")).toBe(false)
  })
})

function at(id: number, value: Partial<Event>): Event {
  return { id, timestamp: "2026-07-14T08:00:00Z", ...value }
}

describe("filterEventsByCategory", () => {
  const history: Event[] = [
    at(1, { workflowStarted: { searchAttributes: null } }),
    at(2, { timerScheduled: { timerId: "t1", delayMs: 1000 } }),
    at(3, { timerFired: { timerId: "t1", scheduledEventId: 2 } }),
    at(5, { activityScheduled: { activityId: "#a1" } }),
    at(6, { workflowCancellationRequested: { cause: "operator" } }),
    at(7, { searchAttributesUpserted: { attributes: { tier: "gold" } } }),
  ]

  it("returns the same array when no category is selected", () => {
    expect(filterEventsByCategory(history, [])).toBe(history)
  })

  it("keeps scheduled and fired timer events in the Timer category", () => {
    expect(filterEventsByCategory(history, ["Timer"]).map((event) => event.id)).toEqual([2, 3])
  })

  it("combines categories and covers cancellation requests and search attribute upserts", () => {
    expect(filterEventsByCategory(history, ["Workflow", "Other"]).map((event) => event.id)).toEqual([1, 6, 7])
    expect(filterEventsByCategory(history, ["Activity", "Timer"]).map(eventTypeOf)).toEqual([
      "timerScheduled",
      "timerFired",
      "activityScheduled",
    ])
  })
})

describe("buildCausalMaps", () => {
  const history: Event[] = [
    at(1, { workflowStarted: { searchAttributes: null } }),
    at(2, { workflowTaskScheduled: {} }),
    at(3, { workflowTaskStarted: { scheduledEventId: 2 } }),
    at(4, { workflowTaskTimedOut: { scheduledEventId: 2 } }),
    at(5, { workflowTaskScheduled: {} }),
    at(6, { workflowTaskStarted: { scheduledEventId: 5 } }),
    at(7, { workflowTaskCompleted: { scheduledEventId: 5 } }),
    at(8, { activityScheduled: { activityId: "#a1" } }),
    at(9, { timerScheduled: { timerId: "t1" } }),
    at(10, { workflowUpdateAccepted: { updateId: "u1", name: "approve" } }),
    at(11, { workflowTaskScheduled: {} }),
    at(12, { workflowTaskStarted: { scheduledEventId: 11 } }),
    at(13, { workflowTaskCompleted: { scheduledEventId: 11 } }),
    at(14, { workflowUpdateCompleted: { updateId: "u1", name: "approve" } }),
    at(15, { workflowCompleted: {} }),
  ]

  it("links WFT close events to the started event with the same scheduledEventId", () => {
    expect(buildCausalMaps(history).wftStarted).toEqual({ 4: 3, 7: 6, 13: 12 })
  })

  it("links command events to the preceding workflowTaskCompleted", () => {
    expect(buildCausalMaps(history).sourceWft).toEqual({ 8: 7, 9: 7, 15: 13 })
  })

  it("links update completion to its accepted event", () => {
    expect(buildCausalMaps(history).updateAccepted).toEqual({ 14: 10 })
  })

  it("gives the same result for newest-first input", () => {
    expect(buildCausalMaps([...history].reverse())).toEqual(buildCausalMaps(history))
  })

  it("uses the latest started event when a scheduledEventId repeats and skips unmatched closes", () => {
    const maps = buildCausalMaps([
      at(1, { workflowTaskStarted: { scheduledEventId: 9 } }),
      at(2, { workflowTaskStarted: { scheduledEventId: 9 } }),
      at(3, { workflowTaskCompleted: { scheduledEventId: 9 } }),
      at(4, { workflowTaskCompleted: { scheduledEventId: 77 } }),
      at(5, { markerRecorded: { markerName: "version" } }),
    ])
    expect(maps.wftStarted).toEqual({ 3: 2 })
    expect(maps.sourceWft).toEqual({ 5: 4 })
  })

  it("records 0 for a command event before any workflowTaskCompleted", () => {
    expect(buildCausalMaps([at(1, { activityScheduled: { activityId: "#a1" } })]).sourceWft).toEqual({ 1: 0 })
  })
})

describe("resetAnchorEvents", () => {
  it("keeps only workflow task boundary events as reset points", () => {
    const history = [
      { id: 1, timestamp: "2026-07-14T08:00:00Z", workflowStarted: {} },
      { id: 2, timestamp: "2026-07-14T08:00:00Z", workflowTaskScheduled: {} },
      { id: 3, timestamp: "2026-07-14T08:00:00Z", workflowTaskStarted: {} },
      { id: 4, timestamp: "2026-07-14T08:00:00Z", workflowTaskCompleted: {} },
      { id: 5, timestamp: "2026-07-14T08:00:00Z", activityScheduled: {} },
      { id: 6, timestamp: "2026-07-14T08:00:00Z", workflowTaskTimedOut: {} },
      { id: 7, timestamp: "2026-07-14T08:00:00Z", workflowTaskFailed: {} },
    ] as Event[]

    expect(resetAnchorEvents(history).map((event) => event.id)).toEqual([3, 4, 6, 7])
    expect(resetAnchorEvents([])).toEqual([])
  })
})

describe("decodePayloads", () => {
  const encode = (value: unknown) => ({
    encoding: "json/plain-v1",
    data: Buffer.from(JSON.stringify(value)).toString("base64"),
  })

  it("replaces json/plain-v1 payloads with their JSON values at any depth", () => {
    expect(decodePayloads({ success: encode({ count: 101, ok: true }) })).toEqual({ success: { count: 101, ok: true } })
    expect(decodePayloads([encode("héllo"), encode(42)])).toEqual(["héllo", 42])
    expect(decodePayloads({ failure: { message: "boom", details: [encode({ card: "card-1" })] } })).toEqual({
      failure: { message: "boom", details: [{ card: "card-1" }] },
    })
  })

  it("keeps other encodings, undecodable data and ordinary objects unchanged", () => {
    const binary = { encoding: "binary/plain", data: "AAE=" }
    const broken = { encoding: "json/plain-v1", data: "not base64 json" }
    const ordinary = { encoding: "json/plain-v1", data: "eyJ9", extra: 1 }
    expect(decodePayloads(binary)).toEqual(binary)
    expect(decodePayloads(broken)).toEqual(broken)
    expect(decodePayloads(ordinary)).toEqual(ordinary)
  })
})
