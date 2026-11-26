import { describe, expect, it } from "vitest"
import {
  activityAttemptTraceTarget,
  activityInvocationCursorPhase,
  activityInvocationSummary,
  activityInvocationVirtualWindow,
  buildActivityInvocationIndex,
  buildActivityInvocationsByLine,
  buildActivityInvocationTraceIndex,
  nextExceptionalActivityInvocation,
  selectActivityInvocations,
  type ActivityInvocationRecord,
} from "./activity-invocations"
import type { Event } from "./types"
import type { TraceStep } from "./wasm-loader"

describe("buildActivityInvocationIndex", () => {
  it("filters terminal failures without hiding other filters behind UI state", () => {
    const invocations: ActivityInvocationRecord[] = [
      { scheduledEventID: 10, status: "completed" },
      { scheduledEventID: 20, status: "failed" },
      { scheduledEventID: 30, status: "timed_out" },
    ].map((item) => ({
      invocationID: `activity:${item.scheduledEventID}`,
      stepID: "#a1",
      activityType: "Charge",
      attempts: [],
      resetEpochs: [],
      ...item,
      status: item.status as ActivityInvocationRecord["status"],
    }))

    expect(selectActivityInvocations(invocations, new Map(), -1, "", "failed", "sequence")
      .map((invocation) => invocation.scheduledEventID)).toEqual([20])
  })

  it("sorts by wall-clock duration and keeps running calls last", () => {
    const invocation = (scheduledEventID: number, startedAt?: string, completedAt?: string) => ({
      invocationID: `activity:${scheduledEventID}`,
      stepID: "#a1",
      scheduledEventID,
      activityType: "Charge",
      status: completedAt ? "completed" as const : "running" as const,
      attempts: [{
        resetEpoch: 0,
        attempt: 1,
        status: completedAt ? "completed" as const : "started" as const,
        startedAt,
        completedAt,
        }],
      resetEpochs: [],
    })
    const invocations = [
      invocation(10, "2026-07-13T00:00:00Z", "2026-07-13T00:00:02Z"),
      invocation(20, "2026-07-13T00:00:00Z", "2026-07-13T00:00:05Z"),
      invocation(30, "2026-07-13T00:00:00Z"),
    ]

    expect(selectActivityInvocations(invocations, new Map(), -1, "", "all", "duration-desc")
      .map((item) => item.scheduledEventID)).toEqual([20, 10, 30])
    expect(selectActivityInvocations(invocations, new Map(), -1, "", "all", "duration-asc")
      .map((item) => item.scheduledEventID)).toEqual([10, 20, 30])
  })

  it("jumps to the next exceptional call and wraps around", () => {
    const invocation = (scheduledEventID: number, status: "completed" | "failed" | "timed_out", attemptFailed = false) => ({
      invocationID: `activity:${scheduledEventID}`,
      stepID: "#a1",
      scheduledEventID,
      activityType: "Charge",
      status,
      attempts: attemptFailed ? [{ resetEpoch: 0, attempt: 1, status: "failed" as const, }] : [],
      resetEpochs: [],
    })
    const invocations = [
      invocation(10, "completed"),
      invocation(20, "completed", true),
      invocation(30, "timed_out"),
      invocation(40, "failed"),
    ]

    expect(nextExceptionalActivityInvocation(invocations, 10)?.scheduledEventID).toBe(20)
    expect(nextExceptionalActivityInvocation(invocations, 30)?.scheduledEventID).toBe(40)
    expect(nextExceptionalActivityInvocation(invocations, 40)?.scheduledEventID).toBe(20)
  })

  it("limits a 500-call selector to the visible rows plus overscan", () => {
    expect(activityInvocationVirtualWindow(500, 6000, 28, 224, 3)).toEqual({
      start: 211,
      end: 226,
      offsetTop: 5908,
      totalHeight: 14000,
    })
  })

  it("combines horizon filtering with business-input search", () => {
    const invocation = (scheduledEventID: number, wallet: string) => ({
      invocationID: `activity:${scheduledEventID}`,
      stepID: "#a1",
      scheduledEventID,
      activityType: "CheckBalance",
      status: "completed" as const,
      input: { wallet_address: wallet },
      attempts: [],
      resetEpochs: [],
    })
    const anchors = new Map([
      [10, { scheduledEventID: 10, laneID: "main", line: 8, stepSeq: 4, laneSeq: 4, iteration: 1 }],
      [20, { scheduledEventID: 20, laneID: "routine-2", line: 8, stepSeq: 12, laneSeq: 2, iteration: 2 }],
    ])

    expect(selectActivityInvocations(
      [invocation(10, "0xAAA"), invocation(20, "0xBBB")],
      anchors,
      6,
      "0xbbb",
      "future",
      "sequence",
    ).map((item) => item.scheduledEventID)).toEqual([20])
  })

  it("groups retries and reset epochs under one scheduled invocation", () => {
    const events = [
      { id: 10, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#a1", activityType: "Charge", taskQueue: "payments", input: { amount: 10 } } },
      { id: 11, timestamp: "2026-07-13T00:00:01Z", activityStarted: { activityId: "#a1", scheduledEventId: 10, attempt: 1, stamp: 1, activityType: "Charge", taskQueue: "payments", workerIdentity: "worker-a", workerBuildId: "v1" } },
      { id: 12, timestamp: "2026-07-13T00:00:02Z", activityAttemptFailed: { activityId: "#a1", scheduledEventId: 10, attempt: 1, stamp: 1, error: "declined" } },
      { id: 13, timestamp: "2026-07-13T00:00:03Z", activityStarted: { activityId: "#a1", scheduledEventId: 10, attempt: 2, stamp: 1, activityType: "Charge", taskQueue: "payments", workerIdentity: "worker-b", workerBuildId: "v1" } },
      { id: 14, timestamp: "2026-07-13T00:00:04Z", activityReset: { activityId: "#a1", scheduledEventId: 10, attempt: 2, reason: "operator retry" } },
      { id: 15, timestamp: "2026-07-13T00:00:05Z", activityStarted: { activityId: "#a1", scheduledEventId: 10, attempt: 1, stamp: 2, activityType: "Charge", taskQueue: "payments", workerIdentity: "worker-c", workerBuildId: "v2" } },
      { id: 16, timestamp: "2026-07-13T00:00:06Z", activityCompleted: { activityId: "#a1", scheduledEventId: 10, attempt: 1, activityType: "Charge", taskQueue: "payments", output: { ok: true } } },
    ] as Event[]

    const invocation = buildActivityInvocationIndex(events).get(10)
    expect(invocation?.status).toBe("completed")
    expect(invocation?.attempts).toHaveLength(3)
    expect(invocation?.attempts.map((attempt) => [attempt.resetEpoch, attempt.attempt, attempt.status])).toEqual([
      [0, 1, "failed"],
      [0, 2, "started"],
      [1, 1, "completed"],
    ])
    expect(invocation?.resetEpochs).toEqual([
      expect.objectContaining({ resetEpoch: 1, resetEventID: 14, reason: "operator retry" }),
    ])
  })

  it("marks outstanding Activity calls cancelled when the Workflow is cancelled", () => {
    const events = [
      { id: 1, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#a1", activityType: "Charge", taskQueue: "payments", input: {} } },
      { id: 2, timestamp: "2026-07-13T00:00:01Z", activityStarted: { activityId: "#a1", scheduledEventId: 1, attempt: 1, activityType: "Charge", taskQueue: "payments", workerIdentity: "worker-a", workerBuildId: "v1" } },
      { id: 3, timestamp: "2026-07-13T00:00:02Z", workflowCancelled: {} },
    ] as Event[]

    expect(buildActivityInvocationIndex(events).get(1)?.status).toBe("cancelled")
  })

  it("keeps scheduled and running frontiers distinct from a terminal timeout", () => {
    const events = [
      { id: 10, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#waiting", activityType: "Wait", taskQueue: "q", input: {} } },
      { id: 20, timestamp: "2026-07-13T00:00:01Z", activityScheduled: { activityId: "#running", activityType: "Run", taskQueue: "q", input: {} } },
      { id: 21, timestamp: "2026-07-13T00:00:02Z", activityStarted: { activityId: "#running", scheduledEventId: 20, attempt: 1, activityType: "Run", taskQueue: "q", workerIdentity: "worker-a", workerBuildId: "v1" } },
      { id: 30, timestamp: "2026-07-13T00:00:03Z", activityScheduled: { activityId: "#timeout", activityType: "Slow", taskQueue: "q", input: {} } },
      { id: 31, timestamp: "2026-07-13T00:00:04Z", activityStarted: { activityId: "#timeout", scheduledEventId: 30, attempt: 3, activityType: "Slow", taskQueue: "q", workerIdentity: "worker-b", workerBuildId: "v1" } },
      { id: 32, timestamp: "2026-07-13T00:00:05Z", activityTimedOut: { activityId: "#timeout", scheduledEventId: 30, attempt: 3, activityType: "Slow", timeoutType: "StartToClose", retryState: "MaximumAttemptsReached", lastHeartbeatDetails: { offset: 42 } } },
    ] as Event[]
    const index = buildActivityInvocationIndex(events)

    expect(index.get(10)?.status).toBe("scheduled")
    expect(index.get(20)?.status).toBe("running")
    expect(index.get(30)).toEqual(expect.objectContaining({ status: "timed_out", error: "StartToClose" }))
    expect(index.get(30)?.attempts[0]).toEqual(expect.objectContaining({
      status: "timed_out",
      errorEventID: 32,
      retryState: "MaximumAttemptsReached",
      lastHeartbeatDetails: { offset: 42 },
    }))
  })

  it("keeps the last heartbeat details on an attempt failed by a workflow reset", () => {
    const events = [
      { id: 10, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#charge", activityType: "Charge", taskQueue: "q", input: {} } },
      { id: 11, timestamp: "2026-07-13T00:00:01Z", activityStarted: { activityId: "#charge", scheduledEventId: 10, attempt: 1, activityType: "Charge", taskQueue: "q", workerIdentity: "worker-a", workerBuildId: "v1" } },
      { id: 13, timestamp: "2026-07-13T00:00:05Z", activityFailed: { activityId: "#charge", scheduledEventId: 10, startedEventId: 11, attempt: 1, activityType: "Charge", error: "bad deploy", retryState: "NonRetryableFailure", failure: { message: "bad deploy", type: "ResetWorkflowError", nonRetryable: true }, lastHeartbeatDetails: { offset: 42 } } },
    ] as Event[]
    const index = buildActivityInvocationIndex(events)

    expect(index.get(10)).toEqual(expect.objectContaining({ status: "failed", error: "bad deploy" }))
    expect(index.get(10)?.attempts[0]).toEqual(expect.objectContaining({
      status: "failed",
      errorEventID: 13,
      retryState: "NonRetryableFailure",
      lastHeartbeatDetails: { offset: 42 },
    }))
  })

  it("keeps loop invocations on the same source line independently selectable", () => {
    const events = [
      { id: 20, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "/0#a1", activityType: "CheckBalance", taskQueue: "wallet" } },
      { id: 21, timestamp: "2026-07-13T00:00:01Z", activityCompleted: { activityId: "/0#a1", scheduledEventId: 20, attempt: 1, stamp: 1, activityType: "CheckBalance", taskQueue: "wallet" } },
      { id: 30, timestamp: "2026-07-13T00:00:02Z", activityScheduled: { activityId: "/1#a1", activityType: "CheckBalance", taskQueue: "wallet" } },
      { id: 31, timestamp: "2026-07-13T00:00:03Z", activityCompleted: { activityId: "/1#a1", scheduledEventId: 30, attempt: 1, stamp: 1, activityType: "CheckBalance", taskQueue: "wallet" } },
    ] as Event[]
    const effect = (scheduledEventID: number): TraceStep => ({
      seq: scheduledEventID,
      lane_id: "main",
      lane_seq: scheduledEventID,
      line: 8,
      path: `/program/${scheduledEventID}`,
      vars: {},
      frames: [],
      effects: [{
        kind: "activity",
        phase: "scheduled",
        invocation_id: `activity:${scheduledEventID}`,
        activity_id: "#a1",
        status: "completed",
        scheduled_event_id: scheduledEventID,
      }],
    })
    const activityIndex = buildActivityInvocationIndex(events)
    const byLine = buildActivityInvocationsByLine([
      effect(20),
      effect(20),
      effect(30),
    ], activityIndex)

    expect(byLine.get(8)?.map((invocation) => invocation.scheduledEventID)).toEqual([20, 30])
  })

  it("keeps different Activity types on one line and excludes Child Workflow effects", () => {
    const events = [
      { id: 10, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#charge", activityType: "Charge", taskQueue: "q", input: {} } },
      { id: 20, timestamp: "2026-07-13T00:00:01Z", activityScheduled: { activityId: "#notify", activityType: "Notify", taskQueue: "q", input: {} } },
    ] as Event[]
    const step: TraceStep = {
      seq: 4,
      lane_id: "main",
      lane_seq: 4,
      line: 12,
      path: "/program/4",
      vars: {},
      frames: [],
      effects: [
        { kind: "activity", phase: "scheduled", invocation_id: "activity:10", activity_id: "#charge", status: "scheduled", scheduled_event_id: 10 },
        { kind: "child-workflow", phase: "scheduled", invocation_id: "child:#child", activity_id: "#child", status: "running", scheduled_event_id: 15 },
        { kind: "activity", phase: "scheduled", invocation_id: "activity:20", activity_id: "#notify", status: "scheduled", scheduled_event_id: 20 },
      ],
    }

    expect(buildActivityInvocationsByLine([step], buildActivityInvocationIndex(events)).get(12)
      ?.map((invocation) => invocation.activityType)).toEqual(["Charge", "Notify"])
  })

  it("uses a stable business input as the call selector summary", () => {
    const invocation = buildActivityInvocationIndex([
      { id: 40, timestamp: "2026-07-13T00:00:00Z", activityScheduled: { activityId: "#a1", activityType: "CheckBalance", taskQueue: "wallet", input: { token: "ETH", wallet_address: "0xAAA111" } } },
    ] as Event[]).get(40)

    expect(invocation && activityInvocationSummary(invocation)).toBe("wallet_address=0xAAA111")
  })

  it("maps loop calls to their lane, iteration and cursor phase", () => {
    const effect = (eventID: number, seq: number, laneID: string): TraceStep => ({
      seq,
      lane_id: laneID,
      lane_seq: seq,
      line: 14,
      path: `/program/${seq}`,
      vars: {},
      frames: [],
      effects: [{
        kind: "activity",
        phase: "scheduled",
        invocation_id: `activity:${eventID}`,
        activity_id: `/${eventID}#a1`,
        status: "completed",
        scheduled_event_id: eventID,
        call_seq: seq,
        resume_seq: seq + 2,
      }],
    })
    const anchors = buildActivityInvocationTraceIndex([
      effect(6, 8, "routine-1"),
      effect(7, 12, "routine-2"),
      effect(8, 16, "routine-3"),
    ])

    expect(anchors.get(7)).toEqual(expect.objectContaining({
      laneID: "routine-2",
      iteration: 2,
      stepSeq: 12,
      resumeSeq: 14,
    }))
    expect(activityInvocationCursorPhase(anchors.get(6), 12)).toBe("past")
    expect(activityInvocationCursorPhase(anchors.get(7), 12)).toBe("current")
    expect(activityInvocationCursorPhase(anchors.get(8), 12)).toBe("future")
  })

  it("maps attempt start, retry failure and terminal outcome to trace positions", () => {
    const anchor = {
      scheduledEventID: 10,
      laneID: "routine-2",
      line: 14,
      stepSeq: 12,
      laneSeq: 4,
      iteration: 2,
      waitSeq: 15,
      resumeSeq: 18,
    }
    const retryAttempt = {
      resetEpoch: 0,
      attempt: 1,
      status: "failed" as const,
      startedEventID: 11,
      errorEventID: 12,
    }
    const completedAttempt = {
      resetEpoch: 0,
      attempt: 2,
      status: "completed" as const,
      startedEventID: 13,
      completedEventID: 14,
    }

    expect(activityAttemptTraceTarget(retryAttempt, anchor, "started")).toEqual({
      eventID: 11,
      laneID: "routine-2",
      seq: 15,
      phase: "started",
    })
    expect(activityAttemptTraceTarget(retryAttempt, anchor)).toEqual({
      eventID: 12,
      laneID: "routine-2",
      seq: 15,
      phase: "outcome",
    })
    expect(activityAttemptTraceTarget(completedAttempt, anchor)).toEqual({
      eventID: 14,
      laneID: "routine-2",
      seq: 18,
      phase: "outcome",
    })
  })

  it("keeps a running attempt on its start event", () => {
    const target = activityAttemptTraceTarget({
      resetEpoch: 0,
      attempt: 1,
      status: "started",
      startedEventID: 21,
    }, {
      scheduledEventID: 20,
      laneID: "main",
      line: 8,
      stepSeq: 6,
      laneSeq: 6,
      iteration: 1,
    })

    expect(target).toEqual({ eventID: 21, laneID: "main", seq: 6, phase: "started" })
  })
})
