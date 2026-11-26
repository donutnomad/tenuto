import { describe, expect, it } from "vitest"
import type { ActivityState, Event } from "@/lib/types"
import { buildStepTree } from "@/lib/build-step-tree"

function state(overrides: Partial<ActivityState>): ActivityState {
  return {
    runId: "run-1",
    scheduledEventId: 5,
    activityId: "#a1",
    activityType: "activities.Block",
    taskQueue: "default",
    attempt: 1,
    stamp: 1,
    startedEventId: 0,
    activityReset: false,
    status: "scheduled",
    scheduledTime: "2026-11-12T10:00:00Z",
    ...overrides,
  }
}

const scheduled: Event = {
  id: 5,
  timestamp: "2026-11-12T10:00:00Z",
  activityScheduled: { activityId: "charge-1", activityType: "activities.Fail", taskQueue: "default" },
}

describe("buildStepTree with activity states", () => {
  it("adds open activities whose ActivityScheduled is not in the loaded history pages", () => {
    const later: Event[] = [
      { id: 300, timestamp: "2026-11-12T10:10:00Z", timerScheduled: { timerId: "t-1" } },
    ]
    const steps = buildStepTree(later, "running", [
      state({
        status: "started",
        startedTime: "2026-11-12T10:00:01Z",
        workerIdentity: "worker-1",
        lastHeartbeat: { tick: 7 },
        input: { name: "hb" },
      }),
    ])
    expect(steps).toHaveLength(1)
    expect(steps[0]).toMatchObject({
      seq: 1,
      stepID: "#a1",
      activityType: "activities.Block",
      status: "running",
      startedAt: "2026-11-12T10:00:01Z",
      workerIdentity: "worker-1",
      heartbeat: { tick: 7 },
      input: { name: "hb" },
      currentAttempt: 1,
    })
  })

  it("shows an activity closed with its run as terminated, not pending", () => {
    const steps = buildStepTree([scheduled], "running", [
      state({ activityId: "charge-1", status: "run_closed", startedTime: "2026-11-12T10:00:01Z" }),
    ])
    expect(steps[0]).toMatchObject({ stepID: "charge-1", status: "terminated" })
  })

  it("shows the last heartbeat details of a timed-out activity from ActivityTimedOut", () => {
    const steps = buildStepTree([
      scheduled,
      {
        id: 7,
        timestamp: "2026-11-12T10:05:00Z",
        activityTimedOut: { activityId: "charge-1", scheduledEventId: 5, attempt: 1, timeoutType: "Heartbeat", lastHeartbeatDetails: { offset: 9 } },
      },
    ], "running")
    expect(steps[0]).toMatchObject({ status: "timed_out", heartbeat: { offset: 9 } })
  })

  it("shows the last heartbeat details of an activity interrupted by a workflow reset from ActivityFailed", () => {
    const steps = buildStepTree([
      scheduled,
      {
        id: 7,
        timestamp: "2026-11-12T10:05:00Z",
        activityFailed: {
          activityId: "charge-1", scheduledEventId: 5, startedEventId: 6, attempt: 1, error: "bad deploy", retryState: "NonRetryableFailure",
          failure: { message: "bad deploy", type: "ResetWorkflowError", nonRetryable: true },
          lastHeartbeatDetails: { offset: 9 },
        },
      },
    ], "running")
    expect(steps[0]).toMatchObject({ status: "failed", heartbeat: { offset: 9 } })
  })

  it("keeps an activity whose attempt timed out open while it retries", () => {
    const steps = buildStepTree([
      scheduled,
      { id: 6, timestamp: "2026-11-12T10:00:01Z", activityStarted: { activityId: "charge-1", scheduledEventId: 5, attempt: 1 } },
      { id: 7, timestamp: "2026-11-12T10:01:01Z", activityTimedOut: { activityId: "charge-1", scheduledEventId: 5, attempt: 1, timeoutType: "StartToClose" } },
      { id: 8, timestamp: "2026-11-12T10:01:01Z", activityAttemptFailed: { activityId: "charge-1", scheduledEventId: 5, attempt: 1, error: "activity StartToClose timeout" } },
    ], "running", [
      state({ activityId: "charge-1", attempt: 2, stamp: 2, status: "started", startedTime: "2026-11-12T10:01:02Z" }),
    ])
    expect(steps[0]).toMatchObject({ status: "running", completedAt: null, error: null, currentAttempt: 2 })
    expect(steps[0].attempts[0]).toMatchObject({ attempt: 1, status: "timed_out", errorEventID: 8 })
  })

  it("closes an activity whose last attempt timed out as timed out", () => {
    const steps = buildStepTree([
      scheduled,
      { id: 7, timestamp: "2026-11-12T10:01:01Z", activityTimedOut: { activityId: "charge-1", scheduledEventId: 5, attempt: 3, timeoutType: "StartToClose", retryState: "MaximumAttemptsReached" } },
      { id: 8, timestamp: "2026-11-12T10:01:01Z", activityFailed: { activityId: "charge-1", scheduledEventId: 5, attempt: 3, error: "activity StartToClose timeout", retryState: "MaximumAttemptsReached", failure: { message: "activity StartToClose timeout", timeoutType: "StartToClose" } } },
    ], "running")
    expect(steps[0]).toMatchObject({ status: "timed_out", completedEventID: 8 })
  })

  it("fills attempt, start and worker of an in-flight attempt that has no ActivityStarted", () => {
    const steps = buildStepTree([scheduled], "running", [
      state({ activityId: "charge-1", status: "started", startedTime: "2026-11-12T10:00:02Z", workerIdentity: "worker-1" }),
    ])
    expect(steps[0]).toMatchObject({ status: "running", startedAt: "2026-11-12T10:00:02Z", workerIdentity: "worker-1", currentAttempt: 1 })
  })

  it("shows the next attempt and the last failure of an activity waiting to retry", () => {
    const steps = buildStepTree([scheduled], "running", [
      state({
        activityId: "charge-1",
        attempt: 2,
        status: "scheduled",
        nextAttemptAt: "2026-11-12T10:10:00Z",
        retryLastFailure: "deliberate failure (attempt 1)",
      }),
    ])
    expect(steps[0]).toMatchObject({
      status: "scheduled",
      currentAttempt: 2,
      nextAttemptAt: "2026-11-12T10:10:00Z",
      lastFailure: "deliberate failure (attempt 1)",
    })
  })
})

describe("buildStepTree with child workflows", () => {
  const started: Event = {
    id: 7,
    timestamp: "2026-11-12T10:00:00Z",
    childWorkflowStarted: { childId: "child-1", name: "e2e-quick", childRunId: "run-child" },
  }

  it("measures a completed child from start to completion", () => {
    const completed: Event = { id: 9, timestamp: "2026-11-12T10:00:01.250Z", childWorkflowCompleted: { childRunId: "run-child", output: { ok: true } } }
    const [step] = buildStepTree([started, completed], "completed")
    expect(step).toMatchObject({ status: "completed", output: { ok: true }, completedAt: completed.timestamp, duration: 1250 })
  })

  it("measures a failed child and keeps its error", () => {
    const failed: Event = { id: 9, timestamp: "2026-11-12T10:00:02Z", childWorkflowFailed: { childRunId: "run-child", error: "boom" } }
    const [step] = buildStepTree([started, failed], "running")
    expect(step).toMatchObject({ status: "failed", error: "boom", duration: 2000 })
  })

  it("leaves a running child without a duration", () => {
    const [step] = buildStepTree([started], "running")
    expect(step).toMatchObject({ status: "running", duration: null })
  })
})
