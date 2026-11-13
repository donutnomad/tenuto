import { describe, expect, it } from "vitest"
import type { Event, Step } from "@/lib/types"
import {
  isTerminalRunStatus,
  lastFailedAttempt,
  pendingActivitySteps,
  runDurationMs,
  runEndTime,
  runTaskQueue,
  runConfigErrorBanner,
  runFailureSummary,
  runUpgradeErrorBanner,
  RUN_DETAIL_EVENT_CATEGORIES,
} from "@/lib/run-detail"

function step(overrides: Partial<Step>): Step {
  return {
    seq: 1,
    stepID: "#a1",
    activityType: "activities.CheckBalance",
    status: "running",
    taskQueue: "default",
    input: null,
    output: null,
    error: null,
    scheduledAt: "2026-07-14T00:00:00Z",
    startedAt: null,
    completedAt: null,
    duration: null,
    workerIdentity: "",
    workerBuildID: "",
    attempts: [],
    heartbeat: null,
    childRunID: null,
    scheduledEventID: 1,
    ...overrides,
  }
}

describe("run detail semantics", () => {
  it("treats continued-as-new as a closed run", () => {
    expect(isTerminalRunStatus("continued_as_new")).toBe(true)
    expect(isTerminalRunStatus("running")).toBe(false)
  })

  it("keeps pending child workflows out of Activity reset", () => {
    const activity = step({})
    const child = step({
      seq: 2,
      stepID: "#c1",
      activityType: "ChildWorkflow:payment",
      childRunID: "child-run-1",
    })
    const completed = step({ seq: 3, status: "completed" })

    expect(pendingActivitySteps([activity, child, completed])).toEqual([activity])
  })

  it("categorizes reset, update, and continue-as-new events", () => {
    expect(RUN_DETAIL_EVENT_CATEGORIES.Activity).toContain("activityReset")
    expect(RUN_DETAIL_EVENT_CATEGORIES.Update).toEqual([
      "workflowUpdateAccepted",
      "workflowUpdateCompleted",
    ])
    expect(RUN_DETAIL_EVENT_CATEGORIES.Workflow).toContain("workflowContinuedAsNew")
    expect(RUN_DETAIL_EVENT_CATEGORIES.Timer).toEqual(["timerScheduled", "timerFired", "timerCancelled"])
    expect(RUN_DETAIL_EVENT_CATEGORIES.Activity).toContain("activityCancelled")
    expect(RUN_DETAIL_EVENT_CATEGORIES["Child Workflow"]).toContain("childWorkflowCancelRequested")
  })

  it("surfaces config_error pauses and hides them when absent", () => {
    expect(runConfigErrorBanner({ status: "running", config_error: undefined })).toBeNull()
    const paused = runConfigErrorBanner({
      status: "running",
      config_error: "activity \"payments.settle\" contract mismatch",
    })
    expect(paused?.detail).toBe("activity \"payments.settle\" contract mismatch")
    expect(paused?.description).toContain("paused until the conflict is fixed")
    expect(
      runConfigErrorBanner({ status: "failed", config_error: "conflict" })?.description,
    ).toContain("run is closed")
  })

  it("only claims AutoUpgrade retries while the run is still alive", () => {
    expect(runUpgradeErrorBanner({ status: "running", upgrade_error: undefined })).toBeNull()
    const retrying = runUpgradeErrorBanner({ status: "running", upgrade_error: "replay failed" })
    expect(retrying?.title).toContain("retrying")
    expect(retrying?.description).toContain("retries with backoff")

    const closed = runUpgradeErrorBanner({ status: "failed", upgrade_error: "replay failed" })
    expect(closed?.title).toBe("Last AutoUpgrade attempt failed.")
    expect(closed?.description).not.toContain("backoff")
  })
})

describe("run detail metadata", () => {
  const events: Event[] = [
    { id: 1, timestamp: "2026-07-14T00:00:00Z", workflowStarted: {} } as Event,
    { id: 2, timestamp: "2026-07-14T00:00:01Z", activityScheduled: { taskQueue: "payments" } } as Event,
    { id: 3, timestamp: "2026-07-14T00:00:02Z", activityScheduled: { taskQueue: "ledger" } } as Event,
    { id: 4, timestamp: "2026-07-14T00:00:05Z", workflowCompleted: {} } as Event,
  ]

  it("uses the last event as the end time and the first scheduled activity as the task queue", () => {
    expect(runEndTime(events)).toBe("2026-07-14T00:00:05Z")
    expect(runTaskQueue(events)).toBe("payments")
  })

  it("reports no end time or task queue before any matching event is loaded", () => {
    expect(runEndTime([])).toBeNull()
    expect(runTaskQueue([])).toBeNull()
    expect(runTaskQueue(events.slice(0, 1))).toBeNull()
  })

  it("measures a running run against the clock and a closed run against its end time", () => {
    const createdAt = "2026-07-14T00:00:00Z"
    const startMs = new Date(createdAt).getTime()
    expect(runDurationMs({ status: "running", created_at: createdAt }, null, startMs + 3_000)).toBe(3_000)
    // 时钟首次 effect 之前为 0：此时不显示时长。
    expect(runDurationMs({ status: "running", created_at: createdAt }, "2026-07-14T00:00:05Z", 0)).toBeNull()
    expect(runDurationMs({ status: "completed", created_at: createdAt }, "2026-07-14T00:00:05Z", startMs + 60_000)).toBe(5_000)
    expect(runDurationMs({ status: "completed", created_at: createdAt }, null, startMs + 60_000)).toBeNull()
  })

  it("picks the most recent failed or timed-out attempt of a pending step", () => {
    expect(lastFailedAttempt(step({ attempts: [] }))).toBeNull()
    expect(lastFailedAttempt(step({ attempts: [{ attempt: 1, status: "started" }] }))).toBeNull()
    expect(lastFailedAttempt(step({
      attempts: [
        { attempt: 1, status: "failed", error: "boom" },
        { attempt: 2, status: "timed_out", error: "deadline" },
        { attempt: 3, status: "started" },
      ],
    }))).toEqual({ attempt: 2, status: "timed_out", error: "deadline" })
  })
})

describe("runFailureSummary", () => {
  it("returns the terminal failure with its cause chain for a failed run", () => {
    const summary = runFailureSummary(
      {
        status: "failed",
        terminal_failure: {
          message: "activity StartToClose timeout",
          timeoutType: "StartToClose",
          cause: { message: "inner", type: "Io" },
        },
      },
      [],
    )
    expect(summary).toEqual({
      failure: expect.objectContaining({ message: "activity StartToClose timeout" }),
      timeoutType: "StartToClose",
      causes: [{ message: "inner", type: "Io" }],
    })
  })

  it("takes the workflow timeout type from the WorkflowFailed event", () => {
    const events: Event[] = [
      { id: 6, timestamp: "2026-11-12T10:00:04Z", workflowFailed: { error: "workflow run timeout", timeoutType: "Run" } },
    ]
    expect(runFailureSummary({ status: "failed", terminal_failure: { message: "workflow run timeout" } }, events)?.timeoutType).toBe("Run")
  })

  it("is empty for runs that did not fail", () => {
    expect(runFailureSummary({ status: "completed" }, [])).toBeNull()
    expect(runFailureSummary({ status: "terminated", terminal_failure: { message: "x" } }, [])).toBeNull()
  })
})
