import { describe, expect, it } from "vitest"
import {
  buildRunRelationships,
  relationshipExpansionGuard,
} from "./run-relationships"
import type { Event, Run } from "./types"

function run(overrides: Partial<Run> = {}): Run {
  return {
    id: "root",
    namespace: "demo",
    workflow_id: "root-workflow",
    workflow_config_version: "v1",
    status: "running",
    first_run_id: "root",
    created_at: "2026-07-14T00:00:00Z",
    ...overrides,
  }
}

function event(id: number, value: Partial<Event>): Event {
  return { id, timestamp: `2026-07-14T00:00:${String(id).padStart(2, "0")}Z`, ...value }
}

describe("buildRunRelationships", () => {
  it("builds a parent and three unique direct children", () => {
    const relationships = buildRunRelationships(run({ parent_run_id: "parent" }), [
      event(1, { childWorkflowStarted: { name: "child-a-flow", childId: "child-a", childRunId: "run-a" } }),
      event(2, { childWorkflowStarted: { name: "child-b-flow", childId: "child-b", childRunId: "run-b" } }),
      event(3, { childWorkflowStarted: { name: "child-c-flow", childId: "child-c", childRunId: "run-c" } }),
      event(4, { childWorkflowStarted: { name: "child-a-flow", childId: "child-a", childRunId: "run-a" } }),
    ])

    expect(relationships.filter((item) => item.kind === "parent").map((item) => item.runID)).toEqual(["parent"])
    expect(relationships.filter((item) => item.kind === "child").map((item) => item.runID)).toEqual([
      "run-a",
      "run-b",
      "run-c",
    ])
  })

  it("derives a child Continue-As-New successor from history", () => {
    const relationships = buildRunRelationships(run({
      id: "child-segment-1",
      workflow_id: "child-flow",
      status: "continued_as_new",
      continued_run_id: "",
    }), [
      event(10, {
        workflowContinuedAsNew: {
          name: "child-flow",
          nextRunId: "child-segment-2",
        },
      }),
    ])

    expect(relationships).toContainEqual(expect.objectContaining({
      kind: "continue-as-new",
      runID: "child-segment-2",
      workflowType: "child-flow",
    }))
  })

  it("uses the final Continue-As-New event and ignores the predecessor pointer as a forward edge", () => {
    const relationships = buildRunRelationships(run({ continued_run_id: "previous-segment" }), [
      event(10, { workflowContinuedAsNew: { name: "flow", nextRunId: "stale-next" } }),
      event(11, { workflowContinuedAsNew: { name: "flow", nextRunId: "actual-next" } }),
    ])

    expect(relationships.filter((item) => item.kind === "continue-as-new").map((item) => item.runID)).toEqual([
      "actual-next",
    ])
    expect(relationships.some((item) => item.runID === "previous-segment")).toBe(false)
  })

  it("builds reset origin and successor edges", () => {
    const relationships = buildRunRelationships(run({ id: "reset-2", reset_run_id: "reset-3" }), [
      event(20, {
        workflowTaskFailed: {
          scheduledEventId: 0,
          startedEventId: 0,
          cause: "reset_workflow",
          baseRunId: "reset-1",
          resetRunId: "reset-2",
          reason: "operator retry",
        },
      }),
    ])

    expect(relationships.filter((item) => item.kind.startsWith("reset"))).toEqual([
      expect.objectContaining({ kind: "reset-from", runID: "reset-1" }),
      expect.objectContaining({ kind: "reset-to", runID: "reset-3" }),
    ])
  })

  it("returns no edges for a missing run", () => {
    expect(buildRunRelationships(undefined, [event(1, {
      workflowContinuedAsNew: { name: "flow", nextRunId: "run-2" },
    })])).toEqual([])
  })
})

describe("relationshipExpansionGuard", () => {
  it("stops cycles before loading a repeated run", () => {
    expect(relationshipExpansionGuard("root", ["root", "child"], 2, 8)).toEqual({
      allowed: false,
      reason: "cycle",
    })
  })

  it("limits deeply nested child and segment chains", () => {
    expect(relationshipExpansionGuard("level-9", ["root"], 8, 8)).toEqual({
      allowed: false,
      reason: "max-depth",
    })
    expect(relationshipExpansionGuard("level-7", ["root"], 7, 8)).toEqual({ allowed: true })
  })
})
