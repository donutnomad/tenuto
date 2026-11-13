import { describe, expect, it } from "vitest"
import type { Run } from "@/lib/types"
import { emptyTraceNavigation } from "@/lib/trace-session"
import type { Trace, TraceEffect, TraceStep } from "@/lib/wasm-loader"
import {
  boundariesAt,
  buildBoundary,
  rootSegmentsAt,
  snapshotForStep,
  workflowPath,
  type WorkflowBoundary,
} from "./trace-boundaries"

function step(overrides: Partial<TraceStep>): TraceStep {
  return {
    seq: 0,
    lane_id: "main",
    lane_seq: 0,
    line: 0,
    path: "",
    vars: {},
    frames: [],
    ...overrides,
  }
}

function trace(steps: TraceStep[]): Trace {
  return { pseudocode: "", lanes: [], steps, activities: [], run_kind: "completed", total_steps: steps.length }
}

function childEffect(overrides: Partial<TraceEffect>): TraceEffect {
  return {
    kind: "child-workflow",
    phase: "scheduled",
    invocation_id: "child-1",
    activity_id: "#c1",
    status: "completed",
    ...overrides,
  }
}

function boundary(overrides: Partial<WorkflowBoundary>): WorkflowBoundary {
  return {
    parentRunID: "root-run",
    parentWorkflowID: "order",
    childRunID: "child-run",
    childWorkflowID: "payment",
    effect: childEffect({ child_run_id: "child-run" }),
    entryNavigation: { ...emptyTraceNavigation(), line: 4 },
    resumeNavigation: emptyTraceNavigation(),
    segments: ["child-run"],
    ...overrides,
  }
}

describe("snapshotForStep", () => {
  const steps = [
    step({ seq: 0, lane_id: "main", lane_seq: 0, line: 1 }),
    step({ seq: 1, lane_id: "routine-1", lane_seq: 0, line: 5 }),
    step({ seq: 2, lane_id: "main", lane_seq: 1, line: 2, frames: [{ name: "charge", line: 9, path: "charge", vars: {} }] }),
  ]

  it("stops on the step's lane cursor and top frame", () => {
    expect(snapshotForStep(trace(steps), steps[2])).toEqual({
      focusLane: "main",
      cursorByLane: [["main", 1]],
      selectedFrameIndex: 0,
      breakpoints: [],
      stepSeq: 2,
      line: 9,
      frameName: "charge",
    })
  })

  it("keeps the other lanes' cursors and the breakpoints of the base snapshot", () => {
    const base = { ...emptyTraceNavigation(), cursorByLane: [["main", 1], ["routine-1", 3]] as Array<[string, number]>, breakpoints: [7] }
    const snapshot = snapshotForStep(trace(steps), steps[1], base)
    expect(snapshot.cursorByLane).toEqual([["main", 1], ["routine-1", 0]])
    expect(snapshot.breakpoints).toEqual([7])
    expect(snapshot.line).toBe(5)
    expect(snapshot.frameName).toBe("routine-1")
  })
})

describe("buildBoundary", () => {
  const parentRun = { id: "root-run", workflow_id: "order" } as Run
  const steps = [
    step({ seq: 0, line: 1 }),
    step({
      seq: 1,
      lane_seq: 1,
      line: 4,
      effects: [childEffect({ child_run_id: "child-run", child_workflow_id: "payment", resume_seq: 2 })],
    }),
    step({ seq: 2, lane_seq: 2, line: 6 }),
  ]

  it("enters at the scheduling step and resumes at the effect's resume step", () => {
    const built = buildBoundary({ run: parentRun, trace: trace(steps) }, "child-run")
    expect(built.parentRunID).toBe("root-run")
    expect(built.parentWorkflowID).toBe("order")
    expect(built.childWorkflowID).toBe("payment")
    expect(built.segments).toEqual(["child-run"])
    expect(built.entryNavigation).toEqual(expect.objectContaining({ stepSeq: 1, line: 4 }))
    expect(built.resumeNavigation).toEqual(expect.objectContaining({ stepSeq: 2, line: 6 }))
  })

  it("resumes at the scheduling step when the effect has no resume position in the trace", () => {
    const pending = [step({ seq: 0, line: 4, effects: [childEffect({ child_run_id: "child-run" })] })]
    const built = buildBoundary({ run: parentRun, trace: trace(pending) }, "child-run")
    expect(built.childWorkflowID).toBe("child-run")
    expect(built.resumeNavigation).toEqual(built.entryNavigation)
  })

  it("rejects a child the parent trace never scheduled", () => {
    expect(() => buildBoundary({ run: parentRun, trace: trace(steps) }, "other-run"))
      .toThrow("child other-run has no parent Trace effect")
  })
})

describe("workflowPath", () => {
  it("lists the root run segments when no child workflow is entered", () => {
    expect(workflowPath("order", [], ["root-run", "root-run-2"])).toEqual([
      { runID: "root-run", workflowID: "order", line: 0 },
      { runID: "root-run-2", workflowID: "order", line: 0 },
    ])
  })

  it("lists each caller at its call line and the deepest child's active segment", () => {
    const boundaries = [
      boundary({}),
      boundary({
        parentRunID: "child-run",
        parentWorkflowID: "payment",
        childRunID: "grandchild-run",
        childWorkflowID: "ledger",
        entryNavigation: { ...emptyTraceNavigation(), line: 11 },
        segments: ["grandchild-run", "grandchild-run-2"],
      }),
    ]
    expect(workflowPath("order", boundaries, ["root-run"])).toEqual([
      { runID: "root-run", workflowID: "order", line: 4 },
      { runID: "child-run", workflowID: "payment", line: 11 },
      { runID: "grandchild-run-2", workflowID: "ledger", line: 0 },
    ])
  })
})

describe("restoring the workflow stack from the URL", () => {
  it("truncates the root segment chain at the restored run", () => {
    expect(rootSegmentsAt(["root-run", "root-run-2", "root-run-3"], "root-run-2", "root-run"))
      .toEqual(["root-run", "root-run-2"])
    expect(rootSegmentsAt(["other"], "root-run", "root-run")).toEqual(["root-run"])
    expect(rootSegmentsAt(["root-run", "root-run-2"], "child-run", "root-run")).toEqual(["root-run", "root-run-2"])
  })

  it("keeps the stack down to the boundary that contains the restored run", () => {
    const outer = boundary({ segments: ["child-run", "child-run-2"] })
    const inner = boundary({ parentRunID: "child-run-2", childRunID: "grandchild-run", segments: ["grandchild-run"] })

    expect(boundariesAt([outer, inner], "child-run", "root-run")).toEqual([{ ...outer, segments: ["child-run"] }])
    expect(boundariesAt([outer, inner], "grandchild-run", "root-run")).toEqual([outer, inner])
  })

  it("clears the stack at the root run and for a run it does not contain", () => {
    expect(boundariesAt([boundary({})], "root-run", "root-run")).toEqual([])
    expect(boundariesAt([boundary({})], "unknown-run", "root-run")).toEqual([])
  })
})
