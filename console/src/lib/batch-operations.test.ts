import { describe, expect, it } from "vitest"
import {
  batchAccepted,
  batchFilter,
  batchProgress,
  batchRequest,
  batchStatusStyle,
  batchSummary,
  isActiveBatch,
  isEmptyBatchFilter,
  selectedRunsBatchRequest,
  succeededLabel,
} from "./batch-operations"

describe("selectedRunsBatchRequest", () => {
  it("includes terminal runs for an explicit run id list", () => {
    expect(selectedRunsBatchRequest("ns", "cancel", ["r1", "r2"], "", "last_workflow_task")).toEqual({
      namespace: "ns",
      type: "cancel",
      filter: { run_ids: ["r1", "r2"], include_terminal: true },
      reason: "Batch cancel from Tenuto Console",
    })
  })
  it("sends reset_type only for reset", () => {
    const req = selectedRunsBatchRequest("ns", "reset", ["r1"], " fix ", "first_workflow_task")
    expect(req.reset_type).toBe("first_workflow_task")
    expect(req.reason).toBe("fix")
    expect(selectedRunsBatchRequest("ns", "terminate", ["r1"], "", "first_workflow_task")).not.toHaveProperty("reset_type")
  })
})

describe("batch filter", () => {
  it("omits empty fields and detects the empty filter", () => {
    expect(batchFilter("  ", [])).toEqual({})
    expect(isEmptyBatchFilter(batchFilter("", []))).toBe(true)
    expect(batchFilter(" order-1 ", ["failed"])).toEqual({ workflow_id: "order-1", statuses: ["failed"] })
    expect(isEmptyBatchFilter({ statuses: ["running"] })).toBe(false)
    expect(isEmptyBatchFilter({ run_ids: ["r1"] })).toBe(false)
  })
  it("builds the request with reset_type for reset", () => {
    expect(batchRequest("ns", "reset", { workflow_id: "w" }, "", "first_workflow_task")).toEqual({
      namespace: "ns",
      type: "reset",
      filter: { workflow_id: "w" },
      reset_type: "first_workflow_task",
    })
    expect(batchRequest("ns", "cancel", {}, "why", "first_workflow_task")).toEqual({
      namespace: "ns",
      type: "cancel",
      filter: {},
      reason: "why",
    })
  })
})

describe("batch progress and summary", () => {
  const op = {
    type: "reset" as const,
    status: "completed" as const,
    total_count: 4,
    succeeded_count: 2,
    failed_count: 1,
    skipped_count: 1,
  }
  it("accepts a started batch without claiming a result", () => {
    expect(batchAccepted({ type: "cancel" })).toEqual({ kind: "success", message: "Batch cancel accepted; it runs in the background." })
  })
  it("polls only pending and running batches", () => {
    expect(isActiveBatch("pending")).toBe(true)
    expect(isActiveBatch("running")).toBe(true)
    expect(isActiveBatch("completed")).toBe(false)
    expect(isActiveBatch("cancelled")).toBe(false)
  })
  it("counts succeeded, failed and skipped runs as processed", () => {
    expect(batchProgress(op)).toEqual({ processed: 4, total: 4, percent: 100 })
    expect(batchProgress({ ...op, status: "running", total_count: 10 })).toEqual({ processed: 4, total: 10, percent: 40 })
    expect(batchProgress({ ...op, status: "pending", total_count: 0, succeeded_count: 0, failed_count: 0, skipped_count: 0 }).total).toBe(0)
  })
  it("says a batch cancel only requested cancellation", () => {
    expect(succeededLabel("cancel")).toBe("Cancellation requested")
    expect(batchSummary({ ...op, type: "cancel" })).toBe(
      "Finished 4 runs (cancellation requested: 2, failed: 1, skipped: 1).",
    )
  })
  it("describes every status", () => {
    expect(batchSummary({ ...op, status: "pending" })).toContain("Waiting")
    expect(batchSummary({ ...op, status: "running", total_count: 10 })).toBe(
      "Processing: 4 of 10 runs done (reset: 2, failed: 1, skipped: 1).",
    )
    expect(batchSummary({ ...op, status: "cancelled" })).toContain("the remaining runs were not processed")
    expect(batchSummary({ ...op, status: "failed" })).toContain("Every processed run failed")
    expect(batchSummary({ ...op, succeeded_count: 0, failed_count: 0, skipped_count: 0, total_count: 0 })).toBe(
      "No runs matched; nothing was changed.",
    )
  })
})

describe("batchStatusStyle", () => {
  it("styles every server status and falls back for unknown ones", () => {
    expect(batchStatusStyle("pending").dot).toBe("bg-slate-500")
    expect(batchStatusStyle("running").dot).toBe("bg-blue-500")
    expect(batchStatusStyle("completed").dot).toBe("bg-emerald-500")
    expect(batchStatusStyle("cancelled").dot).toBe("bg-amber-500")
    expect(batchStatusStyle("failed").dot).toBe("bg-red-500")
    expect(batchStatusStyle("other").dot).toBe("bg-slate-400")
  })
})
