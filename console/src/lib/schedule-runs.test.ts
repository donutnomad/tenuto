import { describe, expect, it } from "vitest"
import { SCHEDULED_BY_ID_ATTRIBUTE, scheduleRunsFilter, scheduleRunsHref } from "./schedule-runs"

describe("schedule runs", () => {
  it("links to the runs list filtered by schedule", () => {
    expect(scheduleRunsHref("default", "nightly report")).toBe("/default/runs?schedule_id=nightly%20report")
  })

  it("filters runs by the TemporalScheduledById search attribute", () => {
    expect(scheduleRunsFilter("sch-1")).toEqual({ TemporalScheduledById: "sch-1" })
    expect(SCHEDULED_BY_ID_ATTRIBUTE).toBe("TemporalScheduledById")
    expect(scheduleRunsFilter("")).toBeUndefined()
  })
})
