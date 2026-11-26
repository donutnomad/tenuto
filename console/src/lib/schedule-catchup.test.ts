import { describe, expect, it } from "vitest"
import { formatCatchupWindow, parseCatchupWindow } from "./schedule-catchup"

describe("parseCatchupWindow", () => {
  it("leaves the field out when empty so the server default applies", () => {
    expect(parseCatchupWindow("  ")).toBeUndefined()
  })
  it("converts units to nanoseconds", () => {
    expect(parseCatchupWindow("10s")).toBe(10e9)
    expect(parseCatchupWindow("30m")).toBe(1800e9)
    expect(parseCatchupWindow("1h")).toBe(3600e9)
    expect(parseCatchupWindow("365d")).toBe(365 * 86400e9)
  })
  it("rejects malformed values and values below 10s", () => {
    expect(() => parseCatchupWindow("1.5h")).toThrow()
    expect(() => parseCatchupWindow("5s")).toThrow(/at least 10s/)
  })
})

describe("formatCatchupWindow", () => {
  it("uses the largest whole unit", () => {
    expect(formatCatchupWindow(365 * 86400e9)).toBe("365d")
    expect(formatCatchupWindow(5400e9)).toBe("90m")
    expect(formatCatchupWindow(10e9)).toBe("10s")
    expect(formatCatchupWindow(undefined)).toBe("-")
  })
})
