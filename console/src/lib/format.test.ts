import { describe, expect, it } from "vitest"
import { countNoun, formatAxisTick, formatDuration, timeAgo } from "@/lib/format"

describe("timeAgo", () => {
  const now = Date.parse("2026-11-12T10:00:00Z")

  it("writes past times as ago", () => {
    expect(timeAgo("2026-11-12T09:59:30Z", now)).toBe("30s ago")
    expect(timeAgo("2026-11-12T09:55:00Z", now)).toBe("5m ago")
    expect(timeAgo("2026-11-10T10:00:00Z", now)).toBe("2d ago")
  })

  it("writes future times such as a schedule's next run as in", () => {
    expect(timeAgo("2026-11-12T10:05:00Z", now)).toBe("in 5m")
    expect(timeAgo("2026-11-12T20:00:00Z", now)).toBe("in 10h")
    expect(timeAgo("2026-11-12T10:00:30Z", now)).toBe("in 30s")
  })

  it("writes times within a second as just now", () => {
    expect(timeAgo("2026-11-12T10:00:00.400Z", now)).toBe("just now")
    expect(timeAgo("2026-11-12T09:59:59.600Z", now)).toBe("just now")
  })
})

describe("formatDuration", () => {
  it("rounds fractional milliseconds from timeline ticks", () => {
    expect(formatDuration(201.20000000000002)).toBe("201ms")
    expect(formatDuration(3.75)).toBe("3.75ms")
    expect(formatDuration(0.5)).toBe("0.5ms")
    expect(formatDuration(1500)).toBe("1s")
  })
})

describe("formatAxisTick", () => {
  it("keeps sub-second precision between 1s and 2s", () => {
    const step = 1200 / 5
    expect([0, 1, 2, 3, 4, 5].map((i) => formatAxisTick(i * step, step))).toEqual(["0ms", "240ms", "480ms", "720ms", "960ms", "1.2s"])
    expect(formatAxisTick(1440, 240)).toBe("1.4s")
  })

  it("never repeats a label on a five-tick axis", () => {
    for (const total of [1001, 1200, 1999, 2500, 7000, 12_345, 59_000, 61_000, 3_600_000]) {
      const step = total / 5
      const labels = [0, 1, 2, 3, 4, 5].map((i) => formatAxisTick(i * step, step))
      expect(new Set(labels).size, `${total}: ${labels.join(", ")}`).toBe(labels.length)
    }
  })

  it("writes whole seconds without a trailing zero", () => {
    expect(formatAxisTick(2000, 400)).toBe("2s")
    expect(formatAxisTick(30_000, 10_000)).toBe("30s")
  })
})

describe("countNoun", () => {
  it("uses the singular only for one", () => {
    expect(countNoun(1, "run")).toBe("1 run")
    expect(countNoun(0, "run")).toBe("0 runs")
    expect(countNoun(2, "Run")).toBe("2 Runs")
  })
})
