import { describe, expect, it } from "vitest"
import { stickyCacheSummary } from "./worker-cache"

describe("stickyCacheSummary", () => {
  it("returns null without stats", () => {
    expect(stickyCacheSummary(undefined)).toBeNull()
    expect(stickyCacheSummary(null)).toBeNull()
  })
  it("summarises snake_case stats", () => {
    expect(stickyCacheSummary({ entries: 1200, estimated_bytes: 2048, hits: 3, misses: 1, rejected: 2 })).toEqual({
      usage: `${(1200).toLocaleString()} entries · 2.00 KB`,
      lookups: "75.0% hit · 2 rejected",
    })
  })
  it("treats missing fields as zero instead of throwing", () => {
    // 结构体缺 json tag 时 sticky_cache 只带 Go 字段名（Entries 等），snake_case 字段全部缺失。
    const legacy = { Entries: 5, Hits: 1 } as unknown as Parameters<typeof stickyCacheSummary>[0]
    expect(stickyCacheSummary(legacy)).toEqual({ usage: "0 entries · 0 B", lookups: "No cache lookups" })
  })
})
