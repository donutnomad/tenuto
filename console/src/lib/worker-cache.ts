import type { StickyCacheStats } from "./types"
import { formatByteSize } from "./history-pagination"

function count(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0
}

// stickyCacheSummary 把 Worker 上报的 sticky cache 统计整理成两行文本；缺失的字段按 0 计。
export function stickyCacheSummary(stats: StickyCacheStats | null | undefined): { usage: string; lookups: string } | null {
  if (!stats || typeof stats !== "object") return null
  const entries = count(stats.entries)
  const hits = count(stats.hits)
  const requests = hits + count(stats.misses)
  return {
    usage: `${entries.toLocaleString()} entries · ${formatByteSize(count(stats.estimated_bytes))}`,
    lookups: requests > 0
      ? `${((hits / requests) * 100).toFixed(1)}% hit · ${count(stats.rejected)} rejected`
      : "No cache lookups",
  }
}
