import type { RunListResult } from "./types"

// Runs 列表页按 offset 翻页、不请求总数（skip_total）：Server 不对整个 namespace 计数，是否有下一页看 has_more。

export interface RunListPage {
  // 本页第一行与最后一行的序号（从 1 起）；空页为 0 与 0。
  first: number
  last: number
  hasPrevious: boolean
  hasNext: boolean
}

export function runListPage(page: number, pageSize: number, result: RunListResult | undefined): RunListPage {
  const count = result?.items.length ?? 0
  const first = count > 0 ? page * pageSize + 1 : 0
  return {
    first,
    last: count > 0 ? first + count - 1 : 0,
    hasPrevious: page > 0,
    hasNext: result?.has_more ?? false,
  }
}
