import type { RunListResult } from "./types"

// Runs 列表页按 page_token 翻页、不请求总数（skip_total）：Server 沿 (created_at, run_id) 游标读一页，不对整个 namespace
// 计数，是否有下一页看 next_page_token。走过的 token 存成栈：tokens[i] 是第 i 页的 page_token，第 0 页是空串，
// 后退即出栈。

export const FIRST_PAGE: readonly string[] = [""]

// nextPage 把本页应答的 next_page_token 压栈；没有下一页时保持不变。
export function nextPage(tokens: readonly string[], result: RunListResult | undefined): readonly string[] {
  return result?.next_page_token ? [...tokens, result.next_page_token] : tokens
}

// previousPage 回到上一页；已在第一页时保持不变。
export function previousPage(tokens: readonly string[]): readonly string[] {
  return tokens.length > 1 ? tokens.slice(0, -1) : tokens
}

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
    hasNext: Boolean(result?.next_page_token),
  }
}
