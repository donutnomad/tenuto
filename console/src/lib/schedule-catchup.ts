// Schedule 的 catchup_window 在 API 里是纳秒整数。表单接受 "90s"、"30m"、"1h"、"7d" 这样的时长，留空取 Server 的缺省值（365 天）。

const NS_PER_UNIT: Record<string, number> = {
  s: 1e9,
  m: 60e9,
  h: 3600e9,
  d: 86400e9,
}

// MIN_CATCHUP_WINDOW_NS 与 Server 的 schedule.MinCatchupWindow 相同。
export const MIN_CATCHUP_WINDOW_NS = 10e9

// parseCatchupWindow 把表单文本换成纳秒：空串返回 undefined（用缺省值），格式不对或小于 10 秒时抛错。
export function parseCatchupWindow(text: string): number | undefined {
  const trimmed = text.trim()
  if (trimmed === "") return undefined
  const match = /^(\d+)\s*([smhd])$/.exec(trimmed)
  if (!match) throw new Error('Catch-up window must look like "90s", "30m", "1h" or "7d"')
  const ns = Number(match[1]) * NS_PER_UNIT[match[2]]
  if (ns < MIN_CATCHUP_WINDOW_NS) throw new Error("Catch-up window must be at least 10s")
  return ns
}

// formatCatchupWindow 用能整除的最大单位显示纳秒时长。
export function formatCatchupWindow(ns: number | undefined): string {
  if (!ns) return "-"
  for (const unit of ["d", "h", "m", "s"]) {
    const size = NS_PER_UNIT[unit]
    if (ns % size === 0) return `${ns / size}${unit}`
  }
  return `${ns / 1e9}s`
}
