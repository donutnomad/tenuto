// timeAgo 把时间写成相对当前的时长：过去的时间写成 "5m ago"，将来的时间（Schedule 的下次运行、Worker 的过期时间）
// 写成 "in 5m"，相差不到 1 秒写成 "just now"。
export function timeAgo(dateStr: string, nowMs: number = Date.now()): string {
  const diff = nowMs - new Date(dateStr).getTime()
  const seconds = Math.floor(Math.abs(diff) / 1000)
  if (seconds < 1) return "just now"
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  const span = seconds < 60 ? `${seconds}s` : minutes < 60 ? `${minutes}m` : hours < 24 ? `${hours}h` : `${days}d`
  return diff < 0 ? `in ${span}` : `${span} ago`
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null) return "-"
  // 时间轴刻度是浮点数（如 201.20000000000002），10ms 以下保留两位小数，其余取整。
  if (ms < 1000) return `${ms < 10 ? Math.round(ms * 100) / 100 : Math.round(ms)}ms`
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  const remainSec = seconds % 60
  if (minutes < 60) return `${minutes}m ${remainSec}s`
  const hours = Math.floor(minutes / 60)
  const remainMin = minutes % 60
  return `${hours}h ${remainMin}m`
}

// formatAxisTick 写时间轴刻度：1s 到 1m 之间按刻度间隔保留小数，相邻刻度不会显示成同一个值
// （间隔 0.24s 时写 1.2s、1.4s，而不是两个 1s）。1s 以下与 1m 以上的刻度间隔不小于显示精度，沿用 formatDuration。
export function formatAxisTick(ms: number, stepMs: number): string {
  if (ms < 1000 || ms >= 60_000) return formatDuration(ms)
  const decimals = stepMs < 100 ? 2 : stepMs < 10_000 ? 1 : 0
  return `${Number((ms / 1000).toFixed(decimals))}s`
}

// 毫秒时长写成完整单词（对齐 Temporal 的事件详情风格）；0 返回空串，调用方据此隐藏整行。
export function formatDurationWords(ms: number): string {
  if (ms === 0) return ""
  if (ms < 1000) return `${ms}ms`

  const totalSeconds = Math.floor(ms / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  const parts: string[] = []
  if (hours > 0) parts.push(hours === 1 ? "1 hour" : `${hours} hours`)
  if (minutes > 0) parts.push(minutes === 1 ? "1 minute" : `${minutes} minutes`)
  if (seconds > 0) parts.push(seconds === 1 ? "1 second" : `${seconds} seconds`)
  return parts.join(" ")
}

export function formatTimestamp(dateStr: string): string {
  return new Date(dateStr).toLocaleString()
}

export function formatTimestampWithAgo(dateStr: string): string {
  const d = new Date(dateStr)
  const mo = d.getMonth() + 1
  const day = d.getDate()
  const h = String(d.getHours()).padStart(2, "0")
  const m = String(d.getMinutes()).padStart(2, "0")
  const s = String(d.getSeconds()).padStart(2, "0")
  const yearPrefix = d.getFullYear() !== new Date().getFullYear() ? `${d.getFullYear()}/` : ""
  return `${yearPrefix}${mo}/${day} ${h}:${m}:${s} (${timeAgo(dateStr)})`
}

// event history 专用：xxxx年xx月xx日 GMT+xx hh:mm:ss.xx，不带「x ago」（相对时间对逐事件无意义）。
export function formatEventTimestamp(dateStr: string): string {
  const d = new Date(dateStr)
  const year = d.getFullYear()
  const mo = d.getMonth() + 1
  const day = d.getDate()
  const h = String(d.getHours()).padStart(2, "0")
  const m = String(d.getMinutes()).padStart(2, "0")
  const s = String(d.getSeconds()).padStart(2, "0")
  // 百分之一秒（2 位）：毫秒 3 位补零后截前 2 位，对齐 Temporal 的 .xx 显示。
  const cs = String(d.getMilliseconds()).padStart(3, "0").slice(0, 2)
  // 本地时区相对 UTC 的偏移（小时）。getTimezoneOffset 返回分钟且符号相反（东八区为 -480）。
  const offsetHours = -d.getTimezoneOffset() / 60
  const gmt = `GMT${offsetHours >= 0 ? "+" : "-"}${Math.abs(offsetHours)}`
  return `${year}年${mo}月${day}日 ${gmt} ${h}:${m}:${s}.${cs}`
}

export function truncateID(id: string, maxLen = 12): string {
  if (id.length <= maxLen) return id
  return id.slice(0, maxLen) + "..."
}

export function formatTimeoutMs(ms: number): string {
  if (ms <= 0) return "-"
  const s = ms / 1000
  if (s < 60) return s === Math.floor(s) ? `${s} second${s !== 1 ? "s" : ""}` : `${s}s`
  const m = s / 60
  if (m < 60) return m === Math.floor(m) ? `${m} minute${m !== 1 ? "s" : ""}` : `${Math.floor(m)}m ${Math.round(s % 60)}s`
  const h = m / 60
  return h === Math.floor(h) ? `${h} hour${h !== 1 ? "s" : ""}` : `${Math.floor(h)}h ${Math.round(m % 60)}m`
}

export function formatDateTimeColumn(dateStr: string): string {
  const d = new Date(dateStr)
  const year = d.getFullYear()
  const mo = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  const h = String(d.getHours()).padStart(2, "0")
  const m = String(d.getMinutes()).padStart(2, "0")
  const s = String(d.getSeconds()).padStart(2, "0")
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone
  return `${year}/${mo}/${day} ${h}:${m}:${s}`
}

// countNoun 写出带数量的名词：1 run、2 runs。
export function countNoun(count: number, singular: string, plural = `${singular}s`): string {
  return `${count.toLocaleString()} ${count === 1 ? singular : plural}`
}
