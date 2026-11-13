import type { Event, HistoryPage } from './types'

export const HISTORY_PAGE_SIZE = 200

export function mergeHistoryPages(pages: HistoryPage[] | undefined): Event[] {
  if (!pages) return []
  const seen = new Set<number>()
  const events: Event[] = []
  for (const page of pages) {
    for (const event of page.events) {
      if (seen.has(event.id)) continue
      seen.add(event.id)
      events.push(event)
    }
  }
  return events
}

// 把轮询拿到的最新一页并入已加载的首页（两者都按 id 降序）。
// 新事件接到首页前面，计数取最新值，next_page_token 保留旧首页的：
// 更旧的已加载分页由旧 token 串起，保留它才能不重拉、不丢页。
// 最新页与首页之间出现断档（一个轮询间隔内新增超过一页）时直接返回最新页，
// 它的新 token 会让更旧的分页按新快照重新加载。
export function mergeLatestHistoryPage(first: HistoryPage | undefined, latest: HistoryPage): HistoryPage {
  if (!first) return latest
  const newestLoaded = first.events[0]?.id ?? 0
  const fresh = latest.events.filter((event) => event.id > newestLoaded)
  if (fresh.length === 0) return first
  if (fresh.length === latest.events.length && latest.next_page_token) return latest
  return { ...latest, events: [...fresh, ...first.events], next_page_token: first.next_page_token }
}

// Run actions and timeline share the same available history. A short history
// is already complete in its first page and needs no separate full-history fetch.
// The full history is a snapshot; newer paged events (live refresh) are merged in by id.
export function runHistoryEvents(fullHistory: Event[] | null, pagedEvents: Event[]): Event[] {
  const byID = new Map<number, Event>()
  for (const event of fullHistory ?? []) byID.set(event.id, event)
  for (const event of pagedEvents) if (!byID.has(event.id)) byID.set(event.id, event)
  return [...byID.values()].sort((left, right) => left.id - right.id)
}

// 完整历史是一次性快照，之后靠轮询到的分页补新事件。一个轮询间隔内新增超过一页时，
// 已加载分页的最旧事件与快照的最新事件之间会缺一段；返回 true 表示需要重新拉取完整历史。
export function fullHistoryIsBehind(fullHistory: Event[] | null, pagedEvents: Event[]): boolean {
  if (!fullHistory || pagedEvents.length === 0) return false
  const newestInSnapshot = fullHistory.reduce((max, event) => Math.max(max, event.id), 0)
  const oldestPaged = pagedEvents.reduce((min, event) => Math.min(min, event.id), Number.POSITIVE_INFINITY)
  return oldestPaged > newestInSnapshot + 1
}

export function historyPageProgress(pages: HistoryPage[] | undefined): {
  loaded: number
  total: number
  bytes: number
  hasMore: boolean
} {
  const loaded = mergeHistoryPages(pages).length
  const first = pages?.[0]
  const last = pages?.at(-1)
  return {
    loaded,
    total: first?.history_event_count ?? loaded,
    bytes: first?.history_size_bytes ?? 0,
    hasMore: Boolean(last?.next_page_token),
  }
}

export function formatByteSize(bytes: number | undefined): string {
  if (bytes == null || bytes < 0) return '-'
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let value = bytes / 1024
  let unit = units[0]
  for (let i = 1; i < units.length && value >= 1024; i++) {
    value /= 1024
    unit = units[i]
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`
}
