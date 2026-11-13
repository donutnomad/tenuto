import { describe, expect, it } from 'vitest'
import { formatByteSize, fullHistoryIsBehind, historyPageProgress, mergeHistoryPages, mergeLatestHistoryPage, runHistoryEvents } from './history-pagination'
import type { HistoryPage } from './types'

function page(ids: number[], next = '', total = ids.length, bytes = 100): HistoryPage {
  return {
    events: ids.map((id) => ({ id, timestamp: '2026-07-16T00:00:00Z' })),
    next_page_token: next,
    snapshot_max_event_id: total,
    history_event_count: total,
    history_size_bytes: bytes,
  }
}

describe('history pagination', () => {
  it('merges pages in server order and de-duplicates boundary events', () => {
    expect(mergeHistoryPages([page([5, 4, 3], 'next', 5), page([3, 2, 1], '', 5)]).map((event) => event.id))
      .toEqual([5, 4, 3, 2, 1])
  })

  it('reports snapshot progress from the first and last pages', () => {
    expect(historyPageProgress([page([5, 4], 'next', 5, 4096), page([3, 2], 'more', 5, 4096)]))
      .toEqual({ loaded: 4, total: 5, bytes: 4096, hasMore: true })
  })

  it('formats byte sizes for visibility metadata', () => {
    expect(formatByteSize(0)).toBe('0 B')
    expect(formatByteSize(1536)).toBe('1.50 KB')
    expect(formatByteSize(10 * 1024 * 1024)).toBe('10.0 MB')
  })
})


describe('run action history source', () => {
  it('supplies short paged history to Reset while full history remains null', () => {
    const boundary = { id: 4, timestamp: '2026-11-04T00:00:00Z', workflowTaskCompleted: { scheduledEventId: 2 } }
    const first = { ...page([5, 3, 2, 1]), events: [boundary, ...page([5, 3, 2, 1]).events] }
    const loaded = mergeHistoryPages([first])
    expect(historyPageProgress([first]).hasMore).toBe(false)
    expect(runHistoryEvents(null, loaded).map((event) => event.id)).toEqual([1, 2, 3, 4, 5])
    expect(runHistoryEvents(null, loaded).find((event) => event.workflowTaskCompleted)?.id).toBe(4)
  })

  it('uses the fetched complete history when older boundaries are outside loaded pages', () => {
    const loaded = mergeHistoryPages([page([11, 10], 'older', 11)])
    const complete = [...page([11, 10, 9, 8, 7, 6, 5, 3, 2, 1]).events,
      { id: 4, timestamp: '2026-11-04T00:00:00Z', workflowTaskCompleted: { scheduledEventId: 2 } }]
    expect(historyPageProgress([page([11, 10], 'older', 11)]).hasMore).toBe(true)
    const events = runHistoryEvents(complete, loaded)
    expect(events).toHaveLength(11)
    expect(events[3].workflowTaskCompleted).toEqual({ scheduledEventId: 2 })
    expect(loaded).toHaveLength(2)
  })
})

describe('live refresh of the newest page', () => {
  it('prepends new events and keeps the cursor of the already loaded older pages', () => {
    const first = page([5, 4, 3], 'older-of-5', 5, 500)
    const latest = page([7, 6, 5], 'older-of-7', 7, 700)
    const merged = mergeLatestHistoryPage(first, latest)
    expect(merged.events.map((event) => event.id)).toEqual([7, 6, 5, 4, 3])
    expect(merged.next_page_token).toBe('older-of-5')
    expect(merged.history_event_count).toBe(7)
    expect(merged.history_size_bytes).toBe(700)

    const older = page([2, 1], '', 5)
    expect(mergeHistoryPages([merged, older]).map((event) => event.id)).toEqual([7, 6, 5, 4, 3, 2, 1])
    expect(historyPageProgress([merged, older])).toEqual({ loaded: 7, total: 7, bytes: 700, hasMore: false })
  })

  it('keeps the loaded first page object when nothing new arrived', () => {
    const first = page([5, 4, 3], 'older', 5)
    expect(mergeLatestHistoryPage(first, page([5, 4, 3], 'other-token', 5))).toBe(first)
  })

  it('accumulates across refreshes beyond one page', () => {
    const afterFirstPoll = mergeLatestHistoryPage(page([3, 2, 1], '', 3), page([5, 4, 3], 'x', 5))
    const afterSecondPoll = mergeLatestHistoryPage(afterFirstPoll, page([7, 6, 5], 'y', 7))
    expect(afterSecondPoll.events.map((event) => event.id)).toEqual([7, 6, 5, 4, 3, 2, 1])
    expect(afterSecondPoll.next_page_token).toBe('')
  })

  it('restarts from the newest page when events were skipped between refreshes', () => {
    const latest = page([20, 19, 18], 'older-of-18', 20)
    expect(mergeLatestHistoryPage(page([5, 4, 3], 'older-of-3', 5), latest)).toBe(latest)
  })

  it('uses the newest page as the first page when nothing is loaded yet', () => {
    const latest = page([2, 1], '', 2)
    expect(mergeLatestHistoryPage(undefined, latest)).toBe(latest)
  })

  it('adds events newer than the fetched complete history', () => {
    const complete = page([1, 2, 3, 4]).events
    const loaded = mergeHistoryPages([page([6, 5, 4], 'older', 6)])
    expect(runHistoryEvents(complete, loaded).map((event) => event.id)).toEqual([1, 2, 3, 4, 5, 6])
  })
})

describe('full history snapshot against live pages', () => {
  it('reports a gap when the loaded pages start after the snapshot ends', () => {
    const complete = page([1, 2, 3, 4]).events
    expect(fullHistoryIsBehind(complete, mergeHistoryPages([page([9, 8, 7], 'older', 9)]))).toBe(true)
  })

  it('reports no gap when the pages continue or overlap the snapshot', () => {
    const complete = page([1, 2, 3, 4]).events
    expect(fullHistoryIsBehind(complete, mergeHistoryPages([page([6, 5], 'older', 6)]))).toBe(false)
    expect(fullHistoryIsBehind(complete, mergeHistoryPages([page([5, 4, 3], 'older', 5)]))).toBe(false)
    expect(fullHistoryIsBehind(null, mergeHistoryPages([page([9, 8], 'older', 9)]))).toBe(false)
    expect(fullHistoryIsBehind(complete, [])).toBe(false)
  })
})
