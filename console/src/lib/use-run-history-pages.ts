import { useCallback, useEffect, useRef } from "react"
import useSWRInfinite from "swr/infinite"
import { api } from "@/lib/api"
import { REFRESH_INTERVAL_DETAIL } from "@/lib/constants"
import { HISTORY_PAGE_SIZE, mergeLatestHistoryPage } from "@/lib/history-pagination"
import type { HistoryPage } from "@/lib/types"

type HistoryPageKey = readonly ["run-history-page", string, string, string]

// Run 的事件历史分页（降序，首页是最新事件）。
// live 为 true 时按 REFRESH_INTERVAL_DETAIL 只重拉首页，并通过 mergeLatestHistoryPage
// 并入已加载的首页；更旧的分页留在缓存里。live 变为 false 时再补拉一次首页。
export function useRunHistoryPages(namespace: string, runID: string, live: boolean) {
  // 已并入轮询结果的首页；fetcher 用它保留旧 token 和累积的事件。
  const firstPage = useRef<{ runID: string; page: HistoryPage } | null>(null)

  const history = useSWRInfinite<HistoryPage>(
    (index, previous): HistoryPageKey | null => {
      if (index > 0 && !previous?.next_page_token) return null
      return ["run-history-page", namespace, runID, previous?.next_page_token ?? "first"]
    },
    async (key: HistoryPageKey) => {
      const isFirst = key[3] === "first"
      const page = await api.getHistoryPage(key[1], key[2], {
        pageSize: HISTORY_PAGE_SIZE,
        direction: "descending",
        pageToken: isFirst ? undefined : key[3],
      })
      if (!isFirst) return page
      const loaded = firstPage.current?.runID === runID ? firstPage.current.page : undefined
      const merged = mergeLatestHistoryPage(loaded, page)
      firstPage.current = { runID, page: merged }
      return merged
    },
    {
      refreshInterval: live ? REFRESH_INTERVAL_DETAIL : 0,
      revalidateOnFocus: live,
    },
  )
  const { mutate, setSize } = history

  // 传入函数形式的 mutate 只重新校验首页（无参 mutate 会重拉全部已加载分页）。
  const wasLive = useRef(live)
  useEffect(() => {
    if (wasLive.current && !live) void mutate((pages) => pages)
    wasLive.current = live
  }, [live, mutate])

  // 丢弃已加载分页，按新快照从首页重新加载。
  const reload = useCallback(async () => {
    firstPage.current = null
    await setSize(1)
    await mutate()
  }, [mutate, setSize])

  return {
    pages: history.data,
    error: history.error as Error | undefined,
    isLoading: history.isLoading,
    isValidating: history.isValidating,
    size: history.size,
    setSize,
    reload,
  }
}
