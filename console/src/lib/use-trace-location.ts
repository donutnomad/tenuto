import { useCallback, useEffect, useState } from "react"
import {
  traceLocationFromSearch,
  traceLocationQuery,
  type TraceLocation,
  type TraceNavigationSnapshot,
} from "@/lib/trace-session"

// Trace 页面的 URL 状态读写。
// 挂载时和每次浏览器前进/后退时，把 URL 解析成 TraceLocation 交给 onRestore
// （fromHistory 为 true 表示来自 popstate）；write 把当前位置写回 history。
// onRestore 需要保持引用稳定（useCallback），它变化时会重新恢复一次。
export function useTraceLocation(
  rootRunID: string,
  onRestore: (location: TraceLocation, fromHistory: boolean) => void,
) {
  // 首次恢复完成之前为 false：此时 URL 还是权威来源，页面状态先不写回。
  const [ready, setReady] = useState(false)

  useEffect(() => {
    const restore = (fromHistory: boolean) => {
      onRestore(traceLocationFromSearch(rootRunID, window.location.search), fromHistory)
      setReady(true)
    }
    const restoreAfterHistoryNavigation = () => restore(true)
    restore(false)
    window.addEventListener("popstate", restoreAfterHistoryNavigation)
    return () => window.removeEventListener("popstate", restoreAfterHistoryNavigation)
  }, [onRestore, rootRunID])

  const write = useCallback((
    activeRunID: string,
    inspectedRunID: string,
    activeNavigation: TraceNavigationSnapshot | undefined,
    inspectedNavigation: TraceNavigationSnapshot | undefined,
    mode: "push" | "replace",
  ) => {
    const query = traceLocationQuery(rootRunID, activeRunID, inspectedRunID, activeNavigation, inspectedNavigation)
    const url = `${window.location.pathname}${query ? `?${query}` : ""}`
    const state = { traceSession: true, activeRunID, inspectedRunID }
    if (mode === "push") window.history.pushState(state, "", url)
    else window.history.replaceState(state, "", url)
  }, [rootRunID])

  return { ready, write }
}
