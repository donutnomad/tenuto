import { useEffect, useMemo, useState } from "react"
import useSWR from "swr"
import { api } from "@/lib/api"
import type { ActivityState, Event, Run, WorkflowConfig, WorkflowConfigListResult } from "@/lib/types"
import { REFRESH_INTERVAL_DETAIL } from "@/lib/constants"
import { fullHistoryIsBehind, historyPageProgress, mergeHistoryPages, runHistoryEvents } from "@/lib/history-pagination"
import { buildStepTree } from "@/lib/build-step-tree"
import { useRunHistoryPages } from "@/lib/use-run-history-pages"
import { buildRunRelationships } from "@/lib/run-relationships"
import { getWlangProgram } from "@/lib/wlang-program"
import { pendingActivitySteps, runEndTime, runTaskQueue } from "@/lib/run-detail"

// Run 详情页的全部远端数据（run、事件历史、activity 状态、配置）及其派生值。
// 运行中的 Run 按 REFRESH_INTERVAL_DETAIL 轮询；事件历史默认分页，
// ensureFullHistory 在 Reset / Replay / Relationships 需要完整历史时一次拉全。
export function useRunDetail(namespace: string, runID: string) {
  const [fullHistory, setFullHistory] = useState<Event[] | null>(null)
  const [fullHistoryLoading, setFullHistoryLoading] = useState(false)

  const {
    data: run,
    error: runError,
    mutate: mutateRun,
    isValidating: isRunValidating,
  } = useSWR<Run>(["run", namespace, runID], () => api.getRun(namespace, runID), {
    refreshInterval: (data) =>
      data?.status === "running" ? REFRESH_INTERVAL_DETAIL : 0,
  })

  const {
    pages: historyPages,
    error: historyError,
    isLoading: isHistoryLoading,
    isValidating: isHistoryValidating,
    size: historyPageCount,
    setSize: setHistoryPageCount,
    reload: reloadHistoryPages,
  } = useRunHistoryPages(namespace, runID, run?.status === "running")
  const pagedEvents = useMemo(() => mergeHistoryPages(historyPages), [historyPages])
  const historyProgress = useMemo(() => historyPageProgress(historyPages), [historyPages])
  const events = useMemo(
    () => runHistoryEvents(fullHistory, pagedEvents),
    [fullHistory, pagedEvents],
  )

  // 完整历史快照落后于轮询分页、中间缺一段时重新拉取。失败时保留旧快照，下一次轮询再试。
  useEffect(() => {
    if (!fullHistoryIsBehind(fullHistory, pagedEvents)) return
    let cancelled = false
    api.getHistory(namespace, runID).then(
      (loaded) => {
        if (!cancelled) setFullHistory(loaded)
      },
      () => {},
    )
    return () => {
      cancelled = true
    }
  }, [fullHistory, pagedEvents, namespace, runID])

  async function ensureFullHistory(): Promise<Event[]> {
    if (fullHistory) return fullHistory
    if (!historyProgress.hasMore) return events
    setFullHistoryLoading(true)
    try {
      const loaded = await api.getHistory(namespace, runID)
      setFullHistory(loaded)
      return loaded
    } finally {
      setFullHistoryLoading(false)
    }
  }

  async function refreshHistory() {
    setFullHistory(null)
    await reloadHistoryPages()
  }

  const { data: activityStates, error: activityStatesError } = useSWR<ActivityState[]>(
    run?.status === "running" ? ["activity-states", namespace, runID] : null,
    () => api.getActivityStates(namespace, runID),
    { refreshInterval: REFRESH_INTERVAL_DETAIL }
  )

  const { data: configSummaries, error: configSummariesError } = useSWR<WorkflowConfigListResult>(
    ["config-summaries", namespace],
    () => api.listWorkflowConfigs(namespace),
    { refreshInterval: REFRESH_INTERVAL_DETAIL },
  )
  const latestVersion = useMemo(
    () => configSummaries?.items.find((item) => item.workflow_type === run?.workflow_type)?.latest_version,
    [configSummaries, run?.workflow_type],
  )

  const { data: workflowConfig, error: workflowConfigError } = useSWR<WorkflowConfig>(
    ["config", namespace, runID],
    () => api.getConfig(namespace, runID),
    { revalidateOnFocus: false, shouldRetryOnError: false }
  )

  const allSteps = useMemo(() => {
    return buildStepTree(events, run?.status, activityStates)
  }, [events, run?.status, activityStates])

  const wlangProgram = useMemo(() => {
    if (!workflowConfig) return null
    return getWlangProgram(workflowConfig)
  }, [workflowConfig])

  // Replay 需要完整历史：分页还没拉全时为 null。
  const eventsJSON = useMemo(() => {
    if (fullHistory) return JSON.stringify(fullHistory)
    if (!historyProgress.hasMore && events.length > 0) return JSON.stringify(events)
    return null
  }, [events, fullHistory, historyProgress.hasMore])

  const pendingSteps = useMemo(() => pendingActivitySteps(allSteps), [allSteps])
  const relationshipCount = useMemo(() => buildRunRelationships(run, events).length, [run, events])
  const endTime = useMemo(() => runEndTime(events), [events])
  const taskQueue = useMemo(() => runTaskQueue(events), [events])

  // 辅助数据加载失败：页面主体照常显示，在提示条里列出失败项。
  const secondaryErrors = [
    { label: "activity states", error: activityStatesError },
    { label: "workflow versions", error: configSummariesError },
    { label: "workflow config", error: workflowConfigError },
  ].filter((item) => item.error)

  return {
    run,
    runError,
    mutateRun,
    isAutoRefreshing: run?.status === "running" && (isRunValidating || isHistoryValidating),

    events,
    historyPages,
    historyProgress,
    historyError,
    isHistoryLoading,
    isHistoryValidating,
    loadOlderHistory: () => setHistoryPageCount(historyPageCount + 1),
    hasFullHistory: fullHistory !== null,
    fullHistoryLoading,
    ensureFullHistory,
    refreshHistory,

    activityStates,
    workflowConfig,
    latestVersion,
    secondaryErrors,

    allSteps,
    pendingSteps,
    wlangProgram,
    eventsJSON,
    relationshipCount,
    runEndTime: endTime,
    taskQueue,
  }
}
