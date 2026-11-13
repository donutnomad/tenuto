export type TraceActivityAttemptPhase = "started" | "outcome"

const TRACE_ACTIVITY_FILTERS = new Set([
  "all",
  "current",
  "past",
  "future",
  "completed",
  "failed",
  "running",
  "timed_out",
  "cancelled",
])
const TRACE_ACTIVITY_SORTS = new Set(["sequence", "duration-desc", "duration-asc"])

export interface TraceActivityInspectorSnapshot {
  scheduledEventID: number
  attemptEventID?: number
  attemptPhase?: TraceActivityAttemptPhase
  query: string
  filter: string
  sort: string
}

export interface TraceNavigationSnapshot {
  focusLane: string
  cursorByLane: Array<[string, number]>
  selectedFrameIndex: number
  breakpoints: number[]
  stepSeq: number
  line: number
  frameName: string
  terminalStop?: boolean
  activityInspector?: TraceActivityInspectorSnapshot
}

export function emptyTraceNavigation(): TraceNavigationSnapshot {
  return {
    focusLane: "main",
    cursorByLane: [],
    selectedFrameIndex: 0,
    breakpoints: [],
    stepSeq: -1,
    line: 0,
    frameName: "main",
    terminalStop: false,
  }
}

export function traceNavigationFromParams(
  params: URLSearchParams,
  prefix = "",
): TraceNavigationSnapshot {
  const key = (name: string) => prefix
    ? `${prefix}${name[0].toUpperCase()}${name.slice(1)}`
    : name
  const sequence = Number(params.get(key("seq")) ?? -1)
  const frame = Number(params.get(key("frame")) ?? 0)
  const call = Number(params.get(key("call")))
  const attempt = Number(params.get(key("attempt")))
  const attemptPhase = params.get(key("attemptPhase"))
  const parsedAttemptPhase: TraceActivityAttemptPhase | undefined = attemptPhase === "started" || attemptPhase === "outcome"
    ? attemptPhase
    : undefined
  const activityFilter = params.get(key("callFilter")) || "all"
  const activitySort = params.get(key("callSort")) || "sequence"
  const activityInspector: TraceActivityInspectorSnapshot | undefined = Number.isInteger(call) && call > 0
    ? {
        scheduledEventID: call,
        attemptEventID: Number.isInteger(attempt) && attempt > 0 ? attempt : undefined,
        attemptPhase: parsedAttemptPhase,
        query: params.get(key("callQuery")) ?? "",
        filter: TRACE_ACTIVITY_FILTERS.has(activityFilter) ? activityFilter : "all",
        sort: TRACE_ACTIVITY_SORTS.has(activitySort) ? activitySort : "sequence",
      }
    : undefined
  return {
    ...emptyTraceNavigation(),
    focusLane: params.get(key("lane")) || "main",
    selectedFrameIndex: Number.isFinite(frame) && frame >= 0 ? frame : 0,
    stepSeq: Number.isFinite(sequence) ? sequence : -1,
    terminalStop: params.get(key("terminal")) === "1",
    activityInspector,
  }
}

export function writeTraceNavigationParams(
  params: URLSearchParams,
  navigation: TraceNavigationSnapshot,
  prefix = "",
): void {
  const key = (name: string) => prefix
    ? `${prefix}${name[0].toUpperCase()}${name.slice(1)}`
    : name
  params.set(key("lane"), navigation.focusLane)
  params.set(key("seq"), String(navigation.stepSeq))
  params.set(key("frame"), String(navigation.selectedFrameIndex))
  if (navigation.terminalStop) params.set(key("terminal"), "1")
  const inspector = navigation.activityInspector
  if (!inspector) return
  params.set(key("call"), String(inspector.scheduledEventID))
  if (inspector.attemptEventID != null) {
    params.set(key("attempt"), String(inspector.attemptEventID))
    if (inspector.attemptPhase) params.set(key("attemptPhase"), inspector.attemptPhase)
  }
  if (inspector.query) params.set(key("callQuery"), inspector.query)
  if (inspector.filter && inspector.filter !== "all") params.set(key("callFilter"), inspector.filter)
  if (inspector.sort && inspector.sort !== "sequence") params.set(key("callSort"), inspector.sort)
}

export function traceLineageContainsRun(
  rootSegments: readonly string[],
  boundarySegments: ReadonlyArray<readonly string[]>,
  runID: string,
): boolean {
  return rootSegments.includes(runID)
    || boundarySegments.some((segments) => segments.includes(runID))
}

// Trace 页面的 URL 状态：run 是当前执行到的 Run，inspect 是正在查看的 Run（只读查看祖先/后代时两者不同）。
// 与 root Run 相同的 run、与 run 相同的 inspect 都省略，URL 保持最短。
export interface TraceLocation {
  activeRunID: string
  inspectedRunID: string
  activeNavigation: TraceNavigationSnapshot
  inspectedNavigation: TraceNavigationSnapshot
}

export function traceLocationFromSearch(rootRunID: string, search: string): TraceLocation {
  const params = new URLSearchParams(search)
  const activeRunID = params.get("run") || rootRunID
  const inspectedRunID = params.get("inspect") || activeRunID
  const activeNavigation = traceNavigationFromParams(params)
  return {
    activeRunID,
    inspectedRunID,
    activeNavigation,
    inspectedNavigation: inspectedRunID === activeRunID
      ? activeNavigation
      : traceNavigationFromParams(params, "inspect"),
  }
}

export function traceLocationQuery(
  rootRunID: string,
  activeRunID: string,
  inspectedRunID: string,
  activeNavigation: TraceNavigationSnapshot | undefined,
  inspectedNavigation: TraceNavigationSnapshot | undefined,
): string {
  const params = new URLSearchParams()
  if (activeRunID !== rootRunID) params.set("run", activeRunID)
  if (inspectedRunID !== activeRunID) params.set("inspect", inspectedRunID)
  if (activeNavigation) writeTraceNavigationParams(params, activeNavigation)
  if (inspectedRunID !== activeRunID && inspectedNavigation) {
    writeTraceNavigationParams(params, inspectedNavigation, "inspect")
  }
  return params.toString()
}
