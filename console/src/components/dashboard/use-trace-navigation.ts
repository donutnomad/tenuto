import { useCallback, useMemo, useState } from "react"
import type { RunStatus } from "@/lib/types"
import type { TraceNavigationSnapshot } from "@/lib/trace-session"
import type { Trace } from "@/lib/wasm-loader"
import {
  splitLanes,
  continueToNextBreakpoint,
  findNextBreakpointStep,
  hydrateTraceNavigation,
  laneCursorOfStep,
  nextCursorOnLaneSwitch,
  terminalStepAction,
} from "./trace-debugger-logic"

// Trace 调试器在一条 Trace 上的导航状态：聚焦 lane、每条 lane 的 cursor、断点、
// 选中的栈帧、终态停靠，以及由导航动作收起的 lane / 子 Workflow 菜单。
// 跨 Workflow 边界的动作由调用方通过 on*Boundary 提供。
export function useTraceNavigation({
  trace,
  runStatus,
  onStepBackBoundary,
  onStepForwardBoundary,
  onContinueBoundary,
}: {
  trace: Trace | null
  runStatus?: RunStatus
  onStepBackBoundary?: () => void
  onStepForwardBoundary?: () => void
  onContinueBoundary?: () => void
}) {
  // focusLane：当前聚焦的执行线（Delve 里 goroutine 的概念）。
  const [focusLane, setFocusLane] = useState("main")
  const [laneMenuOpen, setLaneMenuOpen] = useState(false)
  const [childMenuOpen, setChildMenuOpen] = useState(false)
  const [terminalStop, setTerminalStop] = useState(false)
  const [selectedFrameIndex, setSelectedFrameIndex] = useState(0)
  // cursorByLane：cursor 按 lane 记忆——切 lane 恢复上次位置，而非归零。
  const [cursorByLane, setCursorByLane] = useState<Map<string, number>>(new Map())
  // breakpoints：断点行号集合，全局跨 lane 共享。
  const [breakpoints, setBreakpoints] = useState<Set<number>>(new Set())

  // 派生：按 lane 分组的步序列。
  const lanesSteps = useMemo(() => splitLanes(trace?.steps ?? []), [trace])
  const focusSteps = useMemo(() => lanesSteps.get(focusLane) ?? [], [lanesSteps, focusLane])
  const cursor = cursorByLane.get(focusLane) ?? 0
  const current = focusSteps[cursor]
  const currentFrames = useMemo(() => {
    if (current?.frames?.length) return current.frames
    if (!current) return []
    return [{ name: focusLane, line: current.line, path: current.path, vars: current.vars }]
  }, [current, focusLane])
  const selectedFrame = currentFrames[Math.min(selectedFrameIndex, Math.max(currentFrames.length - 1, 0))]
  const currentLine = selectedFrame?.line ?? current?.line ?? -1
  const variables = selectedFrame?.vars ?? current?.vars ?? {}
  const nextBreakpoint = useMemo(
    () => findNextBreakpointStep(trace?.steps ?? [], current?.seq ?? -1, breakpoints),
    [trace, current?.seq, breakpoints],
  )
  const terminalStatus = runStatus != null
    ? runStatus !== "running"
    : trace?.run_kind === "completed"
  const atTraceEnd = current?.seq === trace?.steps.at(-1)?.seq

  const focusLaneInfo = useMemo(
    () => trace?.lanes.find((l) => l.lane_id === focusLane) ?? null,
    [trace, focusLane],
  )

  // 已执行行集合：focusSteps.slice(0, cursor+1) 覆盖过的 line，用于浅色底。
  const executedLines = useMemo(() => {
    const s = new Set<number>()
    for (let i = 0; i <= cursor && i < focusSteps.length; i++) {
      s.add(focusSteps[i].line)
    }
    return s
  }, [focusSteps, cursor])

  const setCursor = useCallback(
    (laneID: string, value: number) => {
      setChildMenuOpen(false)
      setTerminalStop(false)
      setCursorByLane((prev) => {
        const next = new Map(prev)
        next.set(laneID, value)
        return next
      })
    },
    [],
  )

  // 按快照定位（首次加载、URL 恢复、跨 Workflow 返回）。trace 由调用方传入：
  // 此时它可能还没写进 state。
  const hydrate = useCallback((loaded: Trace, initialNavigation?: TraceNavigationSnapshot) => {
    const hydrated = hydrateTraceNavigation(loaded, initialNavigation, runStatus)
    setFocusLane(hydrated.focusLane)
    if (!hydrated.restored) return
    setCursorByLane(hydrated.restored.cursorByLane)
    setSelectedFrameIndex(hydrated.restored.selectedFrameIndex)
    setBreakpoints(hydrated.restored.breakpoints)
    setTerminalStop(hydrated.restored.terminalStop)
  }, [runStatus])

  const reset = useCallback(() => {
    setCursorByLane(new Map())
    setFocusLane(trace?.lanes.find((lane) => lane.lane_id === "main")?.lane_id ?? trace?.lanes[0]?.lane_id ?? "main")
    setSelectedFrameIndex(0)
    setLaneMenuOpen(false)
    setChildMenuOpen(false)
    setTerminalStop(false)
  }, [trace])

  // 跳到全局 seq 对应的步并聚焦它所在的 lane；该步不存在时返回 false，状态不变。
  const focusStep = useCallback((laneID: string, seq: number): boolean => {
    const destinationCursor = laneCursorOfStep(lanesSteps, laneID, seq)
    if (destinationCursor < 0) return false
    setSelectedFrameIndex(0)
    setFocusLane(laneID)
    setCursor(laneID, destinationCursor)
    setLaneMenuOpen(false)
    setChildMenuOpen(false)
    return true
  }, [lanesSteps, setCursor])

  const stepForward = useCallback(() => {
    if (focusSteps.length === 0) return
    const action = terminalStepAction({
      atLastStep: atTraceEnd,
      terminalStop,
      terminalStatus,
      canCrossBoundary: Boolean(onStepForwardBoundary),
    })
    if (action === "enter-terminal") {
      setTerminalStop(true)
      return
    }
    if (action === "cross-boundary") {
      onStepForwardBoundary?.()
      return
    }
    if (action === "stay") return
    setSelectedFrameIndex(0)
    setCursor(focusLane, Math.min(cursor + 1, focusSteps.length - 1))
  }, [focusSteps.length, focusLane, cursor, setCursor, atTraceEnd, terminalStop, terminalStatus, onStepForwardBoundary])

  const stepBack = useCallback(() => {
    if (terminalStop) {
      setTerminalStop(false)
      return
    }
    if (cursor <= 0 && onStepBackBoundary) {
      onStepBackBoundary()
      return
    }
    setSelectedFrameIndex(0)
    setCursor(focusLane, Math.max(cursor - 1, 0))
  }, [terminalStop, focusLane, cursor, setCursor, onStepBackBoundary])

  const continueRun = useCallback(() => {
    setSelectedFrameIndex(0)
    if (terminalStop) {
      onContinueBoundary?.()
      return
    }
    if (nextBreakpoint) {
      const destinationCursor = laneCursorOfStep(lanesSteps, nextBreakpoint.lane_id, nextBreakpoint.seq)
      if (destinationCursor >= 0) {
        setCursor(nextBreakpoint.lane_id, destinationCursor)
        setFocusLane(nextBreakpoint.lane_id)
        return
      }
    }
    if (terminalStatus && !atTraceEnd) {
      const destination = trace?.steps.at(-1)
      if (destination) {
        const destinationCursor = laneCursorOfStep(lanesSteps, destination.lane_id, destination.seq)
        if (destinationCursor >= 0) {
          setCursor(destination.lane_id, destinationCursor)
          setFocusLane(destination.lane_id)
          return
        }
      }
    }
    if (terminalStatus && runStatus !== "completed" && runStatus !== "continued_as_new") {
      const destination = trace?.steps.at(-1)
      if (destination && !atTraceEnd) {
        const destinationCursor = laneCursorOfStep(lanesSteps, destination.lane_id, destination.seq)
        if (destinationCursor >= 0) {
          setCursor(destination.lane_id, destinationCursor)
          setFocusLane(destination.lane_id)
        }
      }
      setTerminalStop(true)
      return
    }
    if (terminalStatus && onContinueBoundary) {
      onContinueBoundary()
      return
    }
    const next = continueToNextBreakpoint(focusSteps, cursor, breakpoints)
    setCursor(focusLane, next)
  }, [terminalStop, nextBreakpoint, lanesSteps, terminalStatus, atTraceEnd, trace, runStatus, onContinueBoundary, focusSteps, cursor, breakpoints, focusLane, setCursor])

  const switchLane = useCallback(
    (laneID: string) => {
      // 切 lane：Delve 范式——恢复该 lane 上次 cursor，首访从 0。
      const restored = nextCursorOnLaneSwitch(cursorByLane, laneID)
      setCursor(laneID, restored)
      setFocusLane(laneID)
      setLaneMenuOpen(false)
      setChildMenuOpen(false)
      setTerminalStop(false)
      setSelectedFrameIndex(0)
    },
    [cursorByLane, setCursor],
  )

  const toggleBreakpoint = useCallback((line: number) => {
    setBreakpoints((prev) => {
      const next = new Set(prev)
      if (next.has(line)) next.delete(line)
      else next.add(line)
      return next
    })
  }, [])

  const snapshot = useCallback((): TraceNavigationSnapshot => {
    const laneCursors = new Map(cursorByLane)
    laneCursors.set(focusLane, cursor)
    return {
      focusLane,
      cursorByLane: Array.from(laneCursors.entries()),
      selectedFrameIndex,
      breakpoints: Array.from(breakpoints),
      stepSeq: current?.seq ?? -1,
      line: currentLine,
      frameName: selectedFrame?.name ?? focusLane,
      terminalStop,
    }
  }, [focusLane, cursorByLane, cursor, selectedFrameIndex, breakpoints, current?.seq, currentLine, selectedFrame, terminalStop])

  return {
    focusLane,
    focusLaneInfo,
    focusSteps,
    lanesSteps,
    cursor,
    current,
    currentFrames,
    selectedFrame,
    selectedFrameIndex,
    setSelectedFrameIndex,
    currentLine,
    variables,
    executedLines,
    breakpoints,
    nextBreakpoint,
    terminalStop,
    terminalStatus,
    atTraceEnd,
    laneMenuOpen,
    setLaneMenuOpen,
    childMenuOpen,
    setChildMenuOpen,

    hydrate,
    reset,
    focusStep,
    stepForward,
    stepBack,
    continueRun,
    switchLane,
    toggleBreakpoint,
    snapshot,
  }
}
