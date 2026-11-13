// Trace Debugger 的纯交互逻辑（无 DOM、无 React），抽出便于 vitest 单测。
// 数据来源为 wasm 的 traceReplay 一次性返回的全量 Trace，前端在轨迹上做所有交互。

import type { Trace, TraceEffect, TraceStep, LaneInfo } from "@/lib/wasm-loader"
import type { RunStatus } from "@/lib/types"
import type { TraceActivityInspectorSnapshot, TraceNavigationSnapshot } from "@/lib/trace-session"
import {
  activityAttemptTraceTarget,
  type ActivityAttemptTraceTarget,
  type ActivityInvocationFilter,
  type ActivityInvocationRecord,
  type ActivityInvocationSort,
  type ActivityInvocationTraceAnchor,
} from "@/lib/activity-invocations"

// splitLanes 按 lane_id 分组，每组内按 lane_seq 升序。
// wasm 已按全局 seq 排序，但同一 lane 的步在全局序列里是穿插的（并发），
// 这里按 lane_seq 重排以得到该执行线自身的时间轴。
export function splitLanes(
  steps: TraceStep[],
): Map<string, TraceStep[]> {
  const map = new Map<string, TraceStep[]>()
  for (const step of steps) {
    const arr = map.get(step.lane_id)
    if (arr) arr.push(step)
    else map.set(step.lane_id, [step])
  }
  for (const arr of map.values()) {
    arr.sort((a, b) => a.lane_seq - b.lane_seq)
  }
  return map
}

// continueToNextBreakpoint 从 cursor+1 向后（仅在当前 lane 内）扫描，
// 命中断点行则停在该步；无命中则停在 lane 末尾。断点是行号集合，跨 lane 全局共享，
// 但 Continue 只在聚焦 lane 内推进，绝不跨 lane。
export function continueToNextBreakpoint(
  steps: TraceStep[],
  cursor: number,
  breakpoints: Set<number>,
): number {
  if (steps.length === 0) return 0
  const last = steps.length - 1
  for (let i = cursor + 1; i <= last; i++) {
    if (breakpoints.has(steps[i].line)) return i
  }
  return last
}

export function findNextBreakpointStep(
  steps: TraceStep[],
  currentSeq: number,
  breakpoints: Set<number>,
): TraceStep | null {
  for (const step of steps) {
    if (step.seq > currentSeq && breakpoints.has(step.line)) return step
  }
  return null
}

export function findBreakpointStepAtOrAfter(
  steps: TraceStep[],
  resumeSeq: number,
  breakpoints: Set<number>,
): TraceStep | null {
  for (const step of steps) {
    if (step.seq >= resumeSeq && breakpoints.has(step.line)) return step
  }
  return null
}

export function uniqueEnterableChildEffects(effects: TraceEffect[]): TraceEffect[] {
  const byInvocation = new Map<string, TraceEffect>()
  for (const effect of effects) {
    if (
      effect.kind !== "child-workflow"
      || (effect.phase !== "scheduled" && effect.phase !== "awaited")
    ) continue
    const key = effect.invocation_id || `${effect.child_id ?? ""}:${effect.scheduled_event_id ?? ""}`
    const previous = byInvocation.get(key)
    if (
      !previous
      || effect.phase === "awaited"
      || (!previous.child_run_id && effect.child_run_id)
    ) {
      byInvocation.set(key, effect)
    }
  }
  return Array.from(byInvocation.values())
}

export const uniqueScheduledChildEffects = uniqueEnterableChildEffects

export function childResumeSeq(effect: TraceEffect): number | undefined {
  return effect.resume_seq ?? effect.wait_seq ?? effect.call_seq
}

export function shouldStopAtParentBoundary(
  status: RunStatus,
  breakpointFound: boolean,
  reachedRoot: boolean,
): boolean {
  return breakpointFound
    || reachedRoot
    || status !== "completed"
}

export type TerminalStepAction = "advance" | "enter-terminal" | "cross-boundary" | "stay"

export function terminalStepAction({
  atLastStep,
  terminalStop,
  terminalStatus,
  canCrossBoundary,
}: {
  atLastStep: boolean
  terminalStop: boolean
  terminalStatus: boolean
  canCrossBoundary: boolean
}): TerminalStepAction {
  if (terminalStop) return canCrossBoundary ? "cross-boundary" : "stay"
  if (atLastStep && terminalStatus) return "enter-terminal"
  return "advance"
}

export function centeredScrollTop({
  containerTop,
  containerScrollTop,
  containerHeight,
  lineTop,
  lineHeight,
}: {
  containerTop: number
  containerScrollTop: number
  containerHeight: number
  lineTop: number
  lineHeight: number
}): number {
  const lineTopInContent = containerScrollTop + lineTop - containerTop
  return Math.max(0, lineTopInContent - containerHeight / 2 + lineHeight / 2)
}

// nextCursorOnLaneSwitch 实现 Delve 范式的 cursor 按 lane 记忆：
// 切到某 lane 时恢复它上次停留的 cursor；若从未访问过则从 0 开始。
export function nextCursorOnLaneSwitch(
  cursorMap: Map<string, number>,
  laneID: string,
): number {
  return cursorMap.get(laneID) ?? 0
}

// frontierSentinelText 为 pending-frontier lane 生成"执行前沿哨兵"文案；
// 其余 end_kind（completed/error）返回 null（不显示哨兵）。
// 文案按 waiting_activity.kind 替换等待对象的主体名词。
const WAIT_KIND_LABEL: Record<string, string> = {
  activity: "activity",
  timer: "定时器",
  signal: "信号",
  update: "update",
  child: "子工作流",
}

export function frontierSentinelText(lane: LaneInfo): string | null {
  if (lane.end_kind !== "pending-frontier") return null
  const wa = lane.waiting_activity
  if (!wa) {
    return "⏸ 此处是 workflow 当前执行前沿，正在等待中"
  }
  const subject = WAIT_KIND_LABEL[wa.kind] ?? wa.kind
  return `⏸ 此处正在等待 ${subject} ${wa.type}（${wa.activity_id}）完成，这是 workflow 当前执行前沿`
}

// laneCursorOfStep 返回全局 seq 对应的步在其 lane 时间轴上的下标；该 lane 没有这一步时为 -1。
export function laneCursorOfStep(
  lanesSteps: ReadonlyMap<string, TraceStep[]>,
  laneID: string,
  seq: number,
): number {
  return (lanesSteps.get(laneID) ?? []).findIndex((step) => step.seq === seq)
}

export interface HydratedTraceNavigation {
  focusLane: string
  // 有 initialNavigation 时恢复的状态；没有时只定位初始 lane，其余状态保持原样。
  restored: {
    cursorByLane: Map<string, number>
    selectedFrameIndex: number
    breakpoints: Set<number>
    terminalStop: boolean
  } | null
}

// hydrateTraceNavigation 把 URL / 父页面给出的导航快照落到一条 Trace 上：
// 快照里的 stepSeq 优先于 cursorByLane；terminalStop 只对已结束的 Run 生效。
export function hydrateTraceNavigation(
  trace: Pick<Trace, "lanes" | "steps">,
  initialNavigation: TraceNavigationSnapshot | undefined,
  runStatus: RunStatus | undefined,
): HydratedTraceNavigation {
  const focusLane = initialNavigation?.focusLane
    ?? trace.lanes.find((lane) => lane.lane_id === "main")?.lane_id
    ?? trace.lanes[0]?.lane_id
    ?? "main"
  if (!initialNavigation) return { focusLane, restored: null }
  const cursorByLane = new Map(initialNavigation.cursorByLane)
  if (initialNavigation.stepSeq >= 0) {
    const laneSteps = trace.steps.filter((step) => step.lane_id === focusLane)
    const restoredCursor = laneSteps.findIndex((step) => step.seq === initialNavigation.stepSeq)
    if (restoredCursor >= 0) cursorByLane.set(focusLane, restoredCursor)
  }
  return {
    focusLane,
    restored: {
      cursorByLane,
      selectedFrameIndex: initialNavigation.selectedFrameIndex,
      breakpoints: new Set(initialNavigation.breakpoints),
      terminalStop: Boolean(initialNavigation.terminalStop && runStatus !== "running"),
    },
  }
}

// childResumeSnapshot：步入子 Workflow 前记录“子 Workflow 返回后父 Workflow 停在哪”。
// 位置取 childResumeSeq(effect) 对应的步；找不到时沿用进入时的位置。
export function childResumeSnapshot(
  entrySnapshot: TraceNavigationSnapshot,
  effect: TraceEffect,
  steps: readonly TraceStep[],
  lanesSteps: ReadonlyMap<string, TraceStep[]>,
): TraceNavigationSnapshot {
  const resumeSeq = childResumeSeq(effect)
  const resumeStep = resumeSeq == null
    ? undefined
    : steps.find((step) => step.seq === resumeSeq)
  const resumeFrame = resumeStep?.frames?.[0]
  const resumeCursors = new Map(entrySnapshot.cursorByLane)
  if (resumeStep) {
    const resumeCursor = laneCursorOfStep(lanesSteps, resumeStep.lane_id, resumeStep.seq)
    if (resumeCursor >= 0) resumeCursors.set(resumeStep.lane_id, resumeCursor)
  }
  return {
    ...entrySnapshot,
    focusLane: resumeStep?.lane_id ?? entrySnapshot.focusLane,
    cursorByLane: Array.from(resumeCursors.entries()),
    selectedFrameIndex: 0,
    stepSeq: resumeSeq ?? entrySnapshot.stepSeq,
    line: resumeFrame?.line ?? resumeStep?.line ?? entrySnapshot.line,
    frameName: resumeFrame?.name ?? resumeStep?.lane_id ?? entrySnapshot.frameName,
  }
}

export interface RestoredActivityInspector {
  selectedActivity: { line: number; eventID: number }
  selectedAttempt: ActivityAttemptTraceTarget | null
  query: string
  filter: ActivityInvocationFilter
  sort: ActivityInvocationSort
}

// restoreActivityInspector 把导航快照里的 Activity Inspector 状态对回当前历史与 Trace；
// 调用在历史或 Trace 里已不存在时返回 null（Inspector 保持关闭）。
export function restoreActivityInspector(
  inspector: TraceActivityInspectorSnapshot | undefined,
  activityIndex: ReadonlyMap<number, ActivityInvocationRecord>,
  traceIndex: ReadonlyMap<number, ActivityInvocationTraceAnchor>,
): RestoredActivityInspector | null {
  if (!inspector) return null
  const anchor = traceIndex.get(inspector.scheduledEventID)
  const invocation = activityIndex.get(inspector.scheduledEventID)
  if (!anchor || !invocation) return null
  const attempt = inspector.attemptEventID == null
    ? undefined
    : invocation.attempts.find((item) =>
      item.startedEventID === inspector.attemptEventID
        || item.completedEventID === inspector.attemptEventID
        || item.errorEventID === inspector.attemptEventID,
    )
  return {
    selectedActivity: { line: anchor.line, eventID: inspector.scheduledEventID },
    selectedAttempt: attempt
      ? activityAttemptTraceTarget(attempt, anchor, inspector.attemptPhase)
      : null,
    query: inspector.query,
    filter: inspector.filter as ActivityInvocationFilter,
    sort: inspector.sort as ActivityInvocationSort,
  }
}
