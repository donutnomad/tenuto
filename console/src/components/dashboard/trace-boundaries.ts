// Trace 页面跨 Workflow 调试时的调用栈模型（纯逻辑，无 React）：
// 每个 WorkflowBoundary 是一次“父 Workflow 步入子 Workflow”，
// segments 是该子 Workflow 经 ContinueAsNew 串起来的 Run 链（root Workflow 的链单独存为 rootSegments）。

import type { TraceNavigationSnapshot } from "@/lib/trace-session"
import type { TraceRunContext } from "@/lib/trace-context"
import type { Trace, TraceEffect, TraceStep } from "@/lib/wasm-loader"
import { childResumeSeq } from "./trace-debugger-logic"
import type { TraceAncestorFrame } from "./trace-debugger"

export interface WorkflowBoundary {
  parentRunID: string
  parentWorkflowID: string
  childRunID: string
  childWorkflowID: string
  effect: TraceEffect
  entryNavigation: TraceNavigationSnapshot
  resumeNavigation: TraceNavigationSnapshot
  segments: string[]
}

// snapshotForStep 生成停在某一步上的导航快照；base 提供要保留的其他 lane cursor 和断点。
export function snapshotForStep(
  trace: Trace,
  step: TraceStep,
  base?: TraceNavigationSnapshot,
): TraceNavigationSnapshot {
  const laneSteps = trace.steps.filter((item) => item.lane_id === step.lane_id)
  const cursor = laneSteps.findIndex((item) => item.seq === step.seq)
  const frame = step.frames[0]
  const cursorByLane = new Map(base?.cursorByLane ?? [])
  cursorByLane.set(step.lane_id, Math.max(cursor, 0))
  return {
    focusLane: step.lane_id,
    cursorByLane: Array.from(cursorByLane.entries()),
    selectedFrameIndex: 0,
    breakpoints: base?.breakpoints ?? [],
    stepSeq: step.seq,
    line: frame?.line ?? step.line,
    frameName: frame?.name ?? step.lane_id,
  }
}

function findChildEffect(trace: Trace, childRunID: string): { step: TraceStep; effect: TraceEffect } | null {
  for (const step of trace.steps) {
    const effect = step.effects?.find((item) =>
      item.kind === "child-workflow"
      && item.phase === "scheduled"
      && item.child_run_id === childRunID,
    )
    if (effect) return { step, effect }
  }
  return null
}

// buildBoundary 从父 Run 的 Trace 里找到调度该子 Run 的 effect，重建步入时的边界（URL 直达子 Run 时使用）。
export function buildBoundary(parent: Pick<TraceRunContext, "run" | "trace">, childRunID: string): WorkflowBoundary {
  const { run: parentRun, trace } = parent
  const match = findChildEffect(trace, childRunID)
  if (!match) throw new Error(`child ${childRunID} has no parent Trace effect`)
  const entryNavigation = snapshotForStep(trace, match.step)
  const resumeSeq = childResumeSeq(match.effect)
  const resumeStep = resumeSeq == null
    ? match.step
    : trace.steps.find((step) => step.seq === resumeSeq) ?? match.step
  return {
    parentRunID: parentRun.id,
    parentWorkflowID: parentRun.workflow_id,
    childRunID,
    childWorkflowID: match.effect.child_workflow_id ?? childRunID,
    effect: match.effect,
    entryNavigation,
    resumeNavigation: snapshotForStep(trace, resumeStep),
    segments: [childRunID],
  }
}

// workflowPath 是页头面包屑：没有边界时列出 root 的各个 Run Segment；
// 有边界时列出每层父 Workflow（停在调用行）加最深一层子 Workflow 当前的 Segment。
export function workflowPath(
  rootWorkflowID: string,
  boundaries: WorkflowBoundary[],
  rootSegments: string[],
): TraceAncestorFrame[] {
  if (boundaries.length === 0) {
    return rootSegments.map((runID) => ({
      runID,
      workflowID: rootWorkflowID,
      line: 0,
    }))
  }
  const frames: TraceAncestorFrame[] = boundaries.map((boundary) => ({
    runID: boundary.parentRunID,
    workflowID: boundary.parentWorkflowID,
    line: boundary.entryNavigation.line,
  }))
  const deepest = boundaries.at(-1)!
  const activeSegmentRunID = deepest.segments.at(-1) ?? deepest.childRunID
  frames.push({
    runID: activeSegmentRunID,
    workflowID: deepest.childWorkflowID,
    line: 0,
  })
  return frames
}

// URL 恢复到 activeRunID 时裁剪 root 的 Segment 链：已在链上则截到它为止；
// 回到 root 则只剩 root；其余情况（子 Workflow 内）保持原样。
export function rootSegmentsAt(current: string[], activeRunID: string, rootRunID: string): string[] {
  const segmentIndex = current.indexOf(activeRunID)
  if (segmentIndex >= 0) return current.slice(0, segmentIndex + 1)
  return activeRunID === rootRunID ? [rootRunID] : current
}

// URL 恢复到 activeRunID 时裁剪调用栈：保留到包含它的那一层，并把该层的 Segment 链截到它为止；
// 栈里找不到时清空（由页面重新按 parent / continued 关系重建）。
export function boundariesAt(current: WorkflowBoundary[], activeRunID: string, rootRunID: string): WorkflowBoundary[] {
  if (activeRunID === rootRunID) return []
  const boundaryIndex = current.findIndex((boundary) => boundary.segments.includes(activeRunID))
  if (boundaryIndex < 0) return []
  return current.slice(0, boundaryIndex + 1).map((boundary, index) => {
    if (index !== boundaryIndex) return boundary
    const segmentIndex = boundary.segments.indexOf(activeRunID)
    return { ...boundary, segments: boundary.segments.slice(0, segmentIndex + 1) }
  })
}
