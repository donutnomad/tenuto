"use client"

import { useEffect, useMemo, useRef } from "react"
import { cn } from "@/lib/utils"
import type { RunStatus } from "@/lib/types"
import type { ActivityInvocationRecord } from "@/lib/activity-invocations"
import { centeredScrollTop } from "./trace-debugger-logic"
import { tokenizeWlangLine, type WlangTokenKind } from "./wlang-syntax"
import { CheckCircle2, ChevronRight as ChevronRightCollapse, Circle, Pause, XCircle } from "lucide-react"

const WLANG_TOKEN_COLOR: Record<WlangTokenKind, string> = {
  plain: "text-[#c9d1d9]",
  comment: "text-[#8b949e] italic",
  keyword: "text-[#ff7b72]",
  namespace: "text-[#79c0ff]",
  function: "text-[#d2a8ff]",
  type: "text-[#7ee787]",
  string: "text-[#a5d6ff]",
  number: "text-[#ffa657]",
  constant: "text-[#ff7b72]",
  operator: "text-[#8b949e]",
}

function WlangCodeLine({ line }: { line: string }) {
  return (
    <>
      {tokenizeWlangLine(line).map((token, index) => (
        <span key={index} className={WLANG_TOKEN_COLOR[token.kind]}>
          {token.text}
        </span>
      ))}
    </>
  )
}

// Trace 调试器的源码区：行号/断点栏、当前行与已执行行高亮、行内 Activity 入口，
// 以及末尾的执行前沿哨兵和终态停靠行。当前行变化时滚动到视口中间。
export function CodePane({
  pseudocode,
  currentLine,
  focusLane,
  terminalStop,
  executedLines,
  breakpoints,
  activitiesByLine,
  sentinel,
  runStatus,
  runKind,
  boundaryHint,
  fullScreen,
  onToggleBreakpoint,
  onSelectActivity,
}: {
  pseudocode: string
  currentLine: number
  focusLane: string
  terminalStop: boolean
  executedLines: ReadonlySet<number>
  breakpoints: ReadonlySet<number>
  activitiesByLine: ReadonlyMap<number, ActivityInvocationRecord[]>
  // 聚焦 lane 停在 pending 前沿时的说明文字。
  sentinel: string | null
  runStatus?: RunStatus
  runKind: string
  // 终态停靠行上的跨边界提示；没有可跨越的边界时为 null。
  boundaryHint: string | null
  fullScreen: boolean
  onToggleBreakpoint: (line: number) => void
  onSelectActivity: (line: number, scheduledEventID: number) => void
}) {
  const currentLineRef = useRef<HTMLDivElement>(null)
  const codeScrollRef = useRef<HTMLDivElement>(null)
  const lines = useMemo(() => pseudocode.split("\n"), [pseudocode])

  useEffect(() => {
    const container = codeScrollRef.current
    const line = currentLineRef.current
    if (!container || !line) return
    const containerRect = container.getBoundingClientRect()
    const lineRect = line.getBoundingClientRect()
    container.scrollTo({
      top: centeredScrollTop({
        containerTop: containerRect.top,
        containerScrollTop: container.scrollTop,
        containerHeight: container.clientHeight,
        lineTop: lineRect.top,
        lineHeight: lineRect.height,
      }),
      behavior: "auto",
    })
  }, [currentLine, focusLane, terminalStop])

  return (
    <div className={cn("order-1 flex min-h-[320px] flex-none", fullScreen ? "min-h-0 flex-1" : "h-[min(480px,60vh)]")}>
      <div className="relative min-w-0 flex-1 bg-[#2B2B2B]">
        <div className="pointer-events-none absolute inset-y-0 left-20 z-20 w-px bg-[#373737]" />
        <div ref={codeScrollRef} className="h-full overflow-auto">
          <div className="min-w-max py-2">
          {lines.map((line, i) => {
            const isCurrent = !terminalStop && i === currentLine
            const isExecuted = !isCurrent && executedLines.has(i)
            const hasBreakpoint = breakpoints.has(i)
            const lineActivities = activitiesByLine.get(i) ?? []
            const lineAttemptCount = lineActivities.reduce((count, invocation) => count + invocation.attempts.length, 0)
            return (
              <div
                key={i}
                ref={isCurrent ? currentLineRef : undefined}
                className={cn(
                  "font-wlang-code group flex min-w-full items-center text-xs leading-6 hover:bg-[#34363b]",
                  isCurrent && "bg-[#2F3F55] border-l-4 border-[#58a6ff]",
                  !isCurrent && isExecuted && "bg-[#10251a] border-l-4 border-transparent",
                  !isCurrent && !isExecuted && "border-l-4 border-transparent",
                )}
                >
                  <div
                  className={cn(
                    "sticky left-0 z-10 flex w-[76px] shrink-0 items-center px-1",
                    isCurrent && "bg-[#2F3F55]",
                    isExecuted && "bg-[#10251a]",
                    !isCurrent && !isExecuted && "bg-[#2B2B2B] group-hover:bg-[#34363b]",
                  )}
                  >
                  <button
                    type="button"
                    onClick={() => onToggleBreakpoint(i)}
                    className="relative flex h-6 w-8 shrink-0 items-center justify-end pr-1 text-right transition-colors hover:bg-[#34363b]"
                    title={hasBreakpoint ? `清除断点 (行 ${i + 1})` : `设置断点 (行 ${i + 1})`}
                  >
                    <span className={cn("select-none text-[#6e7681]", hasBreakpoint && "text-transparent")}>
                      {i + 1}
                    </span>
                    {hasBreakpoint && (
                      <Circle className="absolute right-0.5 top-1/2 size-4 -translate-y-1/2 fill-[#f85149] text-[#f85149]" />
                    )}
                  </button>
                  <span className="flex w-4 shrink-0 items-center justify-center">
                    {isCurrent && (
                      <ChevronRightCollapse className="size-3 text-[#58a6ff]" />
                    )}
                  </span>
                  </div>
                <code className="flex-1 whitespace-pre px-2">
                  <WlangCodeLine line={line} />
                </code>
                {lineActivities.length > 0 && (
                  <button
                    type="button"
                    onClick={() => onSelectActivity(i, lineActivities[0].scheduledEventID)}
                    className="sticky right-2 ml-8 mr-2 inline-flex h-5 shrink-0 items-center gap-1.5 rounded border border-[#4e5157] bg-[#303033] px-2 text-[10px] text-[#a8adbd] hover:bg-[#393b40] hover:text-[#dfe1e5]"
                    title="查看 Activity 调用与 attempts"
                  >
                    <span className="text-[#d2a8ff]">Activity</span>
                    <span>·</span>
                    <span>{lineActivities.length === 1 ? lineActivities[0].status : `${lineActivities.length} calls`}</span>
                    {lineAttemptCount > 1 && <span className="text-[#6897bb]">{lineAttemptCount} attempts</span>}
                  </button>
                )}
              </div>
            )
          })}
          {/* pending 前沿哨兵：聚焦 pending-frontier lane 且停在末尾时显示 */}
          {sentinel && (
            <div className="mx-2 mt-1 flex items-start gap-2 rounded-md border border-[#9e6a03] bg-[#2d220f] px-3 py-2 text-xs text-[#e3b341]">
              <Pause className="mt-0.5 size-3.5 shrink-0 fill-[#d29922] text-[#d29922]" />
              <span className="leading-relaxed">{sentinel}</span>
            </div>
          )}
          {terminalStop && (
            <div
              ref={currentLineRef}
              className="mx-2 mt-1 flex items-center gap-2 rounded border border-[#4e5157] bg-[#252628] px-3 py-2 font-mono text-xs"
            >
              {(runStatus === "failed" || runStatus === "cancelled" || runStatus === "terminated") ? (
                <XCircle className="size-4 shrink-0 text-[#cf6679]" />
              ) : (
                <CheckCircle2 className="size-4 shrink-0 text-[#6aab73]" />
              )}
              <span className="text-[#dfe1e5]">Workflow {runStatus ?? runKind}</span>
              {boundaryHint && <span className="text-[#7a7e85]">{boundaryHint}</span>}
            </div>
          )}
          </div>
        </div>
      </div>
    </div>
  )
}
