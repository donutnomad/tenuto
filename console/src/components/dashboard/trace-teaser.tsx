"use client"

import Link from "next/link"
import { ArrowUpRight, Braces, GitBranch, ScanSearch } from "lucide-react"
import { RunStatusBadge } from "@/components/dashboard/run-status-badge"

interface TraceTeaserProps {
  namespace: string
  runID: string
  workflowID: string
  status: string
  eventCount: number
}

const CODE_SWATCHES = [
  "text-[#ff7b72]",
  "text-[#79c0ff]",
  "text-[#d2a8ff]",
  "text-[#7ee787]",
  "text-[#a5d6ff]",
  "text-[#ffa657]",
]

export function TraceTeaser({
  namespace,
  runID,
  workflowID,
  status,
  eventCount,
}: TraceTeaserProps) {
  return (
    <div className="relative isolate overflow-hidden rounded-lg border border-border bg-[#1e1f22] shadow-sm">
      <div aria-hidden="true" className="absolute inset-0 overflow-hidden opacity-90 blur-[0.75px] saturate-150">
        <div className="absolute inset-0 grid grid-cols-[auto_1fr] font-wlang-code text-[12px] leading-7">
          <div className="select-none border-r border-[#373737] px-4 py-5 text-right text-[#6e7681]">
            {Array.from({ length: 10 }, (_, index) => <div key={index}>{index + 1}</div>)}
          </div>
          <div className="space-y-0 px-4 py-5">
            {Array.from({ length: 10 }, (_, index) => (
              <div key={index} className="flex items-center gap-2 whitespace-nowrap">
                <span className={CODE_SWATCHES[index % CODE_SWATCHES.length]}>
                  {index % 3 === 0 ? "workflow" : index % 3 === 1 ? "await" : "return"}
                </span>
                <span className="text-[#c9d1d9]">{index % 2 === 0 ? "ExecuteActivity(ctx, input)" : "lane.Go(func(ctx) {"}</span>
                <span className="text-[#a5d6ff]">{index % 2 === 0 ? " // replay" : " })"}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="relative flex min-h-[240px] items-center justify-center bg-[#1e1f22]/65 px-6 py-10 backdrop-blur-md">
        <div className="max-w-xl text-center">
          <div className="mb-4 flex items-center justify-center gap-2 text-[#a8adbd]">
            <ScanSearch className="size-4 text-[#79c0ff]" />
            <span className="text-xs font-semibold uppercase tracking-[0.18em]">Trace Debug Console</span>
          </div>
          <h2 className="text-lg font-semibold text-[#f0f1f3]">在 Trace 调试台中打开</h2>
          <p className="mt-2 text-sm leading-6 text-[#a8adbd]">
            使用执行线、调用栈、变量快照和断点逐步检查这个 workflow run。
          </p>
          <div className="mt-5 flex flex-wrap items-center justify-center gap-2 text-xs text-[#a8adbd]">
            <span className="inline-flex items-center gap-1.5 rounded border border-[#4e5157] bg-[#2b2b2b]/80 px-2.5 py-1.5 font-mono">
              <Braces className="size-3.5 text-[#7ee787]" />
              {workflowID}
            </span>
            <span className="inline-flex items-center gap-1.5 rounded border border-[#4e5157] bg-[#2b2b2b]/80 px-2.5 py-1.5">
              <GitBranch className="size-3.5 text-[#d2a8ff]" />
              {eventCount} events
            </span>
            <RunStatusBadge status={status} />
          </div>
          <Link
            href={`/${namespace}/runs/${runID}/trace`}
            className="mt-6 inline-flex items-center gap-2 rounded-md bg-[#2f81f7] px-4 py-2 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-[#4693ff] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#79c0ff] focus-visible:ring-offset-2 focus-visible:ring-offset-[#1e1f22]"
          >
            跳转到 Trace
            <ArrowUpRight className="size-4" />
          </Link>
        </div>
      </div>
    </div>
  )
}
