"use client"

import { useState } from "react"
import Link from "next/link"
import type { Run } from "@/lib/types"
import { formatDateTimeColumn, formatDuration } from "@/lib/format"
import { isTerminalRunStatus, runDurationMs } from "@/lib/run-detail"
import { useNow } from "@/lib/use-now"
import { cn } from "@/lib/utils"
import { Check, Copy } from "lucide-react"

// Run 详情页的 Metadata 区 — Temporal DetailList: 紧凑 key-value，每列 key 宽自适应，无边框。
export function RunMetadata({
  run,
  namespace,
  runEndTime,
  taskQueue,
  historySize,
  stateTransitions,
  executionTimeout,
  runTimeout,
}: {
  run: Run
  namespace: string
  runEndTime: string | null
  taskQueue: string | null
  historySize: string
  stateTransitions: number
  executionTimeout?: string
  runTimeout?: string
}) {
  const isTerminal = isTerminalRunStatus(run.status)
  return (
    <div className="flex flex-col gap-x-12 gap-y-1 py-2 sm:flex-row sm:flex-wrap">
      <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-4 gap-y-1 content-start">
        <MetaRow label="Start" value={formatDateTimeColumn(run.created_at)} />
        <MetaRow label="End" value={isTerminal && runEndTime ? formatDateTimeColumn(runEndTime) : "-"} />
        <RunDurationRow run={run} runEndTime={runEndTime} />
      </dl>
      <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-4 gap-y-1 content-start min-w-0">
        <MetaRow label="Run ID" value={run.id} mono copyable />
        <MetaRow
          label="Workflow ID"
          value={run.workflow_id}
          mono
          href={`/${namespace}/runs?workflow_id=${encodeURIComponent(run.workflow_id)}`}
        />
        <MetaRow label="Workflow Type" value={run.workflow_type || "-"} mono />
        <MetaRow
          label="Task Queue"
          value={taskQueue ?? "default"}
          mono
          href={`/${namespace}/task-queues`}
        />
      </dl>
      <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-4 gap-y-1 content-start">
        <MetaRow label="History Size" value={historySize} />
        <MetaRow label="State Transitions" value={String(stateTransitions)} />
        <MetaRow label="Namespace" value={run.namespace} />
      </dl>
      <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-4 gap-y-1 content-start">
        <MetaRow label="Execution Timeout" value={executionTimeout || "Unlimited"} />
        <MetaRow label="Run Timeout" value={runTimeout || "Unlimited"} />
      </dl>
    </div>
  )
}

// Duration 行：运行中的 Run 每秒更新，时钟只在这一行里。
function RunDurationRow({ run, runEndTime }: { run: Run; runEndTime: string | null }) {
  const nowMs = useNow(run.status === "running")
  const duration = runDurationMs(run, runEndTime, nowMs)
  return <MetaRow label="Duration" value={duration != null ? formatDuration(duration) : "-"} />
}

function MetaRow({
  label,
  value,
  mono,
  copyable,
  href,
}: {
  label: string
  value: string
  mono?: boolean
  copyable?: boolean
  href?: string
}) {
  const [copied, setCopied] = useState(false)
  const valueClass = cn("text-left text-[13px] leading-snug text-foreground", mono && "font-mono text-sm")
  return (
    <div className="contents">
      <dt className="col-[1] text-[13px] leading-snug text-foreground/70">
        {label}
      </dt>
      <dd className="col-[2] flex items-center gap-1.5 min-w-0">
        {href && value !== "-" ? (
          <Link href={href} className={cn(valueClass, "truncate text-primary hover:underline")}>
            {value}
          </Link>
        ) : (
          <span className={cn(valueClass, "truncate")}>{value}</span>
        )}
        {copyable && value !== "-" && (
          <button
            type="button"
            className="shrink-0 text-muted-foreground/50 hover:text-foreground transition-colors"
            aria-label={copied ? `${label} copied` : `Copy ${label}`}
            title={copied ? "Copied" : `Copy ${label}`}
            onClick={() => {
              void navigator.clipboard.writeText(value)
              setCopied(true)
              window.setTimeout(() => setCopied(false), 1_500)
            }}
          >
            {copied ? <Check className="size-3" /> : <Copy className="size-3" />}
          </button>
        )}
      </dd>
    </div>
  )
}
