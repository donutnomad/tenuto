"use client"

import type { BlockedInfo } from "@/lib/types"
import { formatDuration } from "@/lib/format"
import { useNow } from "@/lib/use-now"
import { AlertTriangle } from "lucide-react"

// Blocked 详情卡：cause / reason / 连续失败次数 / 卡住时长（now - since）。
export function BlockedDetailCard({ blocked }: { blocked: BlockedInfo }) {
  const nowMs = useNow(true)
  const stuckMs = blocked.since && nowMs > 0 ? nowMs - new Date(blocked.since).getTime() : null
  return (
    <div className="rounded-md border border-orange-400/50 bg-orange-50/60 px-4 py-3 dark:border-orange-500/30 dark:bg-orange-500/10">
      <div className="flex items-center gap-2">
        <AlertTriangle className="size-4 text-orange-600 dark:text-orange-400" />
        <h3 className="text-sm font-semibold text-orange-800 dark:text-orange-300">
          Workflow Blocked
        </h3>
      </div>
      <p className="mt-1 text-xs text-orange-700/80 dark:text-orange-300/70">
        This run failed its workflow task and is waiting for a code fix to replay. Status stays{" "}
        <span className="font-mono">running</span> until it recovers or is reset.
      </p>
      <dl className="mt-3 grid grid-cols-[auto_1fr] items-baseline gap-x-4 gap-y-1">
        {blocked.cause && (
          <>
            <dt className="text-[13px] text-foreground/70">Cause</dt>
            <dd className="text-[13px] font-mono text-foreground">{blocked.cause}</dd>
          </>
        )}
        {blocked.reason && (
          <>
            <dt className="text-[13px] text-foreground/70">Reason</dt>
            <dd className="text-[13px] text-foreground break-all">{blocked.reason}</dd>
          </>
        )}
        <dt className="text-[13px] text-foreground/70">Consecutive Failures</dt>
        <dd className="text-[13px] text-foreground tabular-nums">
          {blocked.consecutive_failures ?? 0}
        </dd>
        {stuckMs != null && (
          <>
            <dt className="text-[13px] text-foreground/70">Blocked For</dt>
            <dd className="text-[13px] text-foreground tabular-nums">{formatDuration(stuckMs)}</dd>
          </>
        )}
      </dl>
    </div>
  )
}
