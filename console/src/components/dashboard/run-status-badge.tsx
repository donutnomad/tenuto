import { cn } from "@/lib/utils"

// Temporal UI 状态色：浅色背景 + 黑字 + 方角（bg-*-200/300 + text-black + rounded-sm）
const STATUS_STYLES: Record<string, string> = {
  running: "bg-blue-300",
  completed: "bg-green-200",
  open: "bg-green-200",
  failed: "bg-red-200",
  retrying: "bg-red-200",
  cancelled: "bg-slate-200",
  canceled: "bg-slate-200",
  terminated: "bg-yellow-200",
  paused: "bg-yellow-200",
  cancel_requested: "bg-yellow-200",
  continued_as_new: "bg-purple-200",
  pending: "bg-purple-200",
  scheduled: "bg-blue-300",
  timed_out: "bg-orange-200",
  signaled: "bg-pink-200",
  fired: "bg-pink-200",
}

export function RunStatusBadge({ status, big }: { status: string; big?: boolean }) {
  const bg = STATUS_STYLES[status] ?? "bg-slate-200"
  const label = status.replace(/_/g, " ")
  const isRunning = status === "running" || status === "scheduled"

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-sm font-medium capitalize text-black",
        big ? "h-8 px-4 text-base" : "h-5 px-1.5 text-xs",
        bg
      )}
    >
      {isRunning && (
        <span className="relative flex size-1.5">
          <span className="absolute inline-flex size-full animate-ping rounded-full bg-black/50 opacity-75" />
          <span className="relative inline-flex size-1.5 rounded-full bg-black/70" />
        </span>
      )}
      {label}
    </span>
  )
}
