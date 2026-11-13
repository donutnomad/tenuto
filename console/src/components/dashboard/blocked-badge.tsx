import { AlertTriangle } from "lucide-react"
import { cn } from "@/lib/utils"
import type { BlockedInfo } from "@/lib/types"

// BlockedBadge 是 run 被 Block 卡住的派生标记（橙/红警示色 + AlertTriangle）。
// 仅在 blocked.blocked 为真时渲染——非新 RunStatus，与 status 徽章并列。
export function BlockedBadge({ blocked }: { blocked?: BlockedInfo }) {
  if (!blocked?.blocked) return null

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-sm border px-1.5 h-5 text-xs font-medium",
        "border-orange-400/60 bg-orange-100 text-orange-800",
        "dark:border-orange-500/40 dark:bg-orange-500/15 dark:text-orange-300",
      )}
      title={blocked.reason || "Workflow is blocked waiting for a code fix"}
    >
      <AlertTriangle className="size-3" />
      Blocked
    </span>
  )
}
