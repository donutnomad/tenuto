import type { ReactNode } from "react"
import { X } from "lucide-react"

// ActionFeedback 显示一次写操作的结果：失败用 role="alert"，成功用 role="status"。
export function ActionFeedback({
  error,
  success,
  onDismiss,
}: {
  error?: ReactNode
  success?: ReactNode
  onDismiss?: () => void
}) {
  if (!error && !success) return null
  const dismiss = onDismiss && (
    <button
      type="button"
      aria-label="Dismiss"
      onClick={onDismiss}
      className="shrink-0 rounded p-0.5 opacity-70 hover:opacity-100"
    >
      <X className="size-3.5" />
    </button>
  )
  if (error) {
    return (
      <div role="alert" className="flex items-start justify-between gap-3 rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
        <div className="min-w-0 break-words">{error}</div>
        {dismiss}
      </div>
    )
  }
  return (
    <div role="status" className="flex items-start justify-between gap-3 rounded-md bg-emerald-500/10 px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
      <div className="min-w-0 break-words">{success}</div>
      {dismiss}
    </div>
  )
}
