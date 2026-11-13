import { RefreshCw } from "lucide-react"
import { ServerUnreachableError } from "@/lib/api"
import { Button } from "@/components/ui/button"

// loadErrorText 是读取失败的提示：连不上 Server 时直接说明连接失败，Server 返回的错误带上它的原文。
export function loadErrorText(what: string, error: unknown): string {
  if (error instanceof ServerUnreachableError) return error.message
  const detail = error instanceof Error && error.message ? error.message : String(error)
  return `Failed to load ${what}: ${detail}`
}

// LoadError 显示页面数据读取失败与重试按钮。
export function LoadError({ what, error, onRetry }: { what: string; error: unknown; onRetry: () => void }) {
  return (
    <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
      <span className="min-w-0 break-words">{loadErrorText(what, error)}</span>
      <Button variant="outline" size="sm" onClick={onRetry}>
        <RefreshCw className="size-3.5" />
        Retry
      </Button>
    </div>
  )
}
