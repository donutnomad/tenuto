import Link from "next/link"
import { ArrowRight, MoreHorizontal, RotateCcw } from "lucide-react"
import { buildRunChainItems } from "@/lib/run-chain"

interface RunChainProps {
  viewingRunID: string
  firstRunID: string
  previousRunID?: string
  nextRunID?: string
  resetRunID?: string
  isActive: boolean
  namespace: string
}

export function RunChain({
  viewingRunID,
  firstRunID,
  previousRunID,
  nextRunID,
  resetRunID,
  isActive,
  namespace,
}: RunChainProps) {
  const items = buildRunChainItems({
    viewingRunID,
    firstRunID,
    previousRunID,
    nextRunID,
    isActive,
  })

  if (items.length <= 1 && !resetRunID) return null

  return (
    <div className="space-y-2 overflow-x-auto rounded-md border border-border bg-card px-3 py-2.5 text-sm">
      <div className="flex min-w-max items-center justify-between gap-6">
        <span className="font-medium text-foreground">Run chain</span>
        <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
          Earlier
          <ArrowRight className="size-3" />
          Later
        </span>
      </div>

      <div className="flex min-w-max items-center gap-2">
        {items.map((item, index) => (
          <div key={item.kind === "run" ? item.id : `ellipsis-${index}`} className="flex items-center gap-2">
            {index > 0 && <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />}
            {item.kind === "ellipsis" ? (
              <span
                className="flex size-7 items-center justify-center rounded bg-muted/40 text-muted-foreground"
                title="Intermediate runs"
              >
                <MoreHorizontal className="size-4" />
              </span>
            ) : (
              <div className="space-y-1">
                <div className="flex items-center gap-1">
                  {item.badges.map((badge) => (
                    <span
                      key={badge}
                      className={badge === "Active"
                        ? "rounded bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-emerald-700"
                        : "rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground"}
                    >
                      {badge}
                    </span>
                  ))}
                </div>
                {item.id === viewingRunID ? (
                  <span className="block rounded bg-primary/15 px-2 py-1 font-mono font-medium text-primary">
                    {item.id}
                  </span>
                ) : (
                  <Link
                    href={`/${namespace}/runs/${item.id}`}
                    className="block rounded px-2 py-1 font-mono text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                  >
                    {item.id}
                  </Link>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {resetRunID && resetRunID !== viewingRunID && (
        <div className="flex min-w-max items-center gap-2 border-t border-border pt-2 text-xs">
          <RotateCcw className="size-3.5 text-muted-foreground" />
          <span className="text-muted-foreground">Reset successor</span>
          <ArrowRight className="size-3 text-muted-foreground" />
          <Link
            href={`/${namespace}/runs/${resetRunID}`}
            className="rounded px-1.5 py-0.5 font-mono text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            {resetRunID}
          </Link>
        </div>
      )}
    </div>
  )
}
