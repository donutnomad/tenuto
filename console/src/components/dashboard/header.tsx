"use client"

import { useState } from "react"
import { useParams, useRouter } from "next/navigation"
import { Menu, Search } from "lucide-react"

// onOpenNav 打开窄屏下的导航抽屉；菜单按钮只在 md 以下显示。
export function Header({ navOpen, onOpenNav }: { navOpen: boolean; onOpenNav: () => void }) {
  const params = useParams<{ namespace: string }>()
  const router = useRouter()
  const namespace = params.namespace ?? "default"
  const [runIdQuery, setRunIdQuery] = useState("")

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      const trimmed = runIdQuery.trim()
      if (!trimmed) return
      router.push(`/${namespace}/runs/${encodeURIComponent(trimmed)}`)
      setRunIdQuery("")
    }
  }

  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b border-border bg-background px-4 md:px-6">
      <button
        type="button"
        onClick={onOpenNav}
        aria-label="Open navigation"
        aria-controls="console-nav"
        aria-expanded={navOpen}
        className="-ml-1 rounded-md p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground md:hidden"
      >
        <Menu className="size-4" />
      </button>
      <div className="ml-auto relative min-w-0">
        <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
        <input
          type="text"
          value={runIdQuery}
          onChange={(e) => setRunIdQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Go to Run ID..."
          className="h-8 w-44 sm:w-56 rounded-md border border-input bg-muted/30 pl-8 pr-3 text-xs outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/30 transition-all"
        />
      </div>
    </header>
  )
}
