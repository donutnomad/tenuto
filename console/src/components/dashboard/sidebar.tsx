"use client"

import { useEffect } from "react"
import Link from "next/link"
import { useParams, usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import {
  Play,
  Users,
  Workflow,
  GitBranch,
  Clock,
  Layers,
  Zap,
  Globe2,
} from "lucide-react"

const navItems = [
  { label: "Runs", href: "runs", icon: Play },
  { label: "Definitions", href: "workflows", icon: GitBranch },
  { label: "Workers", href: "workers", icon: Users },
  { label: "Schedules", href: "schedules", icon: Clock },
  { label: "Queues", href: "task-queues", icon: Layers },
  { label: "Batch", href: "batch-operations", icon: Zap },
]

// Sidebar 在 md 及以上常驻；窄屏下隐藏，open 时作为抽屉盖在页面上，点链接、遮罩或按 Esc 关闭。
export function Sidebar({ open, onClose }: { open: boolean; onClose: () => void }) {
  const params = useParams<{ namespace: string }>()
  const pathname = usePathname()
  const namespace = params.namespace ?? "default"

  useEffect(() => {
    if (!open) return
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose()
    }
    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [open, onClose])

  return (
    <>
      {open && <div aria-hidden className="fixed inset-0 z-40 bg-black/40 md:hidden" onClick={onClose} />}
      <aside
        id="console-nav"
        className={cn(
          "w-[145px] shrink-0 flex-col bg-sidebar md:static md:flex",
          open ? "fixed inset-y-0 left-0 z-50 flex" : "hidden",
        )}
      >
        <div className="flex h-14 items-center gap-2.5 px-4">
          <Workflow className="size-5 text-sidebar-primary" />
          <span className="text-sm font-bold tracking-tight text-sidebar-foreground">
            Tenuto
          </span>
        </div>

        <div className="px-4 pb-3">
          <span className="text-xs text-sidebar-foreground/50">
            {namespace}
          </span>
        </div>

        <div className="px-2 pb-2">
          <Link
            href="/"
            onClick={onClose}
            className="flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium text-white transition-colors hover:bg-sidebar-accent/50"
          >
            <Globe2 className="size-4" />
            Namespaces
          </Link>
        </div>

        <nav className="flex flex-1 flex-col gap-0.5 px-2">
          {navItems.map((item) => {
            const href = `/${namespace}/${item.href}`
            const isActive = pathname.startsWith(href)
            return (
              <Link
                key={item.href}
                href={href}
                onClick={onClose}
                className={cn(
                  "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium text-white transition-colors",
                  isActive
                    ? "bg-sidebar-accent"
                    : "hover:bg-sidebar-accent/50"
                )}
              >
                <item.icon className="size-4" />
                {item.label}
              </Link>
            )
          })}
        </nav>

      </aside>
    </>
  )
}
