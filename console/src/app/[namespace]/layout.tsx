"use client"

import { useCallback, useState } from "react"
import { usePathname } from "next/navigation"
import { Sidebar } from "@/components/dashboard/sidebar"
import { Header } from "@/components/dashboard/header"

export default function NamespaceLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const pathname = usePathname()
  const isTraceConsole = /\/runs\/[^/]+\/trace\/?$/.test(pathname ?? "")
  const [navOpen, setNavOpen] = useState(false)
  const closeNav = useCallback(() => setNavOpen(false), [])

  if (isTraceConsole) {
    return <div className="h-dvh overflow-hidden bg-[#1e1f22]">{children}</div>
  }

  return (
    <div className="flex h-full">
      <Sidebar open={navOpen} onClose={closeNav} />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <Header navOpen={navOpen} onOpenNav={() => setNavOpen(true)} />
        <main className="min-w-0 flex-1 overflow-auto p-4 md:p-6">{children}</main>
      </div>
    </div>
  )
}
