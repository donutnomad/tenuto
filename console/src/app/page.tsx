"use client"

import Link from "next/link"
import useSWR from "swr"
import { api } from "@/lib/api"
import { loadErrorText } from "@/components/dashboard/load-error"
import {
  Card,
  CardHeader,
  CardTitle,
  CardContent,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, Play, GitBranch, Users, Clock, RefreshCw } from "lucide-react"

export default function Home() {
  const { data, error, isLoading, mutate } = useSWR("namespaces", api.listNamespaces)

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <Loader2 className="size-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-3 bg-background px-4">
        <p role="alert" className="max-w-md break-words text-center text-sm text-destructive">{loadErrorText("namespaces", error)}</p>
        <Button variant="outline" size="sm" onClick={() => mutate()}>
          <RefreshCw className="size-3.5" />
          Retry
        </Button>
      </div>
    )
  }

  const namespaces = data?.items ?? []

  return (
    <div className="flex min-h-screen flex-col items-center bg-background px-4 py-16">
      <div className="mb-8 text-center">
        <h1 className="text-xl font-semibold tracking-tight">
          Tenuto Console
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Select a namespace to get started
        </p>
      </div>

      <div className="grid w-full max-w-4xl gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {namespaces.map((ns) => (
          <Link key={ns.name} href={`/${encodeURIComponent(ns.name)}/runs`}>
            <Card className="h-full cursor-pointer transition-colors hover:bg-accent/50">
              <CardHeader>
                <CardTitle className="font-mono text-sm">{ns.name}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground">
                  <div className="flex items-center gap-1.5">
                    <Play className="size-3" />
                    <span>{ns.run_count} runs</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <GitBranch className="size-3" />
                    <span>{ns.workflow_count} workflows</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Users className="size-3" />
                    <span>{ns.worker_count} workers</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Clock className="size-3" />
                    <span>{ns.schedule_count} schedules</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
        {namespaces.length === 0 && (
          <div className="col-span-full rounded-lg border border-dashed px-6 py-12 text-center text-sm text-muted-foreground">
            No namespaces found.
          </div>
        )}
      </div>
    </div>
  )
}
