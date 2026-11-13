"use client"

import { useParams } from "next/navigation"
import useSWR from "swr"
import { api } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { WorkerListItem } from "@/lib/types"
import { timeAgo } from "@/lib/format"
import { stickyCacheSummary } from "@/lib/worker-cache"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import { Loader2 } from "lucide-react"

const WORKER_REFRESH_INTERVAL = 5000

export default function WorkersPage() {
  const params = useParams<{ namespace: string }>()
  const namespace = params.namespace ?? "default"

  const { data, isLoading, error, mutate } = useSWR(
    ["workers", namespace],
    () => api.listWorkers(namespace),
    { refreshInterval: WORKER_REFRESH_INTERVAL }
  )

  const workers: WorkerListItem[] = data?.items ?? []

  return (
    <div className="space-y-4">
      {error && <LoadError what="workers" error={error} onRetry={() => void mutate()} />}

      {isLoading && !data && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="size-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {data && (
        <>
          <h1 className="text-lg font-semibold text-foreground">
            {data.total} Worker{data.total !== 1 ? "s" : ""}
          </h1>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Identity</TableHead>
                <TableHead>Build ID</TableHead>
                <TableHead>Task Queues</TableHead>
                <TableHead>Activity Types</TableHead>
                <TableHead>Default</TableHead>
                <TableHead>Weight</TableHead>
                <TableHead>Sticky Cache</TableHead>
                <TableHead>Last Heartbeat</TableHead>
                <TableHead>Expires</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {workers.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="h-24 text-center">
                    No workers registered
                  </TableCell>
                </TableRow>
              ) : (
                workers.map((w) => (
                  <TableRow key={w.identity ?? w.build_id}>
                    <TableCell className="font-mono text-xs">
                      {w.identity ?? "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {w.build_id ?? "-"}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {(w.task_queues ?? []).map((q) => (
                          <Badge key={q} variant="secondary" className="text-[10px]">
                            {q}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {(w.activity_types ?? []).map((t) => (
                          <Badge key={t} variant="outline" className="text-[10px]">
                            {t}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell>
                      {w.default ? (
                        <Badge variant="default" className="text-[10px]">
                          Default
                        </Badge>
                      ) : (
                        <span className="text-xs text-muted-foreground">-</span>
                      )}
                    </TableCell>
                    <TableCell className="text-xs">{w.weight ?? "-"}</TableCell>
                    <TableCell className="text-xs tabular-nums">
                      {(() => {
                        const cache = stickyCacheSummary(w.sticky_cache)
                        return cache ? (
                          <div className="space-y-0.5">
                            <div>{cache.usage}</div>
                            <div className="text-muted-foreground">{cache.lookups}</div>
                          </div>
                        ) : (
                          <span className="text-muted-foreground">-</span>
                        )
                      })()}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {w.last_heartbeat_at ? timeAgo(w.last_heartbeat_at) : "-"}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {w.expires_at ? timeAgo(w.expires_at) : "-"}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </>
      )}
    </div>
  )
}
