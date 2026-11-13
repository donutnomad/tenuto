"use client"

import { useParams } from "next/navigation"
import useSWR from "swr"
import { api } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { TaskQueueInfo } from "@/lib/types"
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

const TASK_QUEUE_REFRESH_INTERVAL = 5000

export default function TaskQueuesPage() {
  const params = useParams<{ namespace: string }>()
  const namespace = params.namespace ?? "default"

  const { data, isLoading, error, mutate } = useSWR(
    ["task-queues", namespace],
    () => api.listTaskQueues(namespace),
    { refreshInterval: TASK_QUEUE_REFRESH_INTERVAL }
  )

  const queues: TaskQueueInfo[] = data?.items ?? []

  return (
    <div className="space-y-4">
      {error && <LoadError what="task queues" error={error} onRetry={() => void mutate()} />}

      {isLoading && !data && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="size-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {data && (
        <>
          <h1 className="text-lg font-semibold text-foreground">
            {data.total} Task Queue{data.total !== 1 ? "s" : ""}
          </h1>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Namespace</TableHead>
                <TableHead>Active Workers</TableHead>
                <TableHead>Worker IDs</TableHead>
                <TableHead>Activity Types</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {queues.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center">
                    No task queues found
                  </TableCell>
                </TableRow>
              ) : (
                queues.map((q) => (
                  <TableRow key={q.name}>
                    <TableCell className="font-mono text-xs">
                      {q.name}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {q.namespace}
                    </TableCell>
                    <TableCell className="text-xs">
                      {q.active_workers}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {(q.worker_ids ?? []).map((id) => (
                          <Badge key={id} variant="secondary" className="text-[10px]">
                            {id}
                          </Badge>
                        ))}
                        {(q.worker_ids ?? []).length === 0 && (
                          <span className="text-xs text-muted-foreground">-</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {(q.activity_types ?? []).map((t) => (
                          <Badge key={t} variant="outline" className="text-[10px]">
                            {t}
                          </Badge>
                        ))}
                        {(q.activity_types ?? []).length === 0 && (
                          <span className="text-xs text-muted-foreground">-</span>
                        )}
                      </div>
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
