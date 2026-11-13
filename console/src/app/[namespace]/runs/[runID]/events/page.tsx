"use client"

import { useState, useMemo } from "react"
import { useParams } from "next/navigation"
import Link from "next/link"
import useSWR from "swr"
import { api, errorMessage } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { Event, Run } from "@/lib/types"
import { REFRESH_INTERVAL_DETAIL } from "@/lib/constants"
import { HISTORY_PAGE_SIZE, historyPageProgress, mergeHistoryPages } from "@/lib/history-pagination"
import { useRunHistoryPages } from "@/lib/use-run-history-pages"
import { formatTimestamp } from "@/lib/format"
import { JsonViewer } from "@/components/dashboard/json-viewer"
import { ActionFeedback } from "@/components/dashboard/action-feedback"
import { decodePayloads, eventTypeOf } from "@/components/dashboard/event-history-table-logic"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import { ArrowLeft, Download, Loader2, Search } from "lucide-react"

function getEventActivityID(event: Event): string {
  for (const [key, val] of Object.entries(event)) {
    if (key === "id" || key === "timestamp" || val == null) continue
    if (typeof val === "object" && "activityId" in val) {
      return (val as { activityId: string }).activityId
    }
  }
  return "-"
}

function getEventDetails(event: Event): Record<string, unknown> | null {
  for (const [key, val] of Object.entries(event)) {
    if (key === "id" || key === "timestamp" || val == null) continue
    if (typeof val === "object") {
      return val as Record<string, unknown>
    }
  }
  return null
}

export default function EventsPage() {
  const params = useParams<{ namespace: string; runID: string }>()
  const namespace = params.namespace ?? "default"
  const runID = params.runID ?? ""

  const [typeFilter, setTypeFilter] = useState("")
  const [expandedRow, setExpandedRow] = useState<number | null>(null)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState<string | null>(null)

  // Run 状态决定事件是否还在增长：运行中按间隔刷新最新一页，终态后停止。
  const { data: run } = useSWR<Run>(["run", namespace, runID], () => api.getRun(namespace, runID), {
    refreshInterval: (data) => (data?.status === "running" ? REFRESH_INTERVAL_DETAIL : 0),
  })
  const { pages, error, size, setSize, isLoading, isValidating, reload } = useRunHistoryPages(
    namespace,
    runID,
    run?.status === "running",
  )
  const events = useMemo(() => mergeHistoryPages(pages), [pages])
  const progress = useMemo(() => historyPageProgress(pages), [pages])

  const filtered = useMemo(() => {
    if (!typeFilter) return events
    const lower = typeFilter.toLowerCase()
    return events.filter((e) => eventTypeOf(e).toLowerCase().includes(lower))
  }, [events, typeFilter])

  async function handleExport() {
    setExporting(true)
    setExportError(null)
    try {
      const allEvents = await api.getHistory(namespace, runID)
      const blob = new Blob([JSON.stringify(allEvents, null, 2)], {
        type: "application/json",
      })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `events-${runID}.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (err) {
      setExportError(`Failed to export event history: ${errorMessage(err, "unknown error")}`)
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Link href={`/${namespace}/runs/${runID}`}>
          <Button variant="ghost" size="icon-sm">
            <ArrowLeft className="size-4" />
          </Button>
        </Link>
        <h2 className="text-sm font-medium">
          Event History
          <span className="ml-2 font-mono text-xs text-muted-foreground">
            {runID}
          </span>
        </h2>
      </div>

      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-2 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Filter by event type..."
            value={typeFilter}
            onChange={(e) =>
              setTypeFilter((e.target as HTMLInputElement).value)
            }
            className="pl-8"
          />
        </div>
        <div className="ml-auto text-xs text-muted-foreground">
          {progress.loaded} of {progress.total} events loaded
        </div>
        <Button variant="outline" size="sm" onClick={handleExport} disabled={exporting}>
          {exporting ? <Loader2 className="size-3.5 animate-spin" /> : <Download className="size-3.5" />}
          Export JSON
        </Button>
      </div>

      <ActionFeedback error={exportError} onDismiss={() => setExportError(null)} />

      {error && <LoadError what="events" error={error} onRetry={() => void reload()} />}

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-16">#</TableHead>
            <TableHead>Event Type</TableHead>
            <TableHead>Activity ID</TableHead>
            <TableHead>Timestamp</TableHead>
            <TableHead className="w-16">Details</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {filtered.length === 0 ? (
            <TableRow>
              <TableCell colSpan={5} className="h-24 text-center">
                {isLoading ? "Loading..." : error && !pages ? "Events are unavailable." : "No matching events."}
              </TableCell>
            </TableRow>
          ) : (
            filtered.map((event) => {
              const eventType = eventTypeOf(event)
              const activityID = getEventActivityID(event)
              const details = getEventDetails(event)
              const isExpanded = expandedRow === event.id

              return (
                <TableRow
                  key={event.id}
                  className="cursor-pointer"
                  onClick={() =>
                    setExpandedRow(isExpanded ? null : event.id)
                  }
                >
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {event.id}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {eventType}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {activityID}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatTimestamp(event.timestamp)}
                  </TableCell>
                  <TableCell>
                    {isExpanded && details ? (
                      <div
                        className="mt-1"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <JsonViewer data={decodePayloads(details)} />
                      </div>
                    ) : details ? (
                      <span className="text-xs text-muted-foreground">
                        ...
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">-</span>
                    )}
                  </TableCell>
                </TableRow>
              )
            })
          )}
        </TableBody>
      </Table>

      {progress.hasMore && (
        <div className="flex justify-center border-t border-border pt-4">
          <Button variant="outline" onClick={() => setSize(size + 1)} disabled={isValidating}>
            {isValidating && <Loader2 className="size-4 animate-spin" />}
            Load older events ({Math.min(HISTORY_PAGE_SIZE, progress.total - progress.loaded)} next)
          </Button>
        </div>
      )}
    </div>
  )
}
