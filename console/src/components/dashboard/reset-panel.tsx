"use client"

import { useMemo, useState } from "react"
import { api } from "@/lib/api"
import type { Event } from "@/lib/types"
import { eventTypeOf, resetAnchorEvents } from "@/components/dashboard/event-history-table-logic"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Select,
  SelectTrigger,
  SelectContent,
  SelectItem,
  SelectValue,
} from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { Loader2, RotateCcw } from "lucide-react"

export function ResetPanel({
  namespace,
  runID,
  events,
  open,
  onOpenChange,
  onReset,
}: {
  namespace: string
  runID: string
  events?: Event[]
  open: boolean
  onOpenChange: (open: boolean) => void
  onReset: (newRunID: string) => void
}) {
  const [selectedEventID, setSelectedEventID] = useState<string>("")
  const [excludeSignals, setExcludeSignals] = useState(false)
  const [excludeUpdates, setExcludeUpdates] = useState(false)
  const [reason, setReason] = useState("")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const resetableEvents = useMemo(() => resetAnchorEvents(events ?? []), [events])

  async function handleReset() {
    setLoading(true)
    setError(null)
    try {
      const excludeTypes: string[] = []
      if (excludeSignals) excludeTypes.push("signal")
      if (excludeUpdates) excludeTypes.push("update")
      const result = await api.resetWorkflow(namespace, runID, {
        reason: reason || "Reset from Tenuto Console",
        reset_event_id: parseInt(selectedEventID, 10),
        reapply_exclude_types: excludeTypes,
      })
      onReset(result.run_id)
    } catch (error) {
      setError(error instanceof Error ? error.message : "Workflow reset failed")
    } finally {
      setLoading(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <RotateCcw className="size-4" />
            Reset Workflow
          </DialogTitle>
          <DialogDescription>
            Create a new run forked from a specific point in this workflow&apos;s history.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Reset Point</label>
            <Select value={selectedEventID} onValueChange={(v) => setSelectedEventID(v ?? "")}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select a workflow task event..." />
              </SelectTrigger>
              <SelectContent>
                {resetableEvents.map((event) => (
                  <SelectItem key={event.id} value={String(event.id)}>
                    {event.id} - {eventTypeOf(event)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {resetableEvents.length === 0 && (
              <p className="text-sm text-muted-foreground">No workflow task boundary events available.</p>
            )}
          </div>

          <div className="space-y-1.5">
            <label className="text-sm font-medium">Reapply Events</label>
            <p className="text-sm text-muted-foreground">
              Events after the reset point are reapplied by default. Exclude specific types below.
            </p>
            <div className="space-y-2 pt-0.5">
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={excludeSignals}
                  onChange={(e) => setExcludeSignals(e.target.checked)}
                  className="accent-primary"
                />
                <span>Exclude Signals</span>
              </label>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={excludeUpdates}
                  onChange={(e) => setExcludeUpdates(e.target.checked)}
                  className="accent-primary"
                />
                <span>Exclude Updates</span>
              </label>
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="text-sm font-medium">Reason (optional)</label>
            <input
              type="text"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              className="w-full rounded-md border border-border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
              placeholder="e.g. fix activity failure"
            />
          </div>

          {error && (
            <div className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">{error}</div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleReset} disabled={loading || !selectedEventID}>
            {loading ? <Loader2 className="size-3.5 animate-spin" /> : <RotateCcw className="size-3.5" />}
            Reset
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
