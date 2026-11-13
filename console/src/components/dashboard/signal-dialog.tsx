"use client"

import { useState } from "react"
import { api } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Loader2 } from "lucide-react"

export function SignalDialog({
  open,
  onOpenChange,
  namespace,
  runID,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  namespace: string
  runID: string
}) {
  const [name, setName] = useState("")
  const [payload, setPayload] = useState("{}")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)

    if (!name.trim()) {
      setError("Signal name is required")
      return
    }

    let parsed: Record<string, unknown>
    try {
      parsed = JSON.parse(payload)
    } catch {
      setError("Invalid JSON payload")
      return
    }

    setLoading(true)
    try {
      await api.signalWorkflow(namespace, runID, name.trim(), parsed)
      onOpenChange(false)
      setName("")
      setPayload("{}")
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to send signal")
    } finally {
      setLoading(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Send Signal</DialogTitle>
          <DialogDescription>
            Send a signal to the running workflow.
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <label className="text-xs font-medium">Signal Name</label>
            <Input
              placeholder="e.g. approve, cancel, retry"
              value={name}
              onChange={(e) =>
                setName((e.target as HTMLInputElement).value)
              }
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium">Payload (JSON)</label>
            <textarea
              className="flex min-h-[100px] w-full rounded-lg border border-input bg-transparent px-3 py-2 font-mono text-xs transition-colors focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 dark:bg-input/30"
              placeholder='{"key": "value"}'
              value={payload}
              onChange={(e) => setPayload(e.target.value)}
            />
          </div>

          {error && (
            <div className="rounded-md bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {error}
            </div>
          )}

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={loading}>
              {loading && <Loader2 className="size-3.5 animate-spin" />}
              Send Signal
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
