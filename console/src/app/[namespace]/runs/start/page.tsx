"use client"

import { useState, useMemo } from "react"
import { useParams, useRouter } from "next/navigation"
import Link from "next/link"
import useSWR from "swr"
import { api } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import {
  Select,
  SelectTrigger,
  SelectContent,
  SelectItem,
  SelectValue,
} from "@/components/ui/select"
import { ArrowLeft, Play, Loader2 } from "lucide-react"

export default function StartWorkflowPage() {
  const params = useParams<{ namespace: string }>()
  const router = useRouter()
  const namespace = params.namespace ?? "default"

  const [workflowType, setWorkflowType] = useState("")
  const [workflowID, setWorkflowID] = useState("")
  const [inputJSON, setInputJSON] = useState("{}")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const { data: configs } = useSWR(
    ["workflow-configs", namespace],
    () => api.listWorkflowConfigs(namespace),
    { revalidateOnFocus: false }
  )

  const workflowTypes = useMemo(() => {
    if (!configs?.items) return []
    return configs.items.map((c) => c.workflow_id)
  }, [configs])

  const jsonError = useMemo(() => {
    if (!inputJSON.trim()) return null
    try {
      JSON.parse(inputJSON)
      return null
    } catch (e) {
      return (e as Error).message
    }
  }, [inputJSON])

  async function handleSubmit() {
    setError(null)

    if (!workflowType) {
      setError("Please select a workflow type")
      return
    }

    let parsed: Record<string, unknown>
    try {
      parsed = inputJSON.trim() ? JSON.parse(inputJSON) : {}
    } catch {
      setError("Invalid JSON input")
      return
    }

    setLoading(true)
    try {
      const result = await api.startWorkflow(namespace, workflowType, workflowID.trim(), parsed)
      router.push(`/${namespace}/runs/${result.run_id}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start workflow")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Link href={`/${namespace}/runs`}>
          <Button variant="ghost" size="icon-sm">
            <ArrowLeft className="size-4" />
          </Button>
        </Link>
        <h2 className="text-sm font-medium">Start Workflow</h2>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">New Workflow Run</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1.5">
            <label className="text-xs font-medium">Workflow Type</label>
            {workflowTypes.length > 0 ? (
              <Select value={workflowType} onValueChange={(v) => setWorkflowType(v ?? "")}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a workflow type..." />
                </SelectTrigger>
                <SelectContent>
                  {workflowTypes.map((id) => (
                    <SelectItem key={id} value={id}>
                      {id}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : (
              <input
                type="text"
                value={workflowType}
                onChange={(e) => setWorkflowType(e.target.value)}
                className="w-full rounded-md border border-border bg-background px-3 py-1.5 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                placeholder="Enter workflow type..."
              />
            )}
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Workflow ID (optional)</label>
            <input
              type="text"
              value={workflowID}
              onChange={(e) => setWorkflowID(e.target.value)}
              className="w-full rounded-md border border-border bg-background px-3 py-1.5 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
              placeholder="Leave empty to let the server generate one"
            />
            <p className="text-[11px] text-muted-foreground">
              One Workflow ID has one running Run at a time. Signals, Queries and Updates address the Run by this ID.
            </p>
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-medium">Input (JSON)</label>
            <textarea
              value={inputJSON}
              onChange={(e) => setInputJSON(e.target.value)}
              rows={8}
              className="w-full rounded-md border border-border bg-background px-3 py-2 font-mono text-xs leading-relaxed focus:outline-none focus:ring-1 focus:ring-ring resize-y"
              placeholder="{}"
            />
            {jsonError && (
              <p className="text-[11px] text-red-400">{jsonError}</p>
            )}
          </div>

          {error && (
            <div className="rounded-md bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {error}
            </div>
          )}

          <div className="flex justify-end">
            <Button
              onClick={handleSubmit}
              disabled={loading || !workflowType || !!jsonError}
            >
              {loading ? (
                <Loader2 className="size-3.5 animate-spin" />
              ) : (
                <Play className="size-3.5" />
              )}
              Start Workflow
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
