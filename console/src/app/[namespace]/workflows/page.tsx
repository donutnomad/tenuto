"use client"

import { useState } from "react"
import { useParams } from "next/navigation"
import useSWR from "swr"
import { api } from "@/lib/api"
import { LoadError } from "@/components/dashboard/load-error"
import type { WorkflowConfigSummary } from "@/lib/types"
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table"
import { Loader2 } from "lucide-react"

export default function WorkflowsPage() {
  const params = useParams<{ namespace: string }>()
  const namespace = params.namespace ?? "default"

  const { data, isLoading, error, mutate } = useSWR(
    ["workflow-configs", namespace],
    () => api.listWorkflowConfigs(namespace),
    { refreshInterval: 5000 },
  )

  const { data: workers } = useSWR(["workers", namespace], () => api.listWorkers(namespace), { refreshInterval: 5000 })
  const [saving, setSaving] = useState("")
  const [saveError, setSaveError] = useState("")
  async function setTarget(workflowID: string, identity: string) {
    setSaving(workflowID)
    setSaveError("")
    try { await api.setWorkerTarget(namespace, workflowID, identity); await mutate() }
    catch (error) { setSaveError(error instanceof Error ? error.message : String(error)) }
    finally { setSaving("") }
  }
  const identities = [...new Set((workers?.items ?? []).filter((w) => (w.activity_types ?? []).length > 0).map((w) => w.identity).filter((id): id is string => Boolean(id)))]
  const items: WorkflowConfigSummary[] = data?.items ?? []

  return (
    <div className="space-y-5">
      <p className="text-sm text-muted-foreground">Activity worker targets apply to new runs. Existing runs and retries retain their original target. Workflow orchestration uses the shared workflow workers.</p>
      {saveError && <div role="alert" className="text-sm text-red-600">{saveError}</div>}
      {error && <LoadError what="workflow definitions" error={error} onRetry={() => void mutate()} />}

      {isLoading && !data && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="size-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {data && (
        <>
          <h1 className="text-lg font-semibold text-foreground">
            {data.total} Definition{data.total !== 1 ? "s" : ""}
          </h1>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Workflow ID</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Latest Version</TableHead>
                <TableHead>Versions</TableHead>
                <TableHead>Activities</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Activity Worker Target</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-24 text-center">
                    No workflows registered.
                  </TableCell>
                </TableRow>
              ) : (
                items.map((wf) => (
                  <TableRow key={wf.workflow_id}>
                    <TableCell className="font-mono text-xs">
                      {wf.workflow_id}
                    </TableCell>
                    <TableCell className="text-sm">
                      {wf.name || "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {wf.latest_version}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {wf.version_count}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {wf.activity_count}
                    </TableCell>
                    <TableCell>
                      {wf.active ? (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-50 px-2.5 py-0.5 text-xs font-medium text-emerald-700 ring-1 ring-emerald-200">
                          <span className="size-1.5 rounded-full bg-emerald-500 shrink-0" />
                          Active
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-slate-50 px-2.5 py-0.5 text-xs font-medium text-slate-500 ring-1 ring-slate-200">
                          <span className="size-1.5 rounded-full bg-slate-400 shrink-0" />
                          Inactive
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      <select aria-label={`Activity worker target for ${wf.workflow_id}`} value={wf.target_worker_identity ?? ""} disabled={saving === wf.workflow_id} onChange={(event) => void setTarget(wf.workflow_id, event.target.value)} className="max-w-64 rounded border bg-background px-2 py-1 text-xs">
                        <option value="">Default scheduling</option>
                        {wf.target_worker_identity && !identities.includes(wf.target_worker_identity) && <option value={wf.target_worker_identity}>{wf.target_worker_identity} (offline)</option>}
                        {identities.map((id) => <option key={id} value={id}>{id}</option>)}
                      </select>
                      <div className="mt-1 text-xs text-muted-foreground">{wf.target_worker_status ?? "default"}{saving === wf.workflow_id ? " · saving" : ""}</div>
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
