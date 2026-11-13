"use client"

import { useMemo, useState } from "react"
import Link from "next/link"
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  GitBranch,
  Loader2,
  Network,
  RotateCcw,
} from "lucide-react"
import { api } from "@/lib/api"
import {
  buildRunRelationships,
  groupRunRelationships,
  relationshipExpansionGuard,
  type RunRelationship,
  type RunRelationshipKind,
} from "@/lib/run-relationships"
import type { Event, Run } from "@/lib/types"
import { cn } from "@/lib/utils"
import { RunStatusBadge } from "@/components/dashboard/run-status-badge"

export interface RelationshipRunContext {
  run: Run
  events: Event[]
}

export type RelationshipContextLoader = (runID: string) => Promise<RelationshipRunContext>

export interface RelationshipsPanelProps {
  run: Run
  events: Event[]
  namespace: string
  maxDepth?: number
  loadContext?: RelationshipContextLoader
}

export async function loadRelationshipRunContext(namespace: string, runID: string): Promise<RelationshipRunContext> {
  const [run, events] = await Promise.all([api.getRun(namespace, runID), api.getHistory(namespace, runID)])
  return { run, events }
}

const KIND_LABEL: Record<RunRelationshipKind, string> = {
  parent: "Parent",
  child: "Child",
  "continue-as-new": "Continue As New",
  "reset-from": "Reset From",
  "reset-to": "Reset To",
}

const KIND_ICON: Record<RunRelationshipKind, typeof Network> = {
  parent: Network,
  child: GitBranch,
  "continue-as-new": GitBranch,
  "reset-from": RotateCcw,
  "reset-to": RotateCcw,
}

export function RelationshipsPanel({
  run,
  events,
  namespace,
  maxDepth = 8,
  loadContext = (runID) => loadRelationshipRunContext(namespace, runID),
}: RelationshipsPanelProps) {
  const relationships = useMemo(() => buildRunRelationships(run, events), [run, events])

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between rounded-md border border-border bg-muted/20 px-3 py-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="truncate font-medium">{run.workflow_id}</span>
            <RunStatusBadge status={run.status} />
          </div>
          <p className="truncate font-mono text-xs text-muted-foreground">{run.id}</p>
        </div>
        <span className="text-xs text-muted-foreground">Current Run</span>
      </div>

      {relationships.length === 0 ? (
        <p className="py-6 text-center text-sm text-muted-foreground">
          No relationships found for this workflow run.
        </p>
      ) : (
        <RelationshipGroups
          relationships={relationships}
          namespace={namespace}
          ancestry={[run.id]}
          depth={0}
          maxDepth={maxDepth}
          loadContext={loadContext}
        />
      )}
    </div>
  )
}

function RelationshipGroups({
  relationships,
  namespace,
  ancestry,
  depth,
  maxDepth,
  loadContext,
}: {
  relationships: RunRelationship[]
  namespace: string
  ancestry: string[]
  depth: number
  maxDepth: number
  loadContext: RelationshipContextLoader
}) {
  const groups = groupRunRelationships(relationships)
  const sections = [
    { title: "Parent Workflow", items: groups.parents },
    { title: `Child Workflows (${groups.children.length})`, items: groups.children },
    { title: "Continue-As-New Chain", items: groups.continuations },
    { title: "Reset Chain", items: groups.resets },
  ].filter((section) => section.items.length > 0)

  return (
    <div className="space-y-3">
      {sections.map((section) => (
        <section key={section.title} className="space-y-1.5">
          <h4 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            {section.title}
          </h4>
          {section.items.map((relationship) => (
            <RelationshipTreeNode
              key={`${relationship.kind}:${relationship.runID}`}
              relationship={relationship}
              namespace={namespace}
              ancestry={ancestry}
              depth={depth + 1}
              maxDepth={maxDepth}
              loadContext={loadContext}
            />
          ))}
        </section>
      ))}
    </div>
  )
}

function RelationshipTreeNode({
  relationship,
  namespace,
  ancestry,
  depth,
  maxDepth,
  loadContext,
}: {
  relationship: RunRelationship
  namespace: string
  ancestry: string[]
  depth: number
  maxDepth: number
  loadContext: RelationshipContextLoader
}) {
  const [expanded, setExpanded] = useState(false)
  const [context, setContext] = useState<RelationshipRunContext>()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string>()
  const guard = relationshipExpansionGuard(relationship.runID, ancestry, depth, maxDepth)
  const Icon = KIND_ICON[relationship.kind]
  const nestedRelationships = useMemo(
    () => buildRunRelationships(context?.run, context?.events),
    [context],
  )

  async function toggleExpanded() {
    if (!guard.allowed) return
    if (expanded) {
      setExpanded(false)
      return
    }
    setExpanded(true)
    if (context || loading) return
    setLoading(true)
    setError(undefined)
    try {
      setContext(await loadContext(relationship.runID))
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Run could not be loaded")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-1.5">
      <div
        className={cn(
          "flex min-w-0 items-center gap-2 rounded-md border border-border bg-muted/20 px-2 py-2 transition-colors",
          guard.allowed && "hover:bg-muted/40",
        )}
      >
        <button
          type="button"
          className="flex size-6 shrink-0 items-center justify-center rounded text-muted-foreground hover:bg-muted hover:text-foreground disabled:cursor-default disabled:opacity-60"
          disabled={!guard.allowed}
          onClick={toggleExpanded}
          aria-label={`${expanded ? "Collapse" : "Expand"} ${relationship.label}`}
          aria-expanded={guard.allowed ? expanded : undefined}
        >
          {loading ? (
            <Loader2 className="size-3.5 animate-spin" />
          ) : guard.reason === "cycle" ? (
            <AlertTriangle className="size-3.5 text-amber-600" />
          ) : expanded ? (
            <ChevronDown className="size-4" />
          ) : (
            <ChevronRight className="size-4" />
          )}
        </button>

        <Icon className="size-3.5 shrink-0 text-muted-foreground" />
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center gap-2">
            <span className="shrink-0 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              {KIND_LABEL[relationship.kind]}
            </span>
            <span className="truncate text-sm font-medium">
              {context?.run.workflow_id || relationship.workflowID || relationship.label}
            </span>
            {context?.run.status && <RunStatusBadge status={context.run.status} />}
          </div>
          <p className="truncate font-mono text-xs text-muted-foreground">{relationship.runID}</p>
        </div>

        <Link
          href={`/${namespace}/runs/${relationship.runID}`}
          className="flex size-7 shrink-0 items-center justify-center rounded text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label={`Open run ${relationship.runID}`}
        >
          <ExternalLink className="size-3.5" />
        </Link>
      </div>

      {guard.reason === "cycle" && (
        <p className="ml-8 text-xs text-amber-600">Cycle detected at {relationship.runID}</p>
      )}
      {guard.reason === "max-depth" && (
        <p className="ml-8 text-xs text-muted-foreground">Maximum relationship depth reached.</p>
      )}
      {expanded && error && (
        <p className="ml-8 text-xs text-destructive">Run unavailable: {error}</p>
      )}
      {expanded && context && nestedRelationships.length === 0 && (
        <p className="ml-8 text-xs text-muted-foreground">No further relationships.</p>
      )}
      {expanded && context && nestedRelationships.length > 0 && (
        <div className="ml-5 border-l border-border pl-3">
          <RelationshipGroups
            relationships={nestedRelationships}
            namespace={namespace}
            ancestry={[...ancestry, relationship.runID]}
            depth={depth}
            maxDepth={maxDepth}
            loadContext={loadContext}
          />
        </div>
      )}
    </div>
  )
}
