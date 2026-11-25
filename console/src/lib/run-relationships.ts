import type { Event, Run } from "./types"

export type RunRelationshipKind =
  | "parent"
  | "child"
  | "continue-as-new"
  | "reset-from"
  | "reset-to"

export interface RunRelationship {
  kind: RunRelationshipKind
  runID: string
  label: string
  workflowType?: string
  eventID?: number
}

export interface RelationshipExpansionGuard {
  allowed: boolean
  reason?: "cycle" | "max-depth"
}

function pushUnique(target: RunRelationship[], relationship: RunRelationship) {
  if (!relationship.runID || target.some((item) => item.kind === relationship.kind && item.runID === relationship.runID)) {
    return
  }
  target.push(relationship)
}

export function buildRunRelationships(run: Run | null | undefined, events: Event[] = []): RunRelationship[] {
  if (!run) return []

  const relationships: RunRelationship[] = []

  if (run.parent_run_id) {
    pushUnique(relationships, {
      kind: "parent",
      runID: run.parent_run_id,
      label: "Parent Workflow",
    })
  }

  for (const event of events) {
    const child = event.childWorkflowStarted
    if (!child?.childRunId) continue
    pushUnique(relationships, {
      kind: "child",
      runID: child.childRunId,
      label: child.childId || child.name || "Child Workflow",
      workflowType: child.name,
      eventID: event.id,
    })
  }

  // The predecessor pointer on a Run cannot identify its successor. The terminal
  // history event is the authoritative forward edge for Continue-As-New.
  const continuation = events.findLast((event) => event.workflowContinuedAsNew)
  if (continuation?.workflowContinuedAsNew?.nextRunId) {
    pushUnique(relationships, {
      kind: "continue-as-new",
      runID: continuation.workflowContinuedAsNew.nextRunId,
      label: "Next Segment",
      workflowType: continuation.workflowContinuedAsNew.name,
      eventID: continuation.id,
    })
  }

  const resetMarker = events.findLast(
    (event) => event.workflowTaskFailed?.cause === "reset_workflow",
  )?.workflowTaskFailed
  if (resetMarker?.baseRunId && resetMarker.baseRunId !== run.id) {
    pushUnique(relationships, {
      kind: "reset-from",
      runID: resetMarker.baseRunId,
      label: "Reset From",
    })
  }

  if (run.reset_run_id && run.reset_run_id !== run.id) {
    pushUnique(relationships, {
      kind: "reset-to",
      runID: run.reset_run_id,
      label: "Reset Run",
    })
  }

  return relationships
}

export function relationshipExpansionGuard(
  runID: string,
  ancestry: readonly string[],
  depth: number,
  maxDepth: number,
): RelationshipExpansionGuard {
  if (ancestry.includes(runID)) return { allowed: false, reason: "cycle" }
  if (depth >= maxDepth) return { allowed: false, reason: "max-depth" }
  return { allowed: true }
}

export function groupRunRelationships(relationships: readonly RunRelationship[]) {
  return {
    parents: relationships.filter((item) => item.kind === "parent"),
    children: relationships.filter((item) => item.kind === "child"),
    continuations: relationships.filter((item) => item.kind === "continue-as-new"),
    resets: relationships.filter((item) => item.kind === "reset-from" || item.kind === "reset-to"),
  }
}
