"use client"

import Link from "next/link"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useParams } from "next/navigation"
import { ArrowLeft, ChevronRight, Loader2, X } from "lucide-react"
import {
  emptyTraceNavigation,
  traceLineageContainsRun,
  type TraceLocation,
  type TraceNavigationSnapshot,
} from "@/lib/trace-session"
import { useTraceLocation } from "@/lib/use-trace-location"
import { type TraceEffect } from "@/lib/wasm-loader"
import {
  TraceContextCache,
  TraceTransitionGate,
  loadTraceRunContext,
  nextContinuationRunID,
  validateChildEdge,
  validateContinuationEdge,
  type TraceRunContext,
} from "@/lib/trace-context"
import { RunStatusBadge } from "@/components/dashboard/run-status-badge"
import { TraceDebugger } from "@/components/dashboard/trace-debugger"
import {
  findBreakpointStepAtOrAfter,
  shouldStopAtParentBoundary,
} from "@/components/dashboard/trace-debugger-logic"
import {
  boundariesAt,
  buildBoundary,
  rootSegmentsAt,
  snapshotForStep,
  workflowPath,
  type WorkflowBoundary,
} from "@/components/dashboard/trace-boundaries"

export default function TracePage() {
  const params = useParams<{ namespace: string; runID: string }>()
  const namespace = params.namespace ?? "default"
  const rootRunID = params.runID ?? ""
  const [activeRunID, setActiveRunID] = useState(rootRunID)
  const [inspectedRunID, setInspectedRunID] = useState(rootRunID)
  const [boundaries, setBoundaries] = useState<WorkflowBoundary[]>([])
  const [rootSegments, setRootSegments] = useState([rootRunID])
  const [initialNavigation, setInitialNavigation] = useState<TraceNavigationSnapshot>()
  const [sessionRevision, setSessionRevision] = useState(0)
  const [hydrationRevision, setHydrationRevision] = useState(0)
  const [lineageLoading, setLineageLoading] = useState(false)
  const [sessionError, setSessionError] = useState<string | null>(null)
  const [transitionError, setTransitionError] = useState<string | null>(null)
  const [transitionKind, setTransitionKind] = useState<"idle" | "loading-child" | "loading-segment" | "loading-boundary">("idle")
  const [pendingChildEffects, setPendingChildEffects] = useState(0)
  const [inspectedContext, setInspectedContext] = useState<TraceRunContext | null>(null)
  const navigationByRun = useRef(new Map<string, TraceNavigationSnapshot>())
  const contextCache = useRef(new TraceContextCache(8, (runID) => loadTraceRunContext(namespace, runID)))
  const transitionGate = useRef(new TraceTransitionGate())
  const run = inspectedContext?.run
  const events = inspectedContext?.events
  const programJSON = inspectedContext?.programJSON
  const eventsJSON = useMemo(() => (events ? JSON.stringify(events) : null), [events])
  const continuationRunID = inspectedContext ? nextContinuationRunID(inspectedContext) : null

  const cancelTransition = useCallback(() => {
    transitionGate.current.cancel()
    setTransitionKind("idle")
    setLineageLoading(false)
  }, [])

  const startTransition = useCallback((kind: "loading-child" | "loading-segment" | "loading-boundary") => {
    const request = transitionGate.current.start()
    setTransitionError(null)
    setTransitionKind(kind)
    return request
  }, [])

  const isCurrentRequest = useCallback((request: { id: number }) => (
    transitionGate.current.isCurrent(request.id)
  ), [])

  useEffect(() => {
    let cancelled = false
    contextCache.current.load(inspectedRunID).then((context) => {
      if (!cancelled) setInspectedContext(context)
    }).catch((loadError) => {
      if (!cancelled && loadError instanceof Error && loadError.name !== "AbortError") {
        setSessionError(loadError.message)
      }
    })
    return () => {
      cancelled = true
    }
  }, [inspectedRunID])

  useEffect(() => () => transitionGate.current.cancel(), [])

  useEffect(() => {
    if (pendingChildEffects === 0 || inspectedRunID !== activeRunID) return
    const controller = new AbortController()
    const pollingRunID = inspectedRunID
    let timer = 0
    const poll = async () => {
      try {
        const context = await contextCache.current.refresh(pollingRunID, controller.signal)
        setInspectedContext((current) => current?.run.id === pollingRunID ? context : current)
      } catch {
        // A later poll or navigation owns the current state.
      } finally {
        if (!controller.signal.aborted) timer = window.setTimeout(poll, 2000)
      }
    }
    timer = window.setTimeout(poll, 2000)
    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [activeRunID, inspectedRunID, pendingChildEffects])

  const restoreLocation = useCallback((location: TraceLocation, fromHistory: boolean) => {
    if (fromHistory) cancelTransition()
    const { activeRunID: restoredActiveRunID, inspectedRunID: restoredInspectedRunID } = location
    navigationByRun.current.set(restoredActiveRunID, location.activeNavigation)
    navigationByRun.current.set(restoredInspectedRunID, location.inspectedNavigation)
    setInspectedContext(contextCache.current.get(restoredInspectedRunID) ?? null)
    setActiveRunID(restoredActiveRunID)
    setInspectedRunID(restoredInspectedRunID)
    setInitialNavigation(location.inspectedNavigation)
    setHydrationRevision((revision) => revision + 1)
    setRootSegments((current) => rootSegmentsAt(current, restoredActiveRunID, rootRunID))
    setBoundaries((current) => boundariesAt(current, restoredActiveRunID, rootRunID))
  }, [cancelTransition, rootRunID])
  const { ready: urlReady, write: writeLocation } = useTraceLocation(rootRunID, restoreLocation)

  useEffect(() => {
    if (!urlReady || activeRunID === rootRunID) return
    if (traceLineageContainsRun(
      rootSegments,
      boundaries.map((boundary) => boundary.segments),
      activeRunID,
    )) return
    let cancelled = false
    async function rebuildLineage() {
      setLineageLoading(true)
      setSessionError(null)
      try {
        const reversed: WorkflowBoundary[] = []
        let childContext = await contextCache.current.load(activeRunID)
        let childRun = childContext.run
        const visited = new Set<string>()
        while (childRun.id !== rootRunID) {
          const segments = [childRun.id]
          while (!childRun.parent_run_id && childRun.continued_run_id) {
            if (visited.has(childRun.id)) {
              throw new Error(`run ${activeRunID} contains a continuation cycle`)
            }
            visited.add(childRun.id)
            const continuedRunID = childRun.id
            childContext = await contextCache.current.load(childRun.continued_run_id)
            if (nextContinuationRunID(childContext) !== continuedRunID) {
              throw new Error(`run ${continuedRunID} is not connected by a ContinueAsNew event`)
            }
            childRun = childContext.run
            segments.unshift(childRun.id)
          }
          if (childRun.id === rootRunID) {
            setRootSegments(segments)
            break
          }
          if (!childRun.parent_run_id || visited.has(childRun.id)) {
            throw new Error(`run ${activeRunID} is outside root Trace ${rootRunID}`)
          }
          visited.add(childRun.id)
          const parentContext = await contextCache.current.load(childRun.parent_run_id)
          validateChildEdge(childRun.parent_run_id, childRun, undefined, undefined, parentContext.run.namespace)
          const boundary = buildBoundary(parentContext, childRun.id)
          boundary.segments = segments
          reversed.push(boundary)
          childContext = parentContext
          childRun = parentContext.run
        }
        if (cancelled) return
        setBoundaries(reversed.reverse())
        setInspectedContext(contextCache.current.get(activeRunID) ?? null)
      } catch (lineageError) {
        if (!cancelled) setSessionError(lineageError instanceof Error ? lineageError.message : String(lineageError))
      } finally {
        if (!cancelled) setLineageLoading(false)
      }
    }
    rebuildLineage()
    return () => {
      cancelled = true
    }
  }, [activeRunID, boundaries, rootRunID, rootSegments, urlReady])

  const navigationChanged = useCallback((snapshot: TraceNavigationSnapshot) => {
    navigationByRun.current.set(inspectedRunID, snapshot)
    if (!urlReady) return
    writeLocation(
      activeRunID,
      inspectedRunID,
      navigationByRun.current.get(activeRunID),
      snapshot,
      "replace",
    )
  }, [activeRunID, inspectedRunID, urlReady, writeLocation])

  const stepIntoChild = useCallback(async (
    effect: TraceEffect,
    entryNavigation: TraceNavigationSnapshot,
    resumeNavigation: TraceNavigationSnapshot,
  ) => {
    if (!effect.child_run_id || !run || inspectedRunID !== activeRunID) return
    const childRunID = effect.child_run_id
    const request = startTransition("loading-child")
    try {
      const childContext = await contextCache.current.load(childRunID, request.signal)
      if (!isCurrentRequest(request)) return
      validateChildEdge(activeRunID, childContext.run, effect.child_id, effect.scheduled_event_id, run.namespace)
      navigationByRun.current.set(activeRunID, entryNavigation)
      const boundary: WorkflowBoundary = {
        parentRunID: activeRunID,
        parentWorkflowType: run.workflow_type ?? run.id,
        childRunID,
        childWorkflowType: effect.child_workflow_type ?? childRunID,
        effect,
        entryNavigation,
        resumeNavigation,
        segments: [childRunID],
      }
      setBoundaries((current) => [...current, boundary])
      setInitialNavigation(undefined)
      setActiveRunID(childRunID)
      setInspectedRunID(childRunID)
      setInspectedContext(childContext)
      writeLocation(childRunID, childRunID, undefined, undefined, "push")
    } catch (loadError) {
      if (isCurrentRequest(request) && loadError instanceof Error && loadError.name !== "AbortError") {
        setTransitionError(loadError.message)
      }
    } finally {
      if (isCurrentRequest(request)) {
        transitionGate.current.finish(request.id)
        setTransitionKind("idle")
      }
    }
  }, [activeRunID, inspectedRunID, isCurrentRequest, run, startTransition, writeLocation])

  const stepOut = useCallback(() => {
    cancelTransition()
    const boundary = boundaries.at(-1)
    if (!boundary) return
    navigationByRun.current.set(boundary.parentRunID, boundary.resumeNavigation)
    setBoundaries((current) => current.slice(0, -1))
    setInitialNavigation(boundary.resumeNavigation)
    const parentContext = contextCache.current.get(boundary.parentRunID)
    setInspectedContext(parentContext ?? null)
    setActiveRunID(boundary.parentRunID)
    setInspectedRunID(boundary.parentRunID)
    writeLocation(
      boundary.parentRunID,
      boundary.parentRunID,
      boundary.resumeNavigation,
      boundary.resumeNavigation,
      "replace",
    )
  }, [boundaries, cancelTransition, writeLocation])

  const enterContinuation = useCallback(async () => {
    const currentContext = contextCache.current.get(activeRunID)
    const nextRunID = currentContext ? nextContinuationRunID(currentContext) : null
    if (!nextRunID || inspectedRunID !== activeRunID) return
    const request = startTransition("loading-segment")
    try {
      const nextContext = await contextCache.current.load(nextRunID, request.signal)
      if (!isCurrentRequest(request)) return
      validateContinuationEdge(activeRunID, nextContext.run)
      if (nextContext.run.namespace !== currentContext?.run.namespace) {
        throw new Error(`continued run ${nextRunID} is outside namespace ${currentContext?.run.namespace}`)
      }
      if (boundaries.length === 0) {
        setRootSegments((current) => [...current, nextRunID])
      } else {
        setBoundaries((current) => current.map((boundary, index) => (
          index === current.length - 1
            ? { ...boundary, segments: [...boundary.segments, nextRunID] }
            : boundary
        )))
      }
      setInitialNavigation(undefined)
      setActiveRunID(nextRunID)
      setInspectedRunID(nextRunID)
      setInspectedContext(nextContext)
      writeLocation(nextRunID, nextRunID, undefined, undefined, "push")
    } catch (loadError) {
      if (isCurrentRequest(request) && loadError instanceof Error && loadError.name !== "AbortError") {
        setTransitionError(loadError.message)
      }
    } finally {
      if (isCurrentRequest(request)) {
        transitionGate.current.finish(request.id)
        setTransitionKind("idle")
      }
    }
  }, [activeRunID, boundaries.length, inspectedRunID, isCurrentRequest, startTransition, writeLocation])

  const stepForwardAcrossBoundary = () => {
    const currentContext = contextCache.current.get(activeRunID)
    if (currentContext && nextContinuationRunID(currentContext)) {
      void enterContinuation()
      return
    }
    stepOut()
  }

  const stepBackAcrossBoundary = useCallback(() => {
    const boundary = boundaries.at(-1)
    const segments = boundary?.segments ?? rootSegments
    const activeSegmentIndex = segments.indexOf(activeRunID)
    if (activeSegmentIndex > 0) {
      const previousRunID = segments[activeSegmentIndex - 1]
      const previousContext = contextCache.current.get(previousRunID)
      const commitPrevious = (context: TraceRunContext) => {
        const previousStep = context.trace.steps.at(-1)
        const previousNavigation = previousStep
          ? {
              ...snapshotForStep(context.trace, previousStep, navigationByRun.current.get(previousRunID)),
              terminalStop: true,
            }
          : { ...emptyTraceNavigation(), terminalStop: true }
        navigationByRun.current.set(previousRunID, previousNavigation)
        setInitialNavigation(previousNavigation)
        setActiveRunID(previousRunID)
        setInspectedRunID(previousRunID)
        setInspectedContext(context)
        if (boundary) {
          setBoundaries((current) => current.map((item, index) => (
            index === current.length - 1
              ? { ...item, segments: item.segments.slice(0, activeSegmentIndex) }
              : item
          )))
        } else {
          setRootSegments((current) => current.slice(0, activeSegmentIndex))
        }
        writeLocation(previousRunID, previousRunID, previousNavigation, previousNavigation, "replace")
      }
      if (previousContext) {
        commitPrevious(previousContext)
      } else {
        const request = startTransition("loading-segment")
        contextCache.current.load(previousRunID, request.signal).then((context) => {
          if (isCurrentRequest(request)) commitPrevious(context)
        }).catch((loadError) => {
          if (isCurrentRequest(request) && loadError instanceof Error && loadError.name !== "AbortError") {
            setTransitionError(loadError.message)
          }
        }).finally(() => {
          if (isCurrentRequest(request)) {
            transitionGate.current.finish(request.id)
            setTransitionKind("idle")
          }
        })
      }
      return
    }
    if (!boundary) return
    navigationByRun.current.set(boundary.parentRunID, boundary.entryNavigation)
    setBoundaries((current) => current.slice(0, -1))
    setInitialNavigation(boundary.entryNavigation)
    const parentContext = contextCache.current.get(boundary.parentRunID)
    setInspectedContext(parentContext ?? null)
    setActiveRunID(boundary.parentRunID)
    setInspectedRunID(boundary.parentRunID)
    writeLocation(
      boundary.parentRunID,
      boundary.parentRunID,
      boundary.entryNavigation,
      boundary.entryNavigation,
      "replace",
    )
  }, [activeRunID, boundaries, isCurrentRequest, rootSegments, startTransition, writeLocation])

  const continueAcrossBoundaries = useCallback(async () => {
    if (boundaries.length === 0) return
    const request = startTransition("loading-boundary")
    setLineageLoading(true)
    setSessionError(null)
    try {
      for (let index = boundaries.length - 1; index >= 0; index--) {
        const boundary = boundaries[index]
        const parentContext = await contextCache.current.load(boundary.parentRunID, request.signal)
        if (!isCurrentRequest(request)) return
        const { run: parentRun, trace: parentTrace } = parentContext
        const latestParentNavigation = navigationByRun.current.get(boundary.parentRunID)
        const parentBreakpoints = latestParentNavigation?.breakpoints ?? boundary.resumeNavigation.breakpoints
        const breakpoint = findBreakpointStepAtOrAfter(
          parentTrace.steps,
          boundary.resumeNavigation.stepSeq,
          new Set(parentBreakpoints),
        )
        const reachedRoot = index === 0
        if (!shouldStopAtParentBoundary(parentRun.status, Boolean(breakpoint), reachedRoot)) continue
        const destination = breakpoint ?? parentTrace.steps.at(-1)
        const resumeNavigation = {
          ...boundary.resumeNavigation,
          breakpoints: parentBreakpoints,
        }
        const navigation = destination
          ? snapshotForStep(parentTrace, destination, resumeNavigation)
          : resumeNavigation
        if (
          parentRun.status === "failed"
          || parentRun.status === "cancelled"
          || parentRun.status === "terminated"
          || parentRun.status === "continued_as_new"
        ) {
          navigation.terminalStop = true
        }
        if (!isCurrentRequest(request)) return
        navigationByRun.current.set(boundary.parentRunID, navigation)
        setBoundaries((current) => current.slice(0, index))
        setInitialNavigation(navigation)
        setInspectedContext(parentContext)
        setActiveRunID(boundary.parentRunID)
        setInspectedRunID(boundary.parentRunID)
        writeLocation(
          boundary.parentRunID,
          boundary.parentRunID,
          navigation,
          navigation,
          "replace",
        )
        return
      }
    } catch (continueError) {
      if (isCurrentRequest(request) && continueError instanceof Error && continueError.name !== "AbortError") {
        setSessionError(continueError.message)
      }
    } finally {
      if (isCurrentRequest(request)) {
        transitionGate.current.finish(request.id)
        setTransitionKind("idle")
        setLineageLoading(false)
      }
    }
  }, [boundaries, isCurrentRequest, startTransition, writeLocation])

  const restartSession = useCallback(() => {
    cancelTransition()
    navigationByRun.current.clear()
    setBoundaries([])
    setRootSegments([rootRunID])
    setInitialNavigation(undefined)
    const rootContext = contextCache.current.get(rootRunID)
    setInspectedContext(rootContext ?? null)
    setActiveRunID(rootRunID)
    setInspectedRunID(rootRunID)
    setSessionRevision((revision) => revision + 1)
    writeLocation(rootRunID, rootRunID, undefined, undefined, "replace")
  }, [cancelTransition, rootRunID, writeLocation])

  const inspectRun = useCallback((runID: string) => {
    cancelTransition()
    const navigation = navigationByRun.current.get(runID)
      ?? boundaries.find((boundary) => boundary.parentRunID === runID)?.entryNavigation
      ?? emptyTraceNavigation()
    setInitialNavigation(navigation)
    setHydrationRevision((revision) => revision + 1)
    const cached = contextCache.current.get(runID)
    setInspectedContext(cached ?? null)
    setInspectedRunID(runID)
    writeLocation(
      activeRunID,
      runID,
      navigationByRun.current.get(activeRunID),
      navigation,
      "replace",
    )
  }, [activeRunID, boundaries, cancelTransition, writeLocation])

  const continueAcrossBoundary = () => {
    if (continuationRunID) {
      void enterContinuation()
      return
    }
    void continueAcrossBoundaries()
  }

  const pathFrames = workflowPath(
    boundaries[0]?.parentWorkflowType ?? run?.workflow_type ?? rootRunID,
    boundaries,
    rootSegments,
  )
  const inspectedPathIndex = pathFrames.findIndex((frame) => frame.runID === inspectedRunID)
  const ancestorFrames = inspectedPathIndex > 0
    ? pathFrames.slice(0, inspectedPathIndex).reverse()
    : []
  const descendantFrames = inspectedPathIndex >= 0
    ? pathFrames.slice(inspectedPathIndex + 1).reverse()
    : []
  const readOnlyInspection = inspectedRunID !== activeRunID

  if (sessionError) {
    return (
      <div className="flex h-full items-center justify-center bg-[#1e1f22] px-6 text-sm text-red-300">
        Failed to load Trace: {sessionError}
      </div>
    )
  }

  if (!inspectedContext || !run || !events || lineageLoading) {
    return (
      <div className="flex h-full items-center justify-center gap-2 bg-[#1e1f22] text-sm text-[#a8adbd]">
        <Loader2 className="size-4 animate-spin" />
        {lineageLoading ? "Restoring workflow stack..." : "Loading Trace console..."}
      </div>
    )
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-[#1e1f22] text-[#dfe1e5]">
      <header className="flex h-12 shrink-0 items-center gap-3 border-b border-[#373737] bg-[#252628] px-4">
        <Link
          href={`/${namespace}/runs/${rootRunID}`}
          className="inline-flex items-center gap-1.5 rounded px-2 py-1.5 text-xs text-[#a8adbd] transition-colors hover:bg-[#34363b] hover:text-[#f0f1f3]"
        >
          <ArrowLeft className="size-3.5" />
          Run detail
        </Link>
        <span className="h-4 w-px bg-[#4e5157]" />
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center gap-1.5">
            {pathFrames.map((frame, index) => (
              <span key={frame.runID} className="contents">
                {index > 0 && <ChevronRight className="size-3 shrink-0 text-[#6f737a]" />}
                <button
                  type="button"
                  onClick={() => inspectRun(frame.runID)}
                  className={index === pathFrames.length - 1
                    ? "max-w-48 truncate font-mono text-sm font-semibold text-[#f0f1f3]"
                    : "max-w-40 truncate font-mono text-xs text-[#a8adbd] hover:text-[#dfe1e5]"}
                >
                  {frame.workflowType}
                </button>
              </span>
            ))}
            <RunStatusBadge status={run.status} />
            {readOnlyInspection && <span className="rounded bg-[#3b3422] px-1.5 py-0.5 text-[10px] text-[#e2b86b]">inspecting {run.workflow_id}</span>}
          </div>
          <div className="truncate font-mono text-[10px] text-[#7a7e85]">{run.id}</div>
        </div>
        <div className="hidden items-center gap-3 text-[11px] text-[#a8adbd] sm:flex">
          <span>{events.length} events</span>
          <span className="text-[#7a7e85]">event horizon {events.at(-1)?.id ?? 0}</span>
        </div>
      </header>

      <main className="flex min-h-0 flex-1 flex-col">
        {transitionError && (
          <div className="flex items-center gap-2 border-b border-[#6b343b] bg-[#3a2529] px-3 py-2 text-xs text-[#ef9a9a]" role="alert">
            <span className="min-w-0 flex-1">Child Trace load failed: {transitionError}</span>
            <button type="button" onClick={() => setTransitionError(null)} className="rounded p-0.5 hover:bg-[#513238]" title="关闭错误">
              <X className="size-3.5" />
            </button>
          </div>
        )}
        {eventsJSON ? (
          <div className="min-h-0 flex-1">
          <TraceDebugger
            key={`${inspectedRunID}:${sessionRevision}`}
            programJSON={programJSON ?? undefined}
            events={events}
            eventsJSON={eventsJSON}
            precomputedTrace={inspectedContext.trace}
            namespace={namespace}
            fullScreen
            initialNavigation={initialNavigation}
            canStepOut={boundaries.length > 0 && !readOnlyInspection}
            onStepIntoChild={readOnlyInspection ? undefined : stepIntoChild}
            onStepOut={stepOut}
            onStepBackBoundary={(boundaries.length > 0 || rootSegments.length > 1) && !readOnlyInspection ? stepBackAcrossBoundary : undefined}
            onStepForwardBoundary={(boundaries.length > 0 || continuationRunID) && !readOnlyInspection ? stepForwardAcrossBoundary : undefined}
            onContinueBoundary={(boundaries.length > 0 || continuationRunID) && !readOnlyInspection ? continueAcrossBoundary : undefined}
            onRestartSession={restartSession}
            ancestorFrames={ancestorFrames}
            descendantFrames={descendantFrames}
            onSelectAncestor={inspectRun}
            onSelectDescendant={inspectRun}
            readOnlyInspection={readOnlyInspection}
            runStatus={run.status}
            controlsBusy={transitionKind !== "idle"}
            boundaryStepLabel={continuationRunID ? "进入下一个 Run Segment" : undefined}
            onReturnToActive={() => inspectRun(activeRunID)}
            onNavigationChange={navigationChanged}
            onPendingChildEffectsChange={setPendingChildEffects}
            hydrationKey={`${inspectedRunID}:${hydrationRevision}`}
          />
          </div>
        ) : (
          <div className="flex h-full items-center justify-center text-sm text-[#a8adbd]">
            This run has no event history to inspect.
          </div>
        )}
      </main>
    </div>
  )
}
