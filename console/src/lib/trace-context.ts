import { api } from "@/lib/api"
import type { Event, Run, WorkflowConfig } from "@/lib/types"
import { getWlangProgram } from "@/lib/wlang-program"
import { loadWasm, traceReplay, type Trace } from "@/lib/wasm-loader"

export interface TraceRunContext {
  run: Run
  events: Event[]
  workflowConfig: WorkflowConfig
  programJSON: string
  trace: Trace
}

export function validateChildEdge(
  parentRunID: string,
  childRun: Run,
  childID?: string,
  scheduledEventID?: number,
  parentNamespace?: string,
): void {
  if (childRun.parent_run_id !== parentRunID) {
    throw new Error(`child run ${childRun.id} does not belong to parent ${parentRunID}`)
  }
  if (childID && childRun.parent_child_id && childRun.parent_child_id !== childID) {
    throw new Error(`child run ${childRun.id} does not match invocation ${childID}`)
  }
  if (
    scheduledEventID
    && childRun.parent_scheduled_event_id
    && childRun.parent_scheduled_event_id !== scheduledEventID
  ) {
    throw new Error(`child run ${childRun.id} does not match scheduled event ${scheduledEventID}`)
  }
  if (parentNamespace && childRun.namespace !== parentNamespace) {
    throw new Error(`child run ${childRun.id} is outside namespace ${parentNamespace}`)
  }
}

export function nextContinuationRunID(context: TraceRunContext): string | null {
  return context.events.findLast((event) => event.workflowContinuedAsNew)?.workflowContinuedAsNew?.nextRunId
    ?? null
}

export function validateContinuationEdge(previousRunID: string, nextRun: Run): void {
  if (nextRun.continued_run_id !== previousRunID) {
    throw new Error(`continued run ${nextRun.id} does not continue ${previousRunID}`)
  }
}

export async function loadTraceRunContext(
  namespace: string,
  runID: string,
  signal?: AbortSignal,
): Promise<TraceRunContext> {
  const [run, events, workflowConfig] = await Promise.all([
    api.getRun(namespace, runID, signal),
    api.getHistory(namespace, runID, signal),
    api.getConfig(namespace, runID, signal),
  ])
  if (signal?.aborted) throw new DOMException("Trace load aborted", "AbortError")
  const programJSON = getWlangProgram(workflowConfig)
  await loadWasm()
  if (signal?.aborted) throw new DOMException("Trace load aborted", "AbortError")
  return {
    run,
    events,
    workflowConfig,
    programJSON,
    trace: traceReplay(programJSON, JSON.stringify(events)),
  }
}

export class TraceContextCache {
  private readonly contexts = new Map<string, TraceRunContext>()
  private readonly inFlight = new Map<string, Promise<TraceRunContext>>()
  private generation = 0
  private readonly runGenerations = new Map<string, number>()

  constructor(
    private readonly maxEntries: number,
    private readonly loader: (runID: string) => Promise<TraceRunContext>,
  ) {}

  get(runID: string): TraceRunContext | undefined {
    const context = this.contexts.get(runID)
    if (!context) return undefined
    this.contexts.delete(runID)
    this.contexts.set(runID, context)
    return context
  }

  load(runID: string, signal?: AbortSignal): Promise<TraceRunContext> {
    const cached = this.get(runID)
    if (cached) return waitForTraceContext(Promise.resolve(cached), signal)
    const pending = this.inFlight.get(runID)
    if (pending) return waitForTraceContext(pending, signal)
    const cacheGeneration = this.generation
    const runGeneration = this.runGenerations.get(runID) ?? 0
    const request = this.loader(runID).then((context) => {
      if (
        this.generation === cacheGeneration
        && (this.runGenerations.get(runID) ?? 0) === runGeneration
      ) this.store(runID, context)
      return context
    }).finally(() => {
      if (this.inFlight.get(runID) === request) this.inFlight.delete(runID)
    })
    this.inFlight.set(runID, request)
    return waitForTraceContext(request, signal)
  }

  refresh(runID: string, signal?: AbortSignal): Promise<TraceRunContext> {
    this.contexts.delete(runID)
    this.runGenerations.set(runID, (this.runGenerations.get(runID) ?? 0) + 1)
    this.inFlight.delete(runID)
    return this.load(runID, signal)
  }

  clear(): void {
    this.generation++
    this.contexts.clear()
    this.inFlight.clear()
    this.runGenerations.clear()
  }

  private store(runID: string, context: TraceRunContext): void {
    this.contexts.set(runID, context)
    while (this.contexts.size > this.maxEntries) {
      const oldest = this.contexts.keys().next().value
      if (oldest) this.contexts.delete(oldest)
      else break
    }
  }
}

function waitForTraceContext(
  request: Promise<TraceRunContext>,
  signal?: AbortSignal,
): Promise<TraceRunContext> {
  if (!signal) return request
  if (signal.aborted) return Promise.reject(new DOMException("Trace load aborted", "AbortError"))
  return new Promise((resolve, reject) => {
    const abort = () => reject(new DOMException("Trace load aborted", "AbortError"))
    signal.addEventListener("abort", abort, { once: true })
    request.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort))
  })
}

export class TraceTransitionGate {
  private sequence = 0
  private current: { id: number; controller: AbortController } | null = null

  start(): { id: number; signal: AbortSignal } {
    this.current?.controller.abort()
    const controller = new AbortController()
    const id = ++this.sequence
    this.current = { id, controller }
    return { id, signal: controller.signal }
  }

  isCurrent(id: number): boolean {
    return this.current?.id === id
  }

  finish(id: number): boolean {
    if (!this.isCurrent(id)) return false
    this.current = null
    return true
  }

  cancel(): void {
    this.current?.controller.abort()
    this.current = null
  }
}
