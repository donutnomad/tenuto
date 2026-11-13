import type { WorkflowQueryResult } from "./types"

export const BUILT_IN_QUERY_NAME = "__stack_trace"
export const QUERY_HISTORY_LIMIT = 20

export type QueryInputValidation =
  | { ok: true; value: Record<string, unknown> }
  | { ok: false; error: string }

export interface QueryHistoryEntry {
  id: string
  name: string
  inputText: string
  input: Record<string, unknown>
  status: "success" | "error"
  result?: unknown
  error?: string
  executedAt: string
}

interface SessionStorageLike {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function staticString(value: unknown): string | null {
  if (typeof value === "string") return value
  if (!isRecord(value)) return null

  const literal = value.literal
  if (typeof literal === "string") return literal
  if (isRecord(literal) && literal.type === "string" && typeof literal.value === "string") {
    return literal.value
  }
  if (value.type === "string" && typeof value.value === "string") return value.value
  return null
}

function isWorkflowPackage(value: unknown): boolean {
  return isRecord(value) && value.pkg === "workflow"
}

function isContextArgument(value: unknown): boolean {
  return isRecord(value) && value.var === "ctx"
}

function queryNameFromArguments(value: unknown): string | null {
  if (Array.isArray(value)) {
    const nameIndex = isWorkflowPackage(value[0]) ? 2 : isContextArgument(value[0]) ? 1 : 0
    return staticString(value[nameIndex])
  }
  if (!isRecord(value)) return null

  const args = value.args ?? value.arguments
  return Array.isArray(args) ? queryNameFromArguments(args) : null
}

function isSetQueryHandlerName(value: unknown): boolean {
  return value === "SetQueryHandler" || value === "workflow.SetQueryHandler"
}

/** Finds statically declared query names without evaluating the WLang program. */
export function discoverQueryNames(programJSON: string | unknown): string[] {
  let root: unknown = programJSON
  if (typeof programJSON === "string") {
    try {
      root = JSON.parse(programJSON)
    } catch {
      return [BUILT_IN_QUERY_NAME]
    }
  }

  const names = new Set<string>([BUILT_IN_QUERY_NAME])
  const visited = new WeakSet<object>()

  function visit(value: unknown) {
    if (Array.isArray(value)) {
      if (visited.has(value)) return
      visited.add(value)
      value.forEach(visit)
      return
    }
    if (!isRecord(value) || visited.has(value)) return
    visited.add(value)

    for (const [key, child] of Object.entries(value)) {
      if (isSetQueryHandlerName(key)) {
        const name = queryNameFromArguments(child)?.trim()
        if (name) names.add(name)
      }
    }

    const callName = value.name ?? value.function
    if (isSetQueryHandlerName(callName)) {
      const name = queryNameFromArguments(value.args ?? value.arguments)?.trim()
      if (name) names.add(name)
    }

    Object.values(value).forEach(visit)
  }

  visit(root)
  return [...names]
}

export function validateQueryInput(text: string): QueryInputValidation {
  if (!text.trim()) return { ok: true, value: {} }

  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown JSON syntax error"
    return { ok: false, error: `Invalid JSON: ${message}` }
  }

  if (!isRecord(parsed)) {
    return { ok: false, error: "Arguments must be a JSON object." }
  }
  return { ok: true, value: parsed }
}

export function addQueryHistory(
  history: QueryHistoryEntry[],
  entry: QueryHistoryEntry,
  limit = QUERY_HISTORY_LIMIT,
): QueryHistoryEntry[] {
  return [entry, ...history].slice(0, Math.max(0, limit))
}

export function queryHistoryStorageKey(runID: string): string {
  return `workflow:query-history:${runID}`
}

function isQueryHistoryEntry(value: unknown): value is QueryHistoryEntry {
  if (!isRecord(value) || typeof value.id !== "string" || typeof value.name !== "string") return false
  if (typeof value.inputText !== "string" || !isRecord(value.input)) return false
  if (value.status !== "success" && value.status !== "error") return false
  return typeof value.executedAt === "string"
}

export function loadQueryHistory(
  storage: SessionStorageLike,
  runID: string,
  limit = QUERY_HISTORY_LIMIT,
): QueryHistoryEntry[] {
  try {
    const raw = storage.getItem(queryHistoryStorageKey(runID))
    if (!raw) return []
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(isQueryHistoryEntry).slice(0, Math.max(0, limit))
  } catch {
    return []
  }
}

export function saveQueryHistory(
  storage: SessionStorageLike,
  runID: string,
  history: QueryHistoryEntry[],
  limit = QUERY_HISTORY_LIMIT,
): void {
  try {
    storage.setItem(queryHistoryStorageKey(runID), JSON.stringify(history.slice(0, Math.max(0, limit))))
  } catch {
    // Querying remains available when browser storage is disabled or full.
  }
}

// queryOutcome 解读 /query 的应答：Server 对被拒或出错的 Query 仍返回 200，
// 用 query_rejected / query_error 描述失败；成功时 query_result 是 Handler 的返回值。
export function queryOutcome(response: WorkflowQueryResult | null | undefined): { result: unknown } | { error: string } {
  if (response?.query_rejected || response?.query_error) {
    return { error: response.query_error || "Query was rejected by the workflow." }
  }
  return { result: response?.query_result ?? null }
}
