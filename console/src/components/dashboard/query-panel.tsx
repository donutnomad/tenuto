"use client"

import { useEffect, useId, useMemo, useRef, useState } from "react"
import { Loader2, Play, Send, Trash2 } from "lucide-react"
import { api } from "@/lib/api"
import {
  addQueryHistory,
  discoverQueryNames,
  loadQueryHistory,
  queryOutcome,
  saveQueryHistory,
  validateQueryInput,
  type QueryHistoryEntry,
} from "@/lib/query-panel"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { JsonViewer } from "./json-viewer"

export interface QueryPanelProps {
  namespace: string
  runID: string
  programJSON?: string | null
}

let entrySequence = 0

function queryErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Query failed with an unknown error."
}

function createHistoryEntry(
  name: string,
  inputText: string,
  input: Record<string, unknown>,
  outcome: { result: unknown } | { error: string },
): QueryHistoryEntry {
  entrySequence += 1
  return {
    id: `${Date.now()}-${entrySequence}`,
    name,
    inputText,
    input,
    status: "result" in outcome ? "success" : "error",
    ...(outcome),
    executedAt: new Date().toISOString(),
  }
}

export function QueryPanel({ namespace, runID, programJSON }: QueryPanelProps) {
  return <QueryPanelSession key={`${namespace}/${runID}`} namespace={namespace} runID={runID} programJSON={programJSON} />
}

function QueryPanelSession({ namespace, runID, programJSON }: QueryPanelProps) {
  const presetID = useId()
  const nameID = useId()
  const inputID = useId()
  const inputErrorID = useId()
  const presets = useMemo(() => discoverQueryNames(programJSON ?? null), [programJSON])
  const [queryName, setQueryName] = useState("__stack_trace")
  const [queryInput, setQueryInput] = useState("")
  const [history, setHistory] = useState<QueryHistoryEntry[]>([])
  const [historyReady, setHistoryReady] = useState(false)
  const [loading, setLoading] = useState(false)
  const [requestError, setRequestError] = useState<string | null>(null)
  const active = useRef(true)
  const inputValidation = useMemo(() => validateQueryInput(queryInput), [queryInput])
  const selectedPreset = presets.includes(queryName) ? queryName : ""

  useEffect(() => {
    active.current = true
    const loadTimer = window.setTimeout(() => {
      setHistory(loadQueryHistory(window.sessionStorage, runID))
      setHistoryReady(true)
    }, 0)
    return () => {
      window.clearTimeout(loadTimer)
      active.current = false
    }
  }, [runID])

  useEffect(() => {
    if (historyReady) saveQueryHistory(window.sessionStorage, runID, history)
  }, [history, historyReady, runID])

  async function executeQuery(name = queryName, inputText = queryInput) {
    const normalizedName = name.trim()
    const validation = validateQueryInput(inputText)
    setQueryName(name)
    setQueryInput(inputText)
    setRequestError(null)

    if (!normalizedName) {
      setRequestError("Query name is required.")
      return
    }
    if (!validation.ok) {
      setRequestError(validation.error)
      return
    }

    const requestRunID = runID
    setLoading(true)
    try {
      const outcome = queryOutcome(await api.queryWorkflow(namespace, requestRunID, normalizedName, validation.value))
      if (!active.current) return
      if ("error" in outcome) setRequestError(outcome.error)
      setHistory((current) => addQueryHistory(
        current,
        createHistoryEntry(normalizedName, inputText, validation.value, outcome),
      ))
    } catch (error) {
      if (!active.current) return
      const message = queryErrorMessage(error)
      setRequestError(message)
      setHistory((current) => addQueryHistory(
        current,
        createHistoryEntry(normalizedName, inputText, validation.value, { error: message }),
      ))
    } finally {
      if (active.current) setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Workflow Query</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-3 md:grid-cols-[minmax(12rem,0.7fr)_minmax(16rem,1.3fr)]">
            <div>
              <label htmlFor={presetID} className="text-sm text-muted-foreground">Query preset</label>
              <select
                id={presetID}
                aria-label="Query preset"
                value={selectedPreset}
                onChange={(event) => {
                  if (event.target.value) setQueryName(event.target.value)
                }}
                className="mt-1 h-8 w-full rounded-md border border-border bg-background px-3 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
              >
                <option value="">Custom query</option>
                {presets.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div>
              <label htmlFor={nameID} className="text-sm text-muted-foreground">Query name</label>
              <input
                id={nameID}
                aria-label="Query name"
                type="text"
                value={queryName}
                onChange={(event) => setQueryName(event.target.value)}
                className="mt-1 h-8 w-full rounded-md border border-border bg-background px-3 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                placeholder="e.g. __stack_trace"
              />
            </div>
          </div>
          <div>
            <label htmlFor={inputID} className="text-sm text-muted-foreground">Arguments (JSON object, optional)</label>
            <textarea
              id={inputID}
              aria-label="Query arguments as JSON"
              aria-invalid={!inputValidation.ok}
              aria-describedby={!inputValidation.ok ? inputErrorID : undefined}
              value={queryInput}
              onChange={(event) => setQueryInput(event.target.value)}
              rows={4}
              className="mt-1 w-full resize-y rounded-md border border-border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring aria-invalid:border-destructive"
              placeholder="{}"
            />
            {!inputValidation.ok && (
              <p id={inputErrorID} role="alert" className="mt-1 text-xs text-destructive">
                {inputValidation.error}
              </p>
            )}
          </div>
          <Button
            size="sm"
            aria-label={`Send query ${queryName.trim() || "with empty name"}`}
            onClick={() => void executeQuery()}
            disabled={loading || !queryName.trim() || !inputValidation.ok}
          >
            {loading ? <Loader2 className="animate-spin" /> : <Send />}
            {loading ? "Querying…" : "Send Query"}
          </Button>
        </CardContent>
      </Card>

      {requestError && (
        <div role="alert" className="rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {requestError}
        </div>
      )}

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Recent Queries</CardTitle>
          <Button
            variant="ghost"
            size="sm"
            aria-label="Clear recent queries"
            onClick={() => setHistory([])}
            disabled={history.length === 0}
          >
            <Trash2 />
            Clear
          </Button>
        </CardHeader>
        <CardContent>
          {history.length === 0 ? (
            <p className="text-sm text-muted-foreground">Queries and results from this browser session appear here.</p>
          ) : (
            <ol aria-label="Recent workflow queries" className="space-y-3">
              {history.map((entry, index) => (
                <li key={entry.id} className="rounded-lg border border-border p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="min-w-0">
                      <code className="break-all text-sm font-medium">{entry.name}</code>
                      <span className="ml-2 text-xs text-muted-foreground">
                        {new Date(entry.executedAt).toLocaleTimeString()}
                      </span>
                    </div>
                    <Button
                      variant="outline"
                      size="xs"
                      aria-label={`Run query ${entry.name} again`}
                      onClick={() => void executeQuery(entry.name, entry.inputText)}
                      disabled={loading}
                    >
                      <Play />
                      Run again
                    </Button>
                  </div>
                  <div className="mt-2 text-xs text-muted-foreground">
                    Arguments: <code className="break-all">{entry.inputText.trim() || "{}"}</code>
                  </div>
                  {entry.status === "success" ? (
                    <div className="mt-3">
                      <JsonViewer data={entry.result ?? null} title="Result" defaultOpen={index === 0} />
                    </div>
                  ) : (
                    <div role="alert" className="mt-3 rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
                      {entry.error}
                    </div>
                  )}
                </li>
              ))}
            </ol>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
