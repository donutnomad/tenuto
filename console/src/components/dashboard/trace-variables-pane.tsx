"use client"

import { useState } from "react"
import { cn } from "@/lib/utils"
import { ChevronDown, ChevronRight as ChevronRightCollapse, Rows3 } from "lucide-react"

// Trace 调试器的变量树：按 Go 调试器的习惯显示 name = {type} value，map / slice 可展开。

function debuggerValueType(data: unknown): string {
  if (data === null) return "nil"
  if (Array.isArray(data)) {
    const itemTypes = [...new Set(data.map(debuggerValueType))]
    return `[]${itemTypes.length === 1 ? itemTypes[0] : "any"}`
  }
  if (typeof data === "object") {
    const valueTypes = [...new Set(Object.values(data as Record<string, unknown>).map(debuggerValueType))]
    return `map[string]${valueTypes.length === 1 ? valueTypes[0] : "any"}`
  }
  if (typeof data === "number") return Number.isInteger(data) ? "int" : "float64"
  return typeof data
}

function DebuggerPrimitiveValue({ data }: { data: unknown }) {
  if (data === null) return <span className="text-[#CB7832]">nil</span>
  if (data === undefined) return <span className="text-[#7a7e85]">undefined</span>
  if (typeof data === "string") return <span className="text-[#6A8759]">&quot;{data}&quot;</span>
  if (typeof data === "number") return <span className="text-[#6897BB]">{String(data)}</span>
  if (typeof data === "boolean") return <span className="text-[#cf8e6d]">{String(data)}</span>
  return <span className="text-[#bcbec4]">{String(data)}</span>
}

function VariableIcon() {
  return <Rows3 className="size-3.5 shrink-0 text-[#e5b13d]" strokeWidth={2.2} />
}

function DebuggerMapEntry({
  index,
  entryKey,
  value,
}: {
  index: number
  entryKey: string
  value: unknown
}) {
  const [expanded, setExpanded] = useState(false)
  const valuePreview = typeof value === "object" && value !== null ? debuggerValueType(value) : String(value)

  return (
    <div>
      <button
        type="button"
        onClick={() => setExpanded((current) => !current)}
        className="flex h-6 w-full items-center gap-0.5 px-1 text-left text-[11px] leading-none hover:bg-[#34363b] focus:bg-[#2e436e] focus:outline-none"
        aria-expanded={expanded}
      >
        {expanded ? <ChevronDown className="size-3 shrink-0 text-[#a8adbd]" /> : <ChevronRightCollapse className="size-3 shrink-0 text-[#a8adbd]" />}
        <VariableIcon />
        <span className="text-[#F0AC80]">{index}</span>
        <span className="text-[#B6B6B6]">=</span>
        <span className="truncate text-[#bcbec4]">{entryKey} → </span>
        {typeof value === "object" && value !== null ? (
          <span className="truncate text-[#6F737A]">{valuePreview}</span>
        ) : (
          <DebuggerPrimitiveValue data={value} />
        )}
      </button>
      {expanded && (
        <div className="ml-[19px] border-l border-[#56595f] pl-1.5">
          <DebuggerVariableNode name="key" data={entryKey} />
          <DebuggerVariableNode name="value" data={value} />
        </div>
      )}
    </div>
  )
}

export function DebuggerVariableNode({ name, data }: { name: string; data: unknown }) {
  const [expanded, setExpanded] = useState(false)
  const isArray = Array.isArray(data)
  const isMap = typeof data === "object" && data !== null && !isArray
  const children = isArray ? data : []
  const entries = isMap ? Object.entries(data as Record<string, unknown>) : []
  const expandable = (isArray && children.length > 0) || (isMap && entries.length > 0)
  const primitive = !isArray && !isMap

  return (
    <div>
      <button
        type="button"
        onClick={() => expandable && setExpanded((current) => !current)}
        className={cn(
          "flex h-6 w-full items-center gap-0.5 px-1 text-left text-[11px] leading-none hover:bg-[#34363b] focus:bg-[#2e436e] focus:outline-none",
          !expandable && "cursor-default",
        )}
        aria-expanded={expandable ? expanded : undefined}
      >
        <span className="flex w-3 shrink-0 items-center justify-center">
          {expandable && (expanded ? <ChevronDown className="size-3 text-[#a8adbd]" /> : <ChevronRightCollapse className="size-3 text-[#a8adbd]" />)}
        </span>
        <VariableIcon />
        <span className="shrink-0 text-[#F0AC80]">{name}</span>
        <span className="text-[#B6B6B6]">=</span>
        <span className="shrink-0 text-[#6F737A]">{"{"}{debuggerValueType(data)}{"}"}</span>
        {primitive && <DebuggerPrimitiveValue data={data} />}
        {isMap && entries.length === 0 && <span className="text-[#7a7e85]">{"{}"}</span>}
        {isArray && children.length === 0 && <span className="text-[#7a7e85]">[]</span>}
        {isArray && <span className="text-[#7a7e85]">length = {children.length}</span>}
      </button>

      {expanded && (
        <div className="ml-[19px] border-l border-[#56595f] pl-1.5">
          {isMap && entries.map(([entryKey, value], index) => (
            <DebuggerMapEntry key={entryKey} index={index} entryKey={entryKey} value={value} />
          ))}
          {isArray && children.map((value, index) => (
            <DebuggerVariableNode key={index} name={String(index)} data={value} />
          ))}
        </div>
      )}
    </div>
  )
}

export function VariablesPane({ variables }: { variables: Record<string, unknown> }) {
  return (
    <div>
      <div className="mb-1 px-1 text-[11px] font-semibold text-[#a8adbd]">Variables</div>
      <div className="min-h-20 overflow-hidden border border-[#393b40] bg-[#2B2B2B] font-mono">
        {Object.keys(variables).length === 0 ? (
          <div className="px-3 py-2 text-[#7a7e85] italic">No variables</div>
        ) : (
          Object.entries(variables).map(([key, value]) => (
            <DebuggerVariableNode key={key} name={key} data={value} />
          ))
        )}
      </div>
    </div>
  )
}
