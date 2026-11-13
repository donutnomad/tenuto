"use client"

import { useState } from "react"
import { ChevronDown, ChevronRight as ChevronRightCollapse } from "lucide-react"
import { cn } from "@/lib/utils"

// 可折叠 JSON 树。debuggerTheme 为 true 时使用 Trace 调试器的深色配色。
export function JsonTree({
  data,
  depth = 0,
  debuggerTheme = false,
}: {
  data: unknown
  depth?: number
  debuggerTheme?: boolean
}) {
  const [expanded, setExpanded] = useState(depth <= 2)

  if (data === null) return <span className={debuggerTheme ? "text-[#7a7e85] italic" : "text-gray-400 italic"}>null</span>
  if (data === undefined) return <span className={debuggerTheme ? "text-[#7a7e85] italic" : "text-gray-400 italic"}>undefined</span>
  if (typeof data === "string") return <span className={debuggerTheme ? "text-[#6aab73]" : "text-green-600"}>&quot;{data}&quot;</span>
  if (typeof data === "number") return <span className={debuggerTheme ? "text-[#2aacb8]" : "text-blue-600"}>{String(data)}</span>
  if (typeof data === "boolean") return <span className={debuggerTheme ? "text-[#cf8e6d]" : "text-purple-600"}>{String(data)}</span>

  if (Array.isArray(data)) {
    if (data.length === 0) return <span className={debuggerTheme ? "text-[#a8adbd]" : "text-gray-500"}>[]</span>
    return (
      <span>
        <button
          type="button"
          onClick={() => setExpanded(!expanded)}
          className={cn(
            "inline-flex items-center gap-0.5",
            debuggerTheme ? "text-[#a8adbd] hover:text-[#dfe1e5]" : "text-gray-500 hover:text-gray-800",
          )}
        >
          {expanded ? <ChevronDown className="size-3" /> : <ChevronRightCollapse className="size-3" />}
          {expanded ? "[" : `[${data.length} items]`}
        </button>
        {expanded && (
          <div className="ml-4">
            {data.map((item, i) => (
              <div key={i} className="leading-relaxed">
                <JsonTree data={item} depth={depth + 1} debuggerTheme={debuggerTheme} />
                {i < data.length - 1 && <span className={debuggerTheme ? "text-[#7a7e85]" : "text-gray-400"}>,</span>}
              </div>
            ))}
          </div>
        )}
        {expanded && <span className={debuggerTheme ? "text-[#a8adbd]" : "text-gray-500"}>]</span>}
      </span>
    )
  }

  if (typeof data === "object") {
    const entries = Object.entries(data as Record<string, unknown>)
    if (entries.length === 0) return <span className={debuggerTheme ? "text-[#a8adbd]" : "text-gray-500"}>{"{}"}</span>
    return (
      <span>
        <button
          type="button"
          onClick={() => setExpanded(!expanded)}
          className={cn(
            "inline-flex items-center gap-0.5",
            debuggerTheme ? "text-[#a8adbd] hover:text-[#dfe1e5]" : "text-gray-500 hover:text-gray-800",
          )}
        >
          {expanded ? <ChevronDown className="size-3" /> : <ChevronRightCollapse className="size-3" />}
          {expanded ? "{" : "{ ... }"}
        </button>
        {expanded && (
          <div className="ml-4">
            {entries.map(([key, value], i) => (
              <div key={key} className="leading-relaxed">
                <span className={debuggerTheme ? "font-semibold text-[#bcbec4]" : "text-gray-700 font-semibold"}>{key}</span>
                <span className={debuggerTheme ? "text-[#7a7e85]" : "text-gray-400"}>: </span>
                <JsonTree data={value} depth={depth + 1} debuggerTheme={debuggerTheme} />
                {i < entries.length - 1 && <span className={debuggerTheme ? "text-[#7a7e85]" : "text-gray-400"}>,</span>}
              </div>
            ))}
          </div>
        )}
        {expanded && <span className={debuggerTheme ? "text-[#a8adbd]" : "text-gray-500"}>{"}"}</span>}
      </span>
    )
  }

  return <span className={debuggerTheme ? "text-[#bcbec4]" : "text-gray-500"}>{String(data)}</span>
}
