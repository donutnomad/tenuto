"use client"

import { createContext, memo, useCallback, useContext, useEffect, useMemo, useState, type KeyboardEvent, type ReactNode } from "react"
import Link from "next/link"
import type { Event, EventType, RetryPolicy } from "@/lib/types"
import { cn } from "@/lib/utils"
import { formatDurationWords, formatEventTimestamp } from "@/lib/format"
import { JsonViewer } from "./json-viewer"
import { ChevronDown, ChevronRight, Info, Search, X } from "lucide-react"
import {
  buildCausalMaps,
  COMMAND_EVENT_TYPES,
  decodePayloads,
  eventDisplayName,
  eventTypeOf,
  filterEventHistory,
  isCausalEventIdField,
  WFT_CLOSE_TYPES,
  type EventHistoryScope,
} from "./event-history-table-logic"

function WorkflowIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path fillRule="evenodd" clipRule="evenodd" d="M12 3.16663C6.47715 3.16663 2 7.64377 2 13.1666H0C0 6.53919 5.37258 1.16663 12 1.16663C18.6274 1.16663 24 6.53919 24 13.1666H22C22 7.64377 17.5229 3.16663 12 3.16663ZM18.3333 13.1666C18.3333 9.66881 15.4978 6.83328 12 6.83328V4.83328C16.6024 4.83328 20.3333 8.56423 20.3333 13.1666C20.3333 17.769 16.6024 21.4999 12 21.4999C7.39763 21.4999 3.66667 17.769 3.66667 13.1666H5.66667C5.66667 16.6644 8.50219 19.4999 12 19.4999C15.4978 19.4999 18.3333 16.6644 18.3333 13.1666ZM12 10.4999C10.5272 10.4999 9.33333 11.6938 9.33333 13.1666C9.33333 14.6394 10.5272 15.8333 12 15.8333C13.4728 15.8333 14.6667 14.6394 14.6667 13.1666C14.6667 11.6938 13.4728 10.4999 12 10.4999ZM7.33333 13.1666C7.33333 10.5893 9.42267 8.49994 12 8.49994C14.5773 8.49994 16.6667 10.5893 16.6667 13.1666C16.6667 15.7439 14.5773 17.8333 12 17.8333C9.42267 17.8333 7.33333 15.7439 7.33333 13.1666Z" fill="currentColor" />
    </svg>
  )
}

function ActivityIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path d="M 10.454314,22.29375 11.949664,24 13.445014,22.29375 22.449664,12 13.445014,1.70625 11.949664,0 10.454314,1.70625 1.4496644,12 Z m 1.49535,-1.7109 L 4.4402894,12 11.949664,3.417195 19.459114,12 Z" fill="currentColor" />
    </svg>
  )
}

function TimerIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path d="M8.25 0H15.75V2.25H13.125V4.56563C15.1594 4.8 17.0062 5.65781 18.4594 6.95156L19.8281 5.57812L20.625 4.78125L22.2141 6.375L21.4172 7.17188L19.9641 8.625C21.0891 10.2141 21.75 12.1547 21.75 14.25C21.75 19.6359 17.3859 24 12 24C6.61406 24 2.25 19.6359 2.25 14.25C2.25 9.24375 6.01875 5.12344 10.875 4.56563V2.25H8.25V0ZM12 21.75C13.9891 21.75 15.8968 20.9598 17.3033 19.5533C18.7098 18.1468 19.5 16.2391 19.5 14.25C19.5 12.2609 18.7098 10.3532 17.3033 8.9467C15.8968 7.54018 13.9891 6.75 12 6.75C10.0109 6.75 8.10322 7.54018 6.6967 8.9467C5.29018 10.3532 4.5 12.2609 4.5 14.25C4.5 16.2391 5.29018 18.1468 6.6967 19.5533C8.10322 20.9598 10.0109 21.75 12 21.75ZM13.125 10.125V15V16.125H10.875V15V10.125V9H13.125V10.125Z" fill="currentColor" />
    </svg>
  )
}

function SignalIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <circle cx="12" cy="18" r="1" fill="currentColor" />
      <path fill="currentColor" d="M4.5 12C4.5 10.2437 5.10312 8.63125 6.1125 7.35313L4.93438 6.42188C3.725 7.95625 3 9.89375 3 12C3 14.1063 3.725 16.0438 4.93438 17.5781L6.1125 16.65C5.10312 15.3687 4.5 13.7563 4.5 12ZM6.5 12C6.5 13.2875 6.94375 14.4719 7.68437 15.4094L8.8625 14.4812C8.32188 13.7969 8 12.9375 8 12C8 11.0625 8.32187 10.2031 8.85938 9.52187L7.68125 8.59375C6.94375 9.52812 6.5 10.7125 6.5 12ZM16.3156 8.59062L15.1375 9.51875C15.6781 10.2031 16 11.0625 16 12C16 12.9375 15.6781 13.7969 15.1406 14.4781L16.3188 15.4062C17.0594 14.4688 17.5031 13.2844 17.5031 11.9969C17.5031 10.7094 17.0594 9.525 16.3188 8.5875L16.3156 8.59062ZM19.5 12C19.5 13.7563 18.8969 15.3688 17.8875 16.6469L19.0656 17.575C20.275 16.0437 21 14.1063 21 12C21 9.89375 20.275 7.95625 19.0656 6.42188L17.8875 7.35C18.8969 8.63125 19.5 10.2437 19.5 12ZM12 13.25C12.3315 13.25 12.6495 13.1183 12.8839 12.8839C13.1183 12.6495 13.25 12.3315 13.25 12C13.25 11.6685 13.1183 11.3505 12.8839 11.1161C12.6495 10.8817 12.3315 10.75 12 10.75C11.6685 10.75 11.3505 10.8817 11.1161 11.1161C10.8817 11.3505 10.75 11.6685 10.75 12C10.75 12.3315 10.8817 12.6495 11.1161 12.8839C11.3505 13.1183 11.6685 13.25 12 13.25Z" />
    </svg>
  )
}

function ChildIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path fillRule="evenodd" clipRule="evenodd" d="M8.55523 0H15.4441V6.88889H12.9997V11H21.4446V17.1111H23.889V24H17.0001V17.1111H19.4446V13H12.9997V17.1111H15.4441V24H8.55523V17.1111H10.9997V13H4.55553V17.1111H6.99997V24H0.111084V17.1111H2.55553V11H10.9997V6.88889H8.55523V0ZM13.4441 4.88889V2H10.5552V4.88889H13.4441ZM2.11108 19.1111V22H4.99997V19.1111H2.11108ZM10.5552 19.1111V22H13.4441V19.1111H10.5552ZM19.0001 19.1111V22H21.889V19.1111H19.0001Z" fill="currentColor" />
    </svg>
  )
}

function UpdateIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path d="M8.2 8.09064L7.69062 8.64064L8.79063 9.66251L9.3 9.11251L11.25 7.00939V14.25V15H12.75V14.25V7.00939L14.7 9.10939L15.2094 9.65939L16.3094 8.63751L15.8 8.08751L12.55 4.58751L12 3.99689L11.45 4.59064L8.2 8.09064ZM12 18.5C8.40937 18.5 5.5 15.5906 5.5 12H4C4 16.4188 7.58125 20 12 20C16.4187 20 20 16.4188 20 12V11.25H18.5V12C18.5 15.5906 15.5906 18.5 12 18.5Z" fill="currentColor" />
    </svg>
  )
}

function OtherIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" className={className} xmlns="http://www.w3.org/2000/svg">
      <path d="M0.595899 3.34165L-0.0791016 2.60415L1.3959 1.25415L2.0709 1.99165L9.40423 9.99165L10.0251 10.6666L9.40423 11.3416L2.0709 19.3416L1.3959 20.0791L-0.0791016 18.7291L0.595899 17.9916L7.3084 10.6666L0.595899 3.34165ZM10.3334 18H23.0001H24.0001V20H23.0001H10.3334H9.3334V18H10.3334Z" fill="currentColor" />
    </svg>
  )
}

// 事件类型（camelCase 字段名）→ 分类。与 reset 面板的 EVENT_CATEGORIES 同源，但这里直接映射到
// category 以便配色（参考 Temporal UI 的 eventTypeCategorizations）。
const EVENT_CATEGORY: Record<EventType, string> = {
  workflowStarted: "workflow",
  workflowCompleted: "workflow",
  workflowFailed: "workflow",
  workflowCancelled: "workflow",
  workflowTerminated: "workflow",
  workflowContinuedAsNew: "workflow",
  workflowCancellationRequested: "workflow",
  workflowTaskScheduled: "workflow",
  workflowTaskStarted: "workflow",
  workflowTaskCompleted: "workflow",
  workflowTaskTimedOut: "workflow",
  workflowTaskFailed: "workflow",
  activityScheduled: "activity",
  activityStarted: "activity",
  activityCompleted: "activity",
  activityFailed: "activity",
  activityAttemptFailed: "activity",
  activityTimedOut: "activity",
  activityReset: "activity",
  activityCancelRequested: "activity",
  activityCancelled: "activity",
  timerScheduled: "timer",
  timerFired: "timer",
  timerCancelled: "timer",
  childWorkflowStarted: "child",
  childWorkflowCompleted: "child",
  childWorkflowFailed: "child",
  childWorkflowCancelled: "child",
  childWorkflowTerminated: "child",
  childWorkflowCancelRequested: "child",
  workflowSignaled: "signal",
  workflowUpdateAccepted: "update",
  workflowUpdateCompleted: "update",
  markerRecorded: "other",
  searchAttributesUpserted: "other",
}

// category → 纯文字颜色（去掉 bg，对标 Temporal 彩色文字风格）。
const CATEGORY_TEXT: Record<string, string> = {
  workflow: "text-blue-600",
  activity: "text-purple-600",
  child: "text-cyan-600",
  timer: "text-yellow-600",
  signal: "text-pink-600",
  update: "text-indigo-600",
  other: "text-slate-500",
}

// category → SVG 图标组件。
const CATEGORY_ICON: Record<string, (props: { className?: string }) => ReactNode> = {
  workflow: WorkflowIcon,
  activity: ActivityIcon,
  timer: TimerIcon,
  child: ChildIcon,
  signal: SignalIcon,
  update: UpdateIcon,
  other: OtherIcon,
}

// 浮层图例：分类 + 名称 + 说明。
const LEGEND: { category: string; label: string; desc: string }[] = [
  { category: "workflow", label: "Workflow",       desc: "A Workflow Task is a Task that contains the context needed to make progress with a Workflow Execution." },
  { category: "activity", label: "Activity",       desc: "An Activity is a normal function or method execution that's intended to execute a single, well-defined action." },
  { category: "timer",    label: "Timer",          desc: "Timers are durable timers set for a fixed time period." },
  { category: "signal",   label: "Signal",         desc: "Signals are asynchronous write requests. They cause changes in the running Workflow, but you cannot await any response or error." },
  { category: "child",    label: "Child Workflow",  desc: "A Child Workflow Execution is a Workflow Execution that is spawned from within another Workflow in the same Namespace." },
  { category: "update",   label: "Update",         desc: "Updates are synchronous, tracked write requests. The sender of the Update can wait for a response on completion or an error on failure." },
  { category: "other",    label: "Other",          desc: "Marker Recorded, Upsert Search Attributes and Workflow Properties Modified." },
]

function categoryOf(eventType: string): string {
  return eventType in EVENT_CATEGORY ? EVENT_CATEGORY[eventType as EventType] : "other"
}

// eventType → Details 列显示的字段（label + 后端真实字段名 + 值样式）。
// 未在此表中的事件类型（如 workflowTaskScheduled）Details 留空。
const DETAIL_FIELD: Partial<Record<EventType, { label: string; field: string; style: "box" | "underline" | "badge" | "text" }>> = {
  workflowStarted:          { label: "Workflow",      field: "workflowId",     style: "badge" },
  workflowCompleted:        { label: "Result",        field: "output",         style: "box" },
  workflowFailed:           { label: "Error",         field: "error",          style: "text" },
  workflowTerminated:       { label: "Reason",        field: "reason",         style: "text" },
  workflowContinuedAsNew:   { label: "Next Run",      field: "nextRunId",      style: "underline" },
  workflowTaskStarted:      { label: "History Size Bytes", field: "historySizeBytes", style: "badge" },
  workflowTaskCompleted:    { label: "Identity",      field: "workerIdentity", style: "badge" },
  workflowTaskTimedOut:     { label: "Error",         field: "error",          style: "text" },
  workflowTaskFailed:       { label: "Cause",         field: "cause",          style: "text" },
  activityScheduled:        { label: "Activity Type", field: "activityType",   style: "badge" },
  activityStarted:          { label: "Attempt",       field: "attempt",        style: "badge" },
  activityCompleted:        { label: "Result",        field: "output",         style: "box" },
  activityFailed:           { label: "Error",         field: "error",          style: "text" },
  activityAttemptFailed:    { label: "Error",         field: "error",          style: "text" },
  activityTimedOut:         { label: "Timeout",       field: "timeoutType",    style: "text" },
  activityReset:            { label: "Reason",        field: "reason",         style: "text" },
  activityCancelRequested:  { label: "Activity ID",   field: "activityId",         style: "badge" },
  activityCancelled:        { label: "Activity ID",   field: "activityId",         style: "badge" },
  timerScheduled:           { label: "Timer ID",      field: "timerId",        style: "underline" },
  timerFired:               { label: "Timer ID",      field: "timerId",        style: "underline" },
  timerCancelled:           { label: "Timer ID",      field: "timerId",        style: "underline" },
  childWorkflowStarted:     { label: "Child",         field: "name",           style: "badge" },
  childWorkflowCompleted:   { label: "Child ID",      field: "childId",        style: "underline" },
  childWorkflowFailed:      { label: "Error",         field: "error",          style: "text" },
  childWorkflowCancelled:   { label: "Child ID",      field: "childId",        style: "underline" },
  childWorkflowTerminated:  { label: "Reason",        field: "reason",         style: "text" },
  childWorkflowCancelRequested: { label: "Child ID",    field: "childId",        style: "underline" },
  workflowSignaled:         { label: "Signal",        field: "name",           style: "badge" },
  workflowUpdateAccepted:   { label: "Update",        field: "name",           style: "badge" },
  workflowUpdateCompleted:  { label: "Update",        field: "name",           style: "badge" },
  markerRecorded:           { label: "Marker",        field: "markerName",     style: "badge" },
  searchAttributesUpserted: { label: "Attributes",    field: "attributes",     style: "text" },
}

const VALUE_STYLE: Record<string, string> = {
  box:       "rounded border border-border px-1.5 py-0.5 font-mono text-[#141414] truncate",
  underline: "underline underline-offset-2 font-mono text-[#141414] truncate",
  badge:     "rounded bg-blue-100 px-1.5 py-0.5 font-mono text-[#141414] truncate",
  text:      "font-mono text-[#141414] truncate",
}

function summarize(eventType: string, attrs: Record<string, unknown>): { label: string; value: string; style: string } | null {
  const mapping = DETAIL_FIELD[eventType as EventType]
  if (!mapping) return null
  const raw = attrs[mapping.field]
  if (raw == null || raw === "") return null
  const value = typeof raw === "object" ? JSON.stringify(raw) : String(raw)
  return { label: mapping.label, value, style: mapping.style }
}

// 字段名末尾是 Ms 的（毫秒字段）
function isMsField(key: string): boolean {
  return key.endsWith("Ms")
}

// 去掉字段名末尾的 Ms 后再 displayName
function fieldLabel(key: string): string {
  const cleaned = isMsField(key) ? key.slice(0, -2) : key
  return eventDisplayName(cleaned)
}

// runId 类字段（展示为链接）
const RUN_ID_FIELDS = new Set([
  "firstRunId",
  "continuedRunId",
  "nextRunId",
  "resetRunId",
  "childRunId",
  "baseRunId",
])

function isScalar(v: unknown): v is string | number | boolean {
  return typeof v === "string" || typeof v === "number" || typeof v === "boolean"
}

// 展开行里的因果事件链接需要"已加载事件集合"和跳转回调。走 context 传给 CausalEventLink，
// 让 memo 的 EventRow 不因集合变化（每次轮询都会变）而重渲染。
const EventLinkContext = createContext<{
  eventIds: Set<number>
  onNavigateToEvent: (eventId: number) => void
}>({ eventIds: new Set(), onNavigateToEvent: () => {} })

function CausalEventLink({ eventId }: { eventId: number }) {
  const { eventIds, onNavigateToEvent } = useContext(EventLinkContext)
  if (!eventIds.has(eventId)) return <span className="font-mono truncate">{eventId}</span>
  return (
    <button
      type="button"
      className="w-fit font-mono text-blue-600 underline underline-offset-2 hover:text-blue-800"
      aria-label={`Go to event ${eventId}`}
      onClick={(clickEvent) => {
        clickEvent.stopPropagation()
        onNavigateToEvent(eventId)
      }}
    >
      {eventId}
    </button>
  )
}

function RetryPolicyRows({ policy }: { policy: RetryPolicy }) {
  const rows: { label: string; value: string }[] = []

  if (policy.initialIntervalMs != null) {
    const v = formatDurationWords(policy.initialIntervalMs)
    if (v) rows.push({ label: "Initial Interval", value: v })
  }
  if (policy.backoffCoefficient != null) {
    rows.push({ label: "Backoff Coefficient", value: String(policy.backoffCoefficient) })
  }
  if (policy.maxIntervalMs != null) {
    const v = formatDurationWords(policy.maxIntervalMs)
    if (v) rows.push({ label: "Maximum Interval", value: v })
  }
  if (policy.maxAttempts != null) {
    rows.push({
      label: "Maximum Attempts",
      value: policy.maxAttempts === 0 ? "Unlimited" : String(policy.maxAttempts),
    })
  }
  if (policy.nonRetryableErrors && policy.nonRetryableErrors.length > 0) {
    rows.push({ label: "Non Retryable Errors", value: policy.nonRetryableErrors.join(", ") })
  }

  if (rows.length === 0) return null

  return (
    <>
      {rows.map(({ label, value }) => (
        <div key={label} className="contents">
          <span className="text-muted-foreground">{label}</span>
          <span className="font-mono truncate">{value}</span>
        </div>
      ))}
    </>
  )
}

const EventRow = memo(function EventRow({
  event,
  striped,
  namespace,
  highlighted,
  sourceWftId,
  wftStartedId,
  acceptedEventId,
}: {
  event: Event
  striped: boolean
  namespace: string
  highlighted: boolean
  sourceWftId?: number
  wftStartedId?: number
  acceptedEventId?: number
}) {
  const [expanded, setExpanded] = useState(false)
  const type = eventTypeOf(event)
  const category = categoryOf(type)
  const attrs = (event[type as keyof Event] ?? {}) as Record<string, unknown>

  // retryPolicy 单独处理，不进 payloadFields
  const retryPolicy = attrs["retryPolicy"] as RetryPolicy | undefined

  // executionTimeoutMs 单独处理（workflowStarted 没设时显示 Unlimited），不进 scalarFields 避免重复。
  const isWorkflowStarted = type === "workflowStarted"
  const scalarFields = Object.entries(attrs).filter(
    ([k, v]) => isScalar(v) && !(isWorkflowStarted && k === "executionTimeoutMs")
  )
  const payloadFields = Object.entries(attrs).filter(
    ([k, v]) => v != null && typeof v === "object" && k !== "retryPolicy"
  )

  const isCommandEvent = COMMAND_EVENT_TYPES.has(type)
  const isWftCloseEvent = WFT_CLOSE_TYPES.has(type)
  const isUpdateCompleted = type === "workflowUpdateCompleted"

  const Icon = CATEGORY_ICON[category] ?? OtherIcon
  const textColor = CATEGORY_TEXT[category] ?? "text-slate-500"
  const summary = summarize(type, attrs)

  // 判断展开区域是否有内容
  const hasCausalInfo =
    (isCommandEvent && sourceWftId != null && sourceWftId > 0) ||
    (isWftCloseEvent && wftStartedId != null && wftStartedId > 0) ||
    (isUpdateCompleted && acceptedEventId != null && acceptedEventId > 0)
  const hasContent =
    scalarFields.length > 0 || retryPolicy != null || payloadFields.length > 0 || hasCausalInfo || isWorkflowStarted
  const detailsId = `event-history-details-${event.id}`

  const toggleExpanded = () => setExpanded((previous) => !previous)
  const handleKeyDown = (keyboardEvent: KeyboardEvent<HTMLTableRowElement>) => {
    if (keyboardEvent.target !== keyboardEvent.currentTarget) return
    if (keyboardEvent.key !== "Enter" && keyboardEvent.key !== " ") return
    keyboardEvent.preventDefault()
    toggleExpanded()
  }

  return (
    <>
      <tr
        id={`event-history-row-${event.id}`}
        tabIndex={0}
        role="button"
        aria-expanded={expanded}
        aria-controls={detailsId}
        aria-label={`${expanded ? "Collapse" : "Expand"} event ${event.id}: ${eventDisplayName(type)}`}
        className={cn(
          "cursor-pointer hover:bg-muted/40 transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-500 focus-visible:outline-offset-[-2px]",
          striped && "bg-[rgb(248,250,252)]",
          highlighted && "bg-blue-100 outline outline-2 outline-blue-400 outline-offset-[-2px]"
        )}
        onClick={toggleExpanded}
        onKeyDown={handleKeyDown}
      >
        <td className="px-3 py-px align-top w-px whitespace-nowrap">
          <div className="flex items-center gap-1.5">
            {expanded ? (
              <ChevronDown className="size-4 shrink-0 text-[#141414]" />
            ) : (
              <ChevronRight className="size-4 shrink-0 text-[#141414]" />
            )}
            <span className="font-mono text-sm text-[#141414] tabular-nums underline underline-offset-2">{event.id}</span>
          </div>
        </td>
        <td className="px-3 py-px align-top text-sm text-[#141414] tabular-nums w-px whitespace-nowrap">
          {formatEventTimestamp(event.timestamp)}
        </td>
        <td className="px-3 py-px align-top w-px whitespace-nowrap">
          <span className={cn("inline-flex items-center gap-1.5 text-base font-medium", textColor)}>
            <Icon className="size-4 shrink-0" />
            {eventDisplayName(type)}
          </span>
        </td>
        <td className="px-3 py-px align-top text-xs w-full">
          {summary && (
            <span className="inline-flex items-center gap-1.5 truncate">
              <span className="text-[#141414] shrink-0">{summary.label}</span>
              <span className={VALUE_STYLE[summary.style]}>{summary.value}</span>
            </span>
          )}
        </td>
      </tr>
      {expanded && (
        <tr id={detailsId} className="bg-muted/10">
          <td colSpan={4} className="px-6 py-3">
            {hasContent ? (
              <>
                {(scalarFields.length > 0 || retryPolicy != null || hasCausalInfo) && (
                  <div className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 text-xs mb-3">
                    {scalarFields.map(([k, v]) => {
                      // 毫秒字段：格式化，0 时隐藏整行
                      if (isMsField(k) && typeof v === "number") {
                        const formatted = formatDurationWords(v)
                        if (!formatted) return null
                        return (
                          <div key={k} className="contents">
                            <span className="text-muted-foreground">{fieldLabel(k)}</span>
                            <span className="font-mono truncate">{formatted}</span>
                          </div>
                        )
                      }
                      // runId 类字段：链接
                      if (RUN_ID_FIELDS.has(k) && typeof v === "string") {
                        return (
                          <div key={k} className="contents">
                            <span className="text-muted-foreground">{fieldLabel(k)}</span>
                            <Link
                              href={`/${namespace}/runs/${v}`}
                              className="font-mono text-blue-600 hover:underline truncate"
                              onClick={(e) => e.stopPropagation()}
                            >
                              {v}
                            </Link>
                          </div>
                        )
                      }
                      if (isCausalEventIdField(k) && typeof v === "number") {
                        return (
                          <div key={k} className="contents">
                            <span className="text-muted-foreground">{fieldLabel(k)}</span>
                            <CausalEventLink eventId={v} />
                          </div>
                        )
                      }
                      // 普通标量
                      return (
                        <div key={k} className="contents">
                          <span className="text-muted-foreground">{fieldLabel(k)}</span>
                          <span className="font-mono truncate">{String(v)}</span>
                        </div>
                      )
                    })}
                    {retryPolicy != null && (
                      <RetryPolicyRows policy={retryPolicy} />
                    )}
                    {isWorkflowStarted && (
                      <div className="contents">
                        <span className="text-muted-foreground">Execution Timeout</span>
                        <span className="font-mono truncate">
                          {(() => {
                            const ms = attrs["executionTimeoutMs"] as number | undefined
                            return ms && ms > 0 ? formatDurationWords(ms) : "Unlimited"
                          })()}
                        </span>
                      </div>
                    )}
                    {isCommandEvent && sourceWftId != null && sourceWftId > 0 && (
                      <div className="contents">
                        <span className="text-muted-foreground">Source WFT Completed Event ID</span>
                        <CausalEventLink eventId={sourceWftId} />
                      </div>
                    )}
                    {isWftCloseEvent && wftStartedId != null && wftStartedId > 0 && (
                      <div className="contents">
                        <span className="text-muted-foreground">Started Event ID</span>
                        <CausalEventLink eventId={wftStartedId} />
                      </div>
                    )}
                    {isUpdateCompleted && acceptedEventId != null && acceptedEventId > 0 && (
                      <div className="contents">
                        <span className="text-muted-foreground">Accepted Event ID</span>
                        <CausalEventLink eventId={acceptedEventId} />
                      </div>
                    )}
                  </div>
                )}
                {payloadFields.map(([k, v]) => (
                  <div key={k} className="mt-2">
                    <JsonViewer data={decodePayloads(v)} title={eventDisplayName(k)} />
                  </div>
                ))}
              </>
            ) : (
              <p className="text-xs text-muted-foreground">No attributes.</p>
            )}
          </td>
        </tr>
      )}
    </>
  )
})

export function EventHistoryTable({
  events,
  namespace,
  loadedEvents = events.length,
  totalEvents = events.length,
}: {
  events: Event[]
  namespace: string
  loadedEvents?: number
  totalEvents?: number
}) {
  const [query, setQuery] = useState("")
  const [scope, setScope] = useState<EventHistoryScope>("all")
  const [failedOnly, setFailedOnly] = useState(false)
  const [highlightedEventId, setHighlightedEventId] = useState<number | null>(null)
  const causalMaps = useMemo(() => buildCausalMaps(events), [events])
  const eventIds = useMemo(() => new Set(events.map((event) => event.id)), [events])
  const filteredEvents = useMemo(
    () => filterEventHistory(events, { query, scope, failedOnly }),
    [events, failedOnly, query, scope]
  )
  const filtersActive = query.trim().length > 0 || scope !== "all" || failedOnly

  useEffect(() => {
    if (highlightedEventId == null) return
    const row = document.getElementById(`event-history-row-${highlightedEventId}`)
    if (!row) return
    row.scrollIntoView({ behavior: "smooth", block: "center" })
    row.focus({ preventScroll: true })
    const timeout = window.setTimeout(() => setHighlightedEventId(null), 1800)
    return () => window.clearTimeout(timeout)
  }, [filteredEvents, highlightedEventId])

  const clearFilters = useCallback(() => {
    setQuery("")
    setScope("all")
    setFailedOnly(false)
  }, [])

  const navigateToEvent = useCallback((eventId: number) => {
    clearFilters()
    setHighlightedEventId(eventId)
  }, [clearFilters])

  const eventLinks = useMemo(
    () => ({ eventIds, onNavigateToEvent: navigateToEvent }),
    [eventIds, navigateToEvent],
  )

  if (events.length === 0) {
    return <div className="py-8 text-center text-sm text-muted-foreground">No events recorded yet.</div>
  }

  // 斑马纹按"从最旧一端数起"的位置计算：新事件接在最新一端时，已有行的条纹不变，
  // memo 的 EventRow 就不会因为新事件到达而整表重渲染。
  const newestFirst = filteredEvents.length > 1 && filteredEvents[0].id > filteredEvents[filteredEvents.length - 1].id

  const scopeButtonClass = (value: EventHistoryScope) =>
    cn(
      "rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
      scope === value
        ? "border-blue-600 bg-blue-50 text-blue-700"
        : "border-border bg-white text-[#141414] hover:bg-muted/60"
    )

  return (
    <div className="rounded-lg border border-border">
      <div className="flex flex-wrap items-end gap-2 border-b border-border bg-muted/20 p-3">
        <label className="min-w-[240px] flex-1">
          <span className="mb-1 block text-xs font-medium text-[#141414]">Search loaded events</span>
          <span className="relative block">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <input
              type="search"
              value={query}
              aria-label="Search Event Histories"
              placeholder="Event ID, type, activity, step, worker, error…"
              className="h-8 w-full rounded-md border border-border bg-white pl-8 pr-8 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-100"
              onChange={(inputEvent) => setQuery(inputEvent.target.value)}
            />
            {query.length > 0 && (
              <button
                type="button"
                aria-label="Clear event search"
                className="absolute right-1.5 top-1/2 -translate-y-1/2 rounded p-1 text-muted-foreground hover:bg-muted hover:text-[#141414]"
                onClick={() => setQuery("")}
              >
                <X className="size-3.5" />
              </button>
            )}
          </span>
        </label>
        <div className="flex flex-wrap items-center gap-1.5" aria-label="Event history quick filters">
          <button
            type="button"
            className={scopeButtonClass("all")}
            aria-pressed={scope === "all"}
            onClick={() => setScope("all")}
          >
            All
          </button>
          <button
            type="button"
            className={scopeButtonClass("workflowTasks")}
            aria-pressed={scope === "workflowTasks"}
            onClick={() => setScope("workflowTasks")}
          >
            Workflow Tasks
          </button>
          <button
            type="button"
            className={scopeButtonClass("activityAttempts")}
            aria-pressed={scope === "activityAttempts"}
            onClick={() => setScope("activityAttempts")}
          >
            Activity Attempts
          </button>
          <button
            type="button"
            className={cn(
              "rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
              failedOnly
                ? "border-red-600 bg-red-50 text-red-700"
                : "border-border bg-white text-[#141414] hover:bg-muted/60"
            )}
            aria-pressed={failedOnly}
            onClick={() => setFailedOnly((active) => !active)}
          >
            Failed only
          </button>
        </div>
        <span className="ml-auto whitespace-nowrap text-xs text-muted-foreground" aria-live="polite">
          {filteredEvents.length} matches · {loadedEvents} loaded
          {totalEvents > loadedEvents && ` of ${totalEvents}`}
        </span>
        {filtersActive && (
          <button type="button" className="text-xs font-medium text-blue-600 hover:underline" onClick={clearFilters}>
            Clear
          </button>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[640px] text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/50 text-left text-sm font-semibold text-[#141414]">
            <th className="px-3 py-2 font-medium w-px whitespace-nowrap">Event ID</th>
            <th className="px-3 py-2 font-medium w-px whitespace-nowrap">Time</th>
            <th className="px-3 py-2 font-medium w-px whitespace-nowrap">
              <span className="inline-flex items-center gap-1">
                Event Type
                <span className="group relative">
                  <Info className="size-3.5 text-muted-foreground cursor-pointer" />
                  {/* 隐形桥：pt-2 透明区让鼠标从 Info 移向浮层时路径上始终有可 hover 元素，浮层不闪退 */}
                  <div className="invisible group-hover:visible absolute z-50 left-0 top-full pt-2">
                    <div className="w-[200px] whitespace-normal rounded-lg border border-border bg-white shadow-md p-2 text-xs font-normal">
                      <p className="font-semibold mb-1.5 text-[#141414]">Event Types</p>
                      {LEGEND.map(({ category, label, desc }) => {
                        const LegendIcon = CATEGORY_ICON[category] ?? OtherIcon
                        return (
                          <div key={category} className="group/item relative flex items-center gap-2 rounded px-1 py-0.5 hover:bg-muted/50">
                            <LegendIcon className={cn("h-3.5 w-3.5 shrink-0", CATEGORY_TEXT[category])} />
                            <span className="font-medium text-[#141414]">{label}</span>
                            {/* 二级 tip：hover 该行时右侧弹出描述。表头 th 是 whitespace-nowrap，
                                这里必须 whitespace-normal break-words 强制换行，否则长描述溢出框外。 */}
                            <div className="invisible group-hover/item:visible absolute z-50 left-full top-0 pl-2">
                              <div className="w-[240px] whitespace-normal break-words rounded-md border border-border bg-white shadow-md p-2 text-[#141414] leading-snug">
                                {desc}
                              </div>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                </span>
              </span>
            </th>
            <th className="px-3 py-2 font-medium w-full">Details</th>
          </tr>
        </thead>
        <tbody>
          <EventLinkContext.Provider value={eventLinks}>
          {filteredEvents.map((event, idx) => (
            <EventRow
              key={event.id}
              event={event}
              striped={(newestFirst ? filteredEvents.length - 1 - idx : idx) % 2 === 0}
              namespace={namespace}
              highlighted={highlightedEventId === event.id}
              sourceWftId={causalMaps.sourceWft[event.id]}
              wftStartedId={causalMaps.wftStarted[event.id]}
              acceptedEventId={causalMaps.updateAccepted[event.id]}
            />
          ))}
          </EventLinkContext.Provider>
          {filteredEvents.length === 0 && (
            <tr>
              <td colSpan={4} className="px-4 py-10 text-center text-sm text-muted-foreground">
                No events match the current search and filters.
              </td>
            </tr>
          )}
        </tbody>
        </table>
      </div>
    </div>
  )
}
