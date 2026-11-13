import type { Event, EventType, Failure, Run, RunStatus, Step, Attempt } from "@/lib/types"

export const RUN_DETAIL_EVENT_CATEGORIES: Record<string, EventType[]> = {
  Activity: [
    "activityScheduled",
    "activityStarted",
    "activityCompleted",
    "activityFailed",
    "activityAttemptFailed",
    "activityTimedOut",
    "activityReset",
    "activityCancelRequested",
    "activityCancelled",
  ],
  Workflow: [
    "workflowStarted",
    "workflowCompleted",
    "workflowFailed",
    "workflowCancelled",
    "workflowTerminated",
    "workflowContinuedAsNew",
    "workflowCancellationRequested",
  ],
  Timer: ["timerScheduled", "timerFired", "timerCancelled"],
  "Child Workflow": [
    "childWorkflowStarted",
    "childWorkflowCompleted",
    "childWorkflowFailed",
    "childWorkflowCancelled",
    "childWorkflowTerminated",
    "childWorkflowCancelRequested",
  ],
  Signal: ["workflowSignaled"],
  Update: ["workflowUpdateAccepted", "workflowUpdateCompleted"],
  Other: [
    "markerRecorded",
    "searchAttributesUpserted",
    "workflowTaskScheduled",
    "workflowTaskStarted",
    "workflowTaskCompleted",
    "workflowTaskTimedOut",
    "workflowTaskFailed",
  ],
}

const TERMINAL_RUN_STATUSES = new Set<RunStatus>([
  "completed",
  "failed",
  "cancelled",
  "terminated",
  "continued_as_new",
])

export function isTerminalRunStatus(status: RunStatus): boolean {
  return TERMINAL_RUN_STATUSES.has(status)
}

export function pendingActivitySteps(steps: Step[]): Step[] {
  return steps.filter(
    (step) =>
      step.childRunID === null &&
      (step.status === "scheduled" || step.status === "running"),
  )
}

// 最后一条事件的时间；终态 Run 用它作为结束时间。
export function runEndTime(events: Event[]): string | null {
  return events.at(-1)?.timestamp ?? null
}

// 第一条 ActivityScheduled 事件上的 Task Queue。
export function runTaskQueue(events: Event[]): string | null {
  for (const event of events) {
    if (event.activityScheduled?.taskQueue) return event.activityScheduled.taskQueue
  }
  return null
}

// 运行中的 Run 用当前时间计算已运行时长（nowMs 为 0 表示时钟尚未就绪）；
// 已结束的 Run 用结束时间。
export function runDurationMs(
  run: Pick<Run, "status" | "created_at">,
  endTime: string | null,
  nowMs: number,
): number | null {
  const start = new Date(run.created_at).getTime()
  if (run.status === "running") return nowMs > 0 ? nowMs - start : null
  return endTime ? new Date(endTime).getTime() - start : null
}

export function lastFailedAttempt(step: Pick<Step, "attempts">): Attempt | null {
  return step.attempts.findLast((attempt) => attempt.status === "failed" || attempt.status === "timed_out") ?? null
}

// runConfigErrorBanner 描述确定性配置/契约/target 冲突导致的暂停状态。
// config_error 只在人工 set-policy 后清空，因此它同时是恢复入口的提示来源。
export function runConfigErrorBanner(
  run: Pick<Run, "status" | "config_error">,
): { title: string; description: string; detail: string } | null {
  if (!run.config_error) return null
  const description = isTerminalRunStatus(run.status)
    ? "This run is closed. The recorded conflict is kept as a diagnostic."
    : "Workflow task dispatch is paused until the conflict is fixed. Resume with the latest content, or pin the current content to keep it."
  return {
    title: "Configuration paused: a deterministic configuration or contract conflict stopped this run.",
    description,
    detail: run.config_error,
  }
}

// runUpgradeErrorBanner 描述最近一次 AutoUpgrade 重放失败的诊断。
// 运行中按退避重试；终态 Run 不再重试，改成只陈述历史诊断。
export function runUpgradeErrorBanner(
  run: Pick<Run, "status" | "upgrade_error">,
): { title: string; description: string; detail: string } | null {
  if (!run.upgrade_error) return null
  if (isTerminalRunStatus(run.status)) {
    return {
      title: "Last AutoUpgrade attempt failed.",
      description: "This run is closed; the failed attempt is kept as a diagnostic. The current content is the last successful version.",
      detail: run.upgrade_error,
    }
  }
  return {
    title: "AutoUpgrade retrying: the latest content failed replay.",
    description: "The workflow task retries with backoff; current content is the last successful version.",
    detail: run.upgrade_error,
  }
}

// runFailureSummary 是失败 Run 的终态失败：get-run 的 terminal_failure（message、type、cause 链）。
// Workflow 级超时的 timeoutType 只写在 WorkflowFailed 事件上，从已加载的事件补上。
export function runFailureSummary(
  run: Pick<Run, "status" | "terminal_failure">,
  events: Event[],
): { failure: Failure; timeoutType?: string; causes: Failure[] } | null {
  if (run.status !== "failed" || !run.terminal_failure) return null
  const failure = run.terminal_failure
  const timeoutType = failure.timeoutType || events.findLast((event) => event.workflowFailed)?.workflowFailed?.timeoutType
  const causes: Failure[] = []
  for (let cause = failure.cause; cause; cause = cause.cause) causes.push(cause)
  return { failure, timeoutType: timeoutType || undefined, causes }
}
