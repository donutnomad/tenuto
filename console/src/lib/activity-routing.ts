import type { ActivityRef, WorkerListItem } from "./types"

// ActivityRouting 是一个 Activity 类型在当前在线 Worker 上的分布：每个 Worker 在它拉取的每个队列上都提供它登记的全部类型。
export interface ActivityRouting {
  activityType: string
  // queues 按名称排序；defaultQueues 是登记为默认路由的 Worker 所在的队列。
  queues: string[]
  defaultQueues: string[]
  workers: number
}

// activityRoutings 按 Activity 类型汇总 Worker 列表，按类型名排序。
// Program 没有声明队列的 Activity 由 Server 在这些队列里选择：优先默认路由，契约不同的路由会被排除。
export function activityRoutings(workers: WorkerListItem[]): ActivityRouting[] {
  const byType = new Map<string, { queues: Set<string>; defaultQueues: Set<string>; workers: Set<string> }>()
  for (const worker of workers) {
    const queues = worker.task_queues?.length ? worker.task_queues : ["default"]
    for (const activityType of worker.activity_types ?? []) {
      let entry = byType.get(activityType)
      if (!entry) {
        entry = { queues: new Set(), defaultQueues: new Set(), workers: new Set() }
        byType.set(activityType, entry)
      }
      entry.workers.add(`${worker.identity ?? ""}\u0000${worker.build_id ?? ""}`)
      for (const queue of queues) {
        entry.queues.add(queue)
        if (worker.default) entry.defaultQueues.add(queue)
      }
    }
  }
  return [...byType.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([activityType, entry]) => ({
      activityType,
      queues: [...entry.queues].sort(),
      defaultQueues: [...entry.defaultQueues].sort(),
      workers: entry.workers.size,
    }))
}

// TaskQueueSource 说明一次 Activity 调度的队列从哪里来。
//   - declared：Run 的 Workflow 定义（workflow.json 的 activities）声明了这个队列；
//   - call-site：定义声明了别的队列，调用处的 ActivityOptions.TaskQueue 改派到了这里；
//   - undeclared：定义没有声明队列，队列来自调用处的 ActivityOptions.TaskQueue，或由 Server 按 Worker 登记选择。
export type TaskQueueSource = "declared" | "call-site" | "undeclared"

// taskQueueSource 按 Run 当前的 Workflow 定义判断调度事件里队列的来源；拿不到定义时返回 null。
export function taskQueueSource(
  taskQueue: string,
  activityType: string,
  activities: Record<string, ActivityRef> | undefined,
): TaskQueueSource | null {
  if (!activities || !activityType) return null
  const ref = Object.values(activities).find((candidate) => candidate.type === activityType)
  if (!ref?.task_queue) return "undeclared"
  return ref.task_queue === (taskQueue || "default") ? "declared" : "call-site"
}

export const TASK_QUEUE_SOURCE_LABEL: Record<TaskQueueSource, string> = {
  declared: "declared in workflow definition",
  "call-site": "overridden at call site",
  undeclared: "not declared: call-site option or chosen from worker registrations",
}
