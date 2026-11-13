// Schedule 启动的 Run 带 Server 维护的搜索属性 TemporalScheduledById（值是 Schedule ID）；continue-as-new 与
// 重试产生的 Run 继承它。Runs 页面的 ?schedule_id= 按它过滤，Schedules 页面由此链接到某个 Schedule 的全部 Run。
export const SCHEDULED_BY_ID_ATTRIBUTE = "TemporalScheduledById"

export function scheduleRunsHref(namespace: string, scheduleID: string): string {
  return `/${encodeURIComponent(namespace)}/runs?schedule_id=${encodeURIComponent(scheduleID)}`
}

// scheduleRunsFilter 是 listRuns 的 search_attributes 条件；没有 Schedule 时不过滤。
export function scheduleRunsFilter(scheduleID: string): Record<string, string> | undefined {
  return scheduleID ? { [SCHEDULED_BY_ID_ATTRIBUTE]: scheduleID } : undefined
}
