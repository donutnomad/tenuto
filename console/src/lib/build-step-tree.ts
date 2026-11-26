import type { ActivityState, Attempt, Event, Step } from './types'

// 按 attempt 号在 step.attempts 里找/建一条完整记录。
// 事件按 history 顺序到达，首见某号即 push → attempts 天然按号递增有序。
function getOrCreateAttempt(step: Step, n: number): Attempt {
  let a = step.attempts.find((x) => x.attempt === n)
  if (!a) {
    a = { attempt: n, status: 'started' }
    step.attempts.push(a)
  }
  return a
}

function mapActivityStateStatus(s: ActivityState['status']): Step['status'] {
  switch (s) {
    case 'started': return 'running'
    case 'scheduled': return 'scheduled'
    case 'completed': return 'completed'
    case 'failed': return 'failed'
    case 'timed_out': return 'timed_out'
    case 'cancelled': return 'cancelled'
    // Run 结束时仍未完成的 Activity 随 Run 关闭（没有事件），按 Run 被终止显示，不再出现在 Pending 里。
    case 'run_closed': return 'terminated'
    default: return 'scheduled'
  }
}

function isTerminalStatus(s: Step['status']): boolean {
  return s === 'completed' || s === 'failed' || s === 'timed_out' || s === 'cancelled' || s === 'terminated'
}

export function buildStepTree(events: Event[], runStatus?: string, activityStates?: ActivityState[]): Step[] {
  const steps = new Map<number, Step>()
  let seq = 0

  for (const event of events) {
    if (event.activityScheduled) {
      const e = event.activityScheduled
      seq++
      steps.set(event.id, {
        seq,
        stepID: e.activityId ?? '',
        activityType: e.activityType ?? '',
        status: 'scheduled',
        taskQueue: e.taskQueue ?? '',
        input: e.input ?? null,
        output: null,
        error: null,
        scheduledAt: event.timestamp,
        startedAt: null,
        completedAt: null,
        duration: null,
        workerIdentity: '',
        workerBuildID: '',
        attempts: [],
        heartbeat: null,
        childRunID: null,
        scheduledEventID: event.id,
        scheduleToStartTimeoutMs: e.scheduleToStartTimeoutMs,
        startToCloseTimeoutMs: e.startToCloseTimeoutMs,
        scheduleToCloseTimeoutMs: e.scheduleToCloseTimeoutMs,
        heartbeatTimeoutMs: e.heartbeatTimeoutMs,
        retryPolicy: e.retryPolicy,
      })
    }

    if (event.activityStarted) {
      const e = event.activityStarted
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        step.status = 'running'
        // step 级 startedAt 取第一次 started（不再被后续重试覆盖），Timeline 等待段起点不丢头。
        if (step.startedAt === null) {
          step.startedAt = event.timestamp
          step.startedEventID = event.id
        }
        step.workerIdentity = e.workerIdentity ?? ''
        step.workerBuildID = e.workerBuildId ?? ''
        const attempt = getOrCreateAttempt(step, e.attempt ?? 0)
        attempt.startedAt = event.timestamp
        attempt.startedEventID = event.id
        attempt.status = 'started'
      }
    }

    if (event.activityCompleted) {
      const e = event.activityCompleted
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        step.status = 'completed'
        step.output = e.output ?? null
        step.completedAt = event.timestamp
        step.completedEventID = event.id
        if (step.startedAt) {
          step.duration =
            new Date(event.timestamp).getTime() - new Date(step.startedAt).getTime()
        }
        const attempt = getOrCreateAttempt(step, e.attempt ?? 0)
        attempt.completedAt = event.timestamp
        attempt.completedEventID = event.id
        attempt.status = 'completed'
      }
    }

    if (event.activityFailed) {
      const e = event.activityFailed
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        step.status = e.failure?.timeoutType ? 'timed_out' : 'failed'
        step.error = e.error ?? null
        step.completedAt = event.timestamp
        step.completedEventID = event.id
        // Workflow Reset 打断已开始的 Activity 时，失败事件带着原 Run 里最后一次心跳的详情。
        if (e.lastHeartbeatDetails) step.heartbeat = e.lastHeartbeatDetails
        if (step.startedAt) {
          step.duration =
            new Date(event.timestamp).getTime() - new Date(step.startedAt).getTime()
        }
        const attempt = getOrCreateAttempt(step, e.attempt ?? 0)
        attempt.error = e.error
        attempt.errorEventID = event.id
        attempt.status = 'failed'
      }
    }

    if (event.activityAttemptFailed) {
      const e = event.activityAttemptFailed
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        const attempt = getOrCreateAttempt(step, e.attempt ?? 0)
        attempt.error = e.error
        attempt.errorEventID = event.id
        // 超时的 attempt 先写 ActivityTimedOut，再写 ActivityAttemptFailed（还会重试）或 ActivityFailed（终态）。
        if (attempt.status !== 'timed_out') attempt.status = 'failed'
        if (step.status === 'timed_out') {
          step.status = 'scheduled'
          step.error = null
          step.completedAt = null
          step.completedEventID = undefined
          step.duration = null
        }
      }
    }

    if (event.activityTimedOut) {
      const e = event.activityTimedOut
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        step.status = 'timed_out'
        step.error = e.timeoutType ?? null
        step.completedAt = event.timestamp
        step.completedEventID = event.id
        // 心跳不写 History：超时事件带着这次 attempt 最后一次心跳的详情。
        if (e.lastHeartbeatDetails) step.heartbeat = e.lastHeartbeatDetails
        const attempt = getOrCreateAttempt(step, e.attempt ?? 0)
        attempt.error = e.timeoutType
        attempt.errorEventID = event.id
        attempt.status = 'timed_out'
      }
    }

    if (event.activityCancelled) {
      const e = event.activityCancelled
      const step = steps.get(e.scheduledEventId ?? 0)
      if (step) {
        step.status = 'cancelled'
        step.error = 'cancelled'
        step.completedAt = event.timestamp
        step.completedEventID = event.id
      }
    }

    if (event.childWorkflowStarted) {
      seq++
      steps.set(event.id, {
        seq,
        stepID: event.childWorkflowStarted.childId || event.childWorkflowStarted.name || '',
        activityType: `ChildWorkflow:${event.childWorkflowStarted.name}`,
        status: 'running',
        taskQueue: '',
        input: null,
        output: null,
        error: null,
        scheduledAt: event.timestamp,
        startedAt: event.timestamp,
        completedAt: null,
        duration: null,
        workerIdentity: '',
        workerBuildID: '',
        attempts: [],
        heartbeat: null,
        childRunID: event.childWorkflowStarted.childRunId ?? null,
        scheduledEventID: event.id,
      })
    }

    const closed = closedChild(event)
    if (closed) {
      for (const step of steps.values()) {
        if (step.childRunID === closed.childRunID) {
          Object.assign(step, closed.fields)
          step.completedAt = event.timestamp
          if (step.startedAt) {
            step.duration = new Date(event.timestamp).getTime() - new Date(step.startedAt).getTime()
          }
          break
        }
      }
    }
  }

  const result = Array.from(steps.values()).sort((a, b) => a.seq - b.seq)

  if (runStatus && ['terminated', 'cancelled', 'failed'].includes(runStatus)) {
    for (const step of result) {
      if (step.status === 'scheduled' || step.status === 'running') {
        step.status = runStatus as Step['status']
      }
    }
  }

  if (!activityStates || activityStates.length === 0) return result

  // activity-states 是运行中 Activity 的当前状态。History 分页只加载了最新的事件时，ActivityScheduled
  // 可能不在已加载的页里；最大重试次数大于 1 的 Activity 在执行中也没有 ActivityStarted。这两种情况的
  // 尝试次数、开始时间、Worker 与心跳都只在 activity-states 里。
  const byScheduled = new Map(result.map((step) => [step.scheduledEventID, step]))
  let synthesized = false
  for (const st of activityStates) {
    let step = byScheduled.get(st.scheduledEventId)
    if (!step) {
      step = stepFromActivityState(st)
      result.push(step)
      synthesized = true
    }
    step.currentAttempt = st.attempt
    step.activityReset = st.activityReset
    if (isTerminalStatus(step.status)) continue
    step.status = mapActivityStateStatus(st.status)
    if (!step.startedAt && st.startedTime) step.startedAt = st.startedTime
    if (st.workerIdentity) step.workerIdentity = st.workerIdentity
    if (st.workerBuildId) step.workerBuildID = st.workerBuildId
    if (st.lastHeartbeat) step.heartbeat = st.lastHeartbeat
    step.nextAttemptAt = st.status === 'scheduled' ? st.nextAttemptAt : undefined
    step.lastFailure = st.retryLastFailure
  }
  if (synthesized) {
    result.sort((a, b) => a.scheduledEventID - b.scheduledEventID)
    result.forEach((step, index) => {
      step.seq = index + 1
    })
  }
  return result
}

function stepFromActivityState(st: ActivityState): Step {
  return {
    seq: 0,
    stepID: st.activityId,
    activityType: st.activityType,
    status: mapActivityStateStatus(st.status),
    taskQueue: st.taskQueue,
    input: st.input ?? null,
    output: null,
    error: null,
    scheduledAt: st.scheduledTime,
    startedAt: null,
    completedAt: null,
    duration: null,
    workerIdentity: '',
    workerBuildID: '',
    attempts: [],
    heartbeat: null,
    childRunID: null,
    scheduledEventID: st.scheduledEventId,
    scheduleToStartTimeoutMs: st.scheduleToStartTimeoutMs,
    startToCloseTimeoutMs: st.startToCloseTimeoutMs,
    scheduleToCloseTimeoutMs: st.scheduleToCloseTimeoutMs,
    heartbeatTimeoutMs: st.heartbeatTimeoutMs,
    retryPolicy: st.retryPolicy,
  }
}

// closedChild 读出 Child Workflow 结束事件对应的 child run 与步骤的终态字段；其他事件返回 null。
function closedChild(event: Event): { childRunID: string | undefined; fields: Partial<Step> } | null {
  if (event.childWorkflowCompleted) {
    const e = event.childWorkflowCompleted
    return { childRunID: e.childRunId, fields: { status: 'completed', output: e.output ?? null } }
  }
  if (event.childWorkflowFailed) {
    const e = event.childWorkflowFailed
    return { childRunID: e.childRunId, fields: { status: 'failed', error: e.error ?? null } }
  }
  if (event.childWorkflowCancelled) {
    return { childRunID: event.childWorkflowCancelled.childRunId, fields: { status: 'cancelled', error: 'cancelled' } }
  }
  if (event.childWorkflowTerminated) {
    const e = event.childWorkflowTerminated
    return { childRunID: e.childRunId, fields: { status: 'terminated', error: e.reason || 'terminated' } }
  }
  return null
}
