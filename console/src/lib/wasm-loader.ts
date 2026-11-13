export interface ReplayState {
  pseudocode: string
  currentLine: number
  variables: Record<string, unknown>
  activities: Array<{
    activityID: string
    type: string
    status: "scheduled" | "started" | "completed" | "failed"
    input?: Record<string, unknown>
    output?: Record<string, unknown>
    error?: string
    attempt?: number
    workerIdentity?: string
    childRunID?: string
  }>
  timers?: Array<{ timerID: string; status: string }>
  signals?: Record<string, unknown[]>
  condResults?: Array<{ line: number; result: boolean; expr: string }>
  totalEvents: number
  error?: string
}

// ── Trace Debugger 类型 ──
// 字段命名对齐 wasm 的 Go 结构体 JSON 标签：Trace 顶层/lane/step 用 snake_case
// （lane_id、pending_frontier、run_kind 等），activities 复用 ReplayState["activities"]
// （camelCase，wasm 的 activityState 标签即 camelCase，与现有类型一致）。

export interface TraceStep {
  seq: number
  lane_id: string
  lane_seq: number
  line: number
  path: string
  vars: Record<string, unknown>
  frames: TraceFrame[]
  effects?: TraceEffect[]
}

export interface TraceEffect {
  kind: "activity" | "child-workflow" | "timer" | "signal" | "update"
  phase: "scheduled" | "awaited" | "resolved"
  invocation_id: string
  activity_id: string
  status: string
  activity_type?: string
  child_id?: string
  child_run_id?: string
  child_workflow_id?: string
  scheduled_event_id?: number
  call_seq?: number
  wait_seq?: number
  resume_seq?: number
}

export interface TraceFrame {
  name: string
  line: number
  path: string
  vars: Record<string, unknown>
}

export interface WaitingActivity {
  activity_id: string
  type: string
  kind: string // activity | timer | signal | update | child
}

export interface LaneInfo {
  lane_id: string
  lane_kind: string // main | routine
  end_kind: string // completed | pending-frontier | error
  pending_frontier: boolean
  waiting_activity?: WaitingActivity
  error_msg?: string
  start_line: number
  end_line: number
  step_count: number
}

export interface Trace {
  pseudocode: string
  lanes: LaneInfo[]
  steps: TraceStep[]
  activities: ReplayState["activities"]
  run_kind: string // completed | running
  total_steps: number
  error?: string
}

let wasmReady: Promise<void> | null = null

export function loadWasm(): Promise<void> {
  if (wasmReady) return wasmReady

  wasmReady = new Promise<void>((resolve, reject) => {
    const script = document.createElement("script")
    script.src = "/wasm_exec.js"
    script.onload = async () => {
      try {
        const go = new (window as any).Go()
        const result = await WebAssembly.instantiateStreaming(
          fetch(`/replay.wasm?v=${Date.now()}`),
          go.importObject,
        )
        go.run(result.instance)
        resolve()
      } catch (err) {
        reject(err)
      }
    }
    script.onerror = () => reject(new Error("failed to load wasm_exec.js"))
    document.head.appendChild(script)
  })

  return wasmReady
}

export function initReplay(programJSON: string, eventsJSON: string): number {
  const result = (window as any).initReplay(programJSON, eventsJSON)
  if (result?.error) throw new Error(result.error)
  return result.handle as number
}

export function stepTo(handle: number, eventIndex: number): ReplayState {
  const result = (window as any).stepTo(handle, eventIndex)
  if (!result?.pseudocode && result?.error) throw new Error(result.error)
  return result as ReplayState
}

export function getPseudocode(programJSON: string): string {
  return (window as any).getPseudocode(programJSON)
}

// traceReplay 一次性拿到全量 Trace：wasm 内部对整个事件日志跑一遍，记录每一步。
// 前端在返回的轨迹上做所有交互（step/back/continue/切 lane），不再回 wasm。
export function traceReplay(programJSON: string, eventsJSON: string): Trace {
  const fn = (window as unknown as {
    traceReplay: (p: string, e: string) => Trace & { error?: string; pseudocode?: string }
  }).traceReplay
  const result = fn(programJSON, eventsJSON)
  // wasm 出错时（编译失败等）只返回 { error }，此时无 pseudocode 才抛。
  // 运行中的 running run 会带 error 但同时有完整 trace（前沿信息），不应抛。
  if (result?.error && !result?.pseudocode) throw new Error(result.error)
  return result
}

export function getWasmVersion(): string {
  try {
    return (window as any).wasmVersion?.() ?? "unknown"
  } catch {
    return "unknown"
  }
}
