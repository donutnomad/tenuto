import { describe, expect, it } from "vitest"
import {
  nextContinuationRunID,
  validateChildEdge,
  validateContinuationEdge,
  TraceTransitionGate,
  TraceContextCache,
  type TraceRunContext,
} from "./trace-context"
import { getWlangProgram } from "./wlang-program"
import type { Event, Run, WorkflowConfig } from "./types"

function run(overrides: Partial<Run> = {}): Run {
  return {
    id: "run-1",
    namespace: "demo",
    workflow_id: "flow",
    workflow_config_version: "v1",
    status: "running",
    first_run_id: "run-1",
    created_at: "2026-07-13T00:00:00Z",
    ...overrides,
  }
}

const config: WorkflowConfig = {
  type: "flow",
  name: "flow",
  program: { schema: "wlang/program/v2", lang: "wflang/v1", program: [] },
}

describe("getWlangProgram", () => {
  it("preserves the complete v2 artifact for WASM replay", () => {
    const artifact = {
      schema: "wlang/program/v2",
      lang: "wflang/v1",
      program: [{ return: { literal: { type: "null", value: null } } }],
      imports: { workflow: "github.com/donutnomad/tenuto/sdk/workflow" },
      types: { "example.Result": { id: "example.Result", kind: "record" } },
      handler_abi_schema: "workflow/update-handler-abi/v1",
    }
    const typed: WorkflowConfig = { type: "typed", name: "typed", program: artifact }
    expect(JSON.parse(getWlangProgram(typed))).toEqual(artifact)
  })
})

describe("Trace lineage validation", () => {
  it("校验 parent、child ID、scheduled event 与 namespace", () => {
    expect(() => validateChildEdge("parent", run({
      id: "child",
      parent_run_id: "parent",
      parent_child_id: "child-1",
      parent_scheduled_event_id: 7,
    }), "child-1", 7, "demo")).not.toThrow()
    expect(() => validateChildEdge("other", run({ parent_run_id: "parent" }))).toThrow(/does not belong/)
  })

  it("从终态事件读取下一 continuation 并校验反向链", () => {
    const context = {
      events: [{ id: 9, timestamp: "2026-07-13T00:00:00Z", workflowContinuedAsNew: { name: "flow", nextRunId: "run-2" } }],
    } as TraceRunContext
    expect(nextContinuationRunID(context)).toBe("run-2")
    expect(() => validateContinuationEdge("run-1", run({ id: "run-2", continued_run_id: "run-1" }))).not.toThrow()
    expect(() => validateContinuationEdge("run-x", run({ id: "run-2", continued_run_id: "run-1" }))).toThrow(/does not continue/)
  })
})

describe("TraceTransitionGate", () => {
  it("新请求取消旧请求并拒绝旧响应提交", () => {
    const gate = new TraceTransitionGate()
    const first = gate.start()
    const second = gate.start()
    expect(first.signal.aborted).toBe(true)
    expect(gate.isCurrent(first.id)).toBe(false)
    expect(gate.isCurrent(second.id)).toBe(true)
    expect(gate.finish(first.id)).toBe(false)
    expect(gate.finish(second.id)).toBe(true)
  })

  it("cancel 中止当前请求", () => {
    const gate = new TraceTransitionGate()
    const request = gate.start()
    gate.cancel()
    expect(request.signal.aborted).toBe(true)
    expect(gate.isCurrent(request.id)).toBe(false)
  })
})

describe("TraceContextCache", () => {
  const context = (runID: string): TraceRunContext => ({
    run: run({ id: runID }),
    events: [],
    workflowConfig: config,
    programJSON: getWlangProgram(config),
    trace: { pseudocode: "", lanes: [], steps: [], activities: [], run_kind: "running", total_steps: 0 },
  })

  it("合并同一 Run 的并发加载并复用缓存", async () => {
    let loads = 0
    const cache = new TraceContextCache(2, async (runID) => {
      loads++
      await Promise.resolve()
      return context(runID)
    })
    const [first, second] = await Promise.all([cache.load("run-a"), cache.load("run-a")])
    expect(first).toBe(second)
    expect(await cache.load("run-a")).toBe(first)
    expect(loads).toBe(1)
  })

  it("按 LRU 容量淘汰最久未访问的 Context", async () => {
    let loads = 0
    const cache = new TraceContextCache(2, async (runID) => {
      loads++
      return context(runID)
    })
    await cache.load("run-a")
    await cache.load("run-b")
    cache.get("run-a")
    await cache.load("run-c")
    await cache.load("run-b")
    expect(loads).toBe(4)
  })

  it("共享 in-flight 时单个 caller 取消不影响其他 caller", async () => {
    let resolveLoad!: (value: TraceRunContext) => void
    const cache = new TraceContextCache(2, () => new Promise((resolve) => {
      resolveLoad = resolve
    }))
    const controller = new AbortController()
    const first = cache.load("run-a", controller.signal)
    const second = cache.load("run-a")
    controller.abort()
    resolveLoad(context("run-a"))
    await expect(first).rejects.toMatchObject({ name: "AbortError" })
    await expect(second).resolves.toEqual(context("run-a"))
  })

  it("refresh generation 胜出且 clear 阻止旧请求回填", async () => {
    const resolvers: Array<(value: TraceRunContext) => void> = []
    const cache = new TraceContextCache(2, () => new Promise((resolve) => resolvers.push(resolve)))
    const oldRequest = cache.load("run-a")
    const refreshed = cache.refresh("run-a")
    resolvers[0](context("old"))
    resolvers[1](context("new"))
    await oldRequest
    await refreshed
    expect(cache.get("run-a")?.run.id).toBe("new")

    const late = cache.load("run-b")
    cache.clear()
    resolvers[2](context("late"))
    await late
    expect(cache.get("run-b")).toBeUndefined()
  })
})
