import { describe, expect, it } from "vitest"
import {
  BUILT_IN_QUERY_NAME,
  addQueryHistory,
  discoverQueryNames,
  loadQueryHistory,
  queryHistoryStorageKey,
  queryOutcome,
  saveQueryHistory,
  validateQueryInput,
  type QueryHistoryEntry,
} from "./query-panel"

function historyEntry(id: string): QueryHistoryEntry {
  return {
    id,
    name: "getCount",
    inputText: "{}",
    input: {},
    status: "success",
    result: { count: 1 },
    executedAt: "2026-07-14T00:00:00.000Z",
  }
}

describe("discoverQueryNames", () => {
  it("保留内置查询并递归发现 WLang 静态 query handler", () => {
    const program = {
      lang: "wflang/v1",
      program: [{
        branch: {
          do: [{
            expr: {
              SetQueryHandler: [
                { pkg: "workflow" },
                { var: "ctx" },
                { literal: { type: "string", value: "getCount" } },
                { fn: { do: [] } },
              ],
            },
          }],
        },
      }],
    }

    expect(discoverQueryNames(JSON.stringify(program))).toEqual([BUILT_IN_QUERY_NAME, "getCount"])
  })

  it("支持通用 call 形态并去重", () => {
    const program = {
      calls: [
        { call: { name: "workflow.SetQueryHandler", args: [{ var: "ctx" }, "status", { fn: {} }] } },
        { SetQueryHandler: [{ var: "ctx" }, { literal: "status" }, { fn: {} }] },
      ],
    }
    expect(discoverQueryNames(program)).toEqual([BUILT_IN_QUERY_NAME, "status"])
  })

  it("安全忽略动态名称、空名称和损坏的 program JSON", () => {
    const program = {
      do: [
        { SetQueryHandler: [{ pkg: "workflow" }, { var: "ctx" }, { var: "queryName" }, { fn: {} }] },
        { SetQueryHandler: [{ pkg: "workflow" }, { var: "ctx" }, { literal: { type: "string", value: " " } }, { fn: {} }] },
      ],
    }
    expect(discoverQueryNames(program)).toEqual([BUILT_IN_QUERY_NAME])
    expect(discoverQueryNames("{broken")).toEqual([BUILT_IN_QUERY_NAME])
  })

  it("可处理带循环引用的对象输入", () => {
    const program: Record<string, unknown> = {}
    program.self = program
    expect(discoverQueryNames(program)).toEqual([BUILT_IN_QUERY_NAME])
  })
})

describe("validateQueryInput", () => {
  it("空输入转换为空对象", () => {
    expect(validateQueryInput("  ")).toEqual({ ok: true, value: {} })
  })

  it("解析 JSON 对象", () => {
    expect(validateQueryInput('{"account":"A-1","limit":3}')).toEqual({
      ok: true,
      value: { account: "A-1", limit: 3 },
    })
  })

  it("返回具体语法错误并限制顶层参数类型", () => {
    const syntax = validateQueryInput('{"account":}')
    expect(syntax.ok).toBe(false)
    if (!syntax.ok) expect(syntax.error).toMatch(/^Invalid JSON: .+/)

    expect(validateQueryInput("[1,2]")).toEqual({
      ok: false,
      error: "Arguments must be a JSON object.",
    })
    expect(validateQueryInput("null")).toEqual({
      ok: false,
      error: "Arguments must be a JSON object.",
    })
  })
})

describe("query history", () => {
  it("最新记录在前并执行上限", () => {
    expect(addQueryHistory([historyEntry("2"), historyEntry("1")], historyEntry("3"), 2).map((entry) => entry.id))
      .toEqual(["3", "2"])
  })

  it("按 run 隔离保存并过滤损坏记录", () => {
    const values = new Map<string, string>()
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => { values.set(key, value) },
    }
    saveQueryHistory(storage, "run-1", [historyEntry("1")])
    expect(values.has(queryHistoryStorageKey("run-1"))).toBe(true)
    expect(loadQueryHistory(storage, "run-1")).toEqual([historyEntry("1")])
    expect(loadQueryHistory(storage, "run-2")).toEqual([])

    values.set(queryHistoryStorageKey("run-1"), JSON.stringify([{ id: 1 }, historyEntry("2")]))
    expect(loadQueryHistory(storage, "run-1")).toEqual([historyEntry("2")])
  })

  it("存储不可用时保持安全", () => {
    const storage = {
      getItem: () => { throw new Error("blocked") },
      setItem: () => { throw new Error("full") },
    }
    expect(loadQueryHistory(storage, "run-1")).toEqual([])
    expect(() => saveQueryHistory(storage, "run-1", [historyEntry("1")])).not.toThrow()
  })
})

describe("queryOutcome", () => {
  it("returns the handler result for an accepted query", () => {
    expect(queryOutcome({ query_name: "count", query_result: { n: 1 } })).toEqual({ result: { n: 1 } })
    expect(queryOutcome({ query_name: "noop" })).toEqual({ result: null })
  })
  it("reports rejected queries as errors", () => {
    expect(queryOutcome({ query_rejected: true, query_error: "unknown query: x" })).toEqual({ error: "unknown query: x" })
    expect(queryOutcome({ query_rejected: true })).toEqual({ error: "Query was rejected by the workflow." })
    expect(queryOutcome({ query_error: "boom" })).toEqual({ error: "boom" })
  })
})
