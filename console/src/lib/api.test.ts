import { afterEach, describe, expect, it, vi } from "vitest"
import { api, ApiError, errorMessage, ServerUnreachableError, UNAUTHENTICATED_MESSAGE } from "./api"

afterEach(() => vi.unstubAllGlobals())

function captureRequest() {
  const fetch = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) })
  vi.stubGlobal("fetch", fetch)
  return fetch
}

describe("management request bodies", () => {
  it("sends schedule creation fields at the HTTP root", async () => {
    const fetch = captureRequest()
    const req = { namespace: "validation", workflow_type: "probe", cron_expression: "* * * * *", paused: true }
    await api.createSchedule(req)
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual(req)
  })
  it("starts a run by workflow type and omits an empty workflow id", async () => {
    const fetch = captureRequest()
    await api.startWorkflow("validation", "probe", "", { a: 1 })
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ namespace: "validation", workflow_type: "probe", input: { a: 1 } })
    await api.startWorkflow("validation", "probe", "order-1", {})
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({ namespace: "validation", workflow_type: "probe", workflow_id: "order-1", input: {} })
  })
  it("sends a batch operation in req", async () => {
    const fetch = captureRequest()
    const req = { namespace: "validation", type: "cancel" as const, filter: { workflow_id: "probe" } }
    await api.startBatchOperation(req)
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ req })
  })
  it("addresses a batch operation by id for get and cancel", async () => {
    const fetch = captureRequest()
    await api.getBatchOperation("validation", "op-1")
    await api.cancelBatchOperation("validation", "op-1")
    expect(fetch.mock.calls[0][0]).toMatch(/\/get-batch$/)
    expect(fetch.mock.calls[1][0]).toMatch(/\/cancel-batch$/)
    for (const call of fetch.mock.calls) {
      expect(JSON.parse(call[1].body)).toEqual({ namespace: "validation", operation_id: "op-1" })
    }
  })
  it("sets and clears the worker target", async () => {
    const fetch = captureRequest()
    await api.setWorkerTarget("validation", "probe", "B")
    await api.setWorkerTarget("validation", "probe", "")
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ namespace: "validation", workflow_type: "probe", target_worker_identity: "B" })
    expect(JSON.parse(fetch.mock.calls[1][1].body).target_worker_identity).toBe("")
  })
})

describe("run-scoped request bodies", () => {
  it("sends the namespace with every call addressed by run ID", async () => {
    const fetch = captureRequest()
    const ns = "validation"
    const calls: Array<[() => Promise<unknown>, string, Record<string, unknown>]> = [
      [() => api.getRun(ns, "r1"), "/get-run", { run_id: "r1" }],
      [() => api.getHistory(ns, "r1"), "/history", { run_id: "r1" }],
      [() => api.getHistoryPage(ns, "r1"), "/history-page", { run_id: "r1", page_size: 200, direction: "descending" }],
      [() => api.setRunPolicy(ns, "r1", "pinned"), "/set-policy", { run_id: "r1", policy: "pinned" }],
      [() => api.getConfig(ns, "r1"), "/get-config", { run_id: "r1" }],
      [() => api.cancelWorkflow(ns, "r1"), "/cancel", { run_id: "r1" }],
      [() => api.terminateWorkflow(ns, "r1", "why"), "/terminate", { run_id: "r1", reason: "why" }],
      [() => api.signalWorkflow(ns, "r1", "sig", { a: 1 }), "/signal", { run_id: "r1", name: "sig", input: { a: 1 } }],
      [() => api.queryWorkflow(ns, "r1", "q", {}), "/query", { run_id: "r1", name: "q", input: {} }],
      [() => api.resetWorkflow(ns, "r1", { reset_event_id: 3 }), "/reset", { run_id: "r1", reset_event_id: 3 }],
      [() => api.resetActivity(ns, "r1", "charge"), "/reset-activity", { run_id: "r1", activity_id: "charge" }],
      [() => api.getActivityStates(ns, "r1"), "/activity-states", { run_id: "r1" }],
    ]
    for (const [call] of calls) await call()
    calls.forEach(([, path, body], i) => {
      expect(fetch.mock.calls[i][0]).toBe(`/runtime/workflow${path}`)
      expect(JSON.parse(fetch.mock.calls[i][1].body)).toEqual({ namespace: ns, ...body })
    })
  })
})

describe("request failures", () => {
  it("throws ApiError with the HTTP status and the server message", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 400, text: async () => "run not found\n" }))
    const error = await api.getRun("default", "missing").catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(ApiError)
    expect((error as ApiError).status).toBe(400)
    expect((error as ApiError).message).toBe("run not found")
  })
  it("falls back to the status code when the body is empty", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 504, headers: { "Content-Type": "text/plain; charset=utf-8" } })))
    await expect(api.getRun("default", "r1")).rejects.toMatchObject({ status: 504, message: "HTTP 504" })
  })
  it("explains a 401 as rejected Console credentials", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("unauthorized\n", { status: 401, headers: { "Content-Type": "text/plain; charset=utf-8" } })))
    const error = await api.listNamespaces().catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(ApiError)
    expect(error).toMatchObject({ status: 401, message: UNAUTHENTICATED_MESSAGE })
    expect(UNAUTHENTICATED_MESSAGE).toContain("TENUTO_SERVER_AUTH_TOKEN")
  })
  it("keeps a 5xx written by the Tenuto Server as ApiError", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("internal server error\n", { status: 500, headers: { "Content-Type": "text/plain; charset=utf-8" } })))
    const error = await api.listNamespaces().catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(ApiError)
    expect(error).toMatchObject({ status: 500, message: "internal server error" })
  })
})

describe("unreachable server", () => {
  it("reports a network failure as ServerUnreachableError", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")))
    await expect(api.listNamespaces()).rejects.toBeInstanceOf(ServerUnreachableError)
  })
  it("reports the Console proxy's bare 500 and a gateway's HTML 502 as ServerUnreachableError", async () => {
    for (const res of [
      // Blob 不带类型，与代理应答一样没有 Content-Type（字符串 body 会被自动补上 text/plain）。
      new Response(new Blob(["Internal Server Error"]), { status: 500 }),
      new Response("<html>Bad Gateway</html>", { status: 502, headers: { "Content-Type": "text/html" } }),
    ]) {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(res))
      await expect(api.listNamespaces()).rejects.toBeInstanceOf(ServerUnreachableError)
    }
  })
  it("rethrows an abort instead of reporting the server unreachable", async () => {
    const controller = new AbortController()
    controller.abort()
    const abort = new DOMException("aborted", "AbortError")
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(abort))
    await expect(api.getRun("default", "r1", controller.signal)).rejects.toBe(abort)
  })
  it("names the server address in the message", () => {
    expect(new ServerUnreachableError("http://127.0.0.1:7400").message).toBe("Cannot connect to Tenuto Server at http://127.0.0.1:7400.")
    expect(new ServerUnreachableError("").message).toBe("Cannot connect to Tenuto Server.")
  })
})

describe("get-run response", () => {
  it("merges blocked and terminal_failure into the run", async () => {
    const failure = { message: "boom", type: "AppError" }
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ run: { id: "r1", status: "failed" }, blocked: { blocked: false }, terminal_failure: failure }),
    }))
    await expect(api.getRun("default", "r1")).resolves.toMatchObject({ id: "r1", blocked: { blocked: false }, terminal_failure: failure })
  })
})

describe("errorMessage", () => {
  it("uses the error text and falls back otherwise", () => {
    expect(errorMessage(new ApiError(400, "bad"), "fallback")).toBe("bad")
    expect(errorMessage(new Error(""), "fallback")).toBe("fallback")
    expect(errorMessage("nope", "fallback")).toBe("fallback")
  })
})
