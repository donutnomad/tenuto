import { describe, expect, it } from "vitest"
import {
  emptyTraceNavigation,
  traceLineageContainsRun,
  traceLocationFromSearch,
  traceLocationQuery,
  traceNavigationFromParams,
  writeTraceNavigationParams,
} from "./trace-session"

describe("traceNavigationFromParams", () => {
  it("restores active and inspected navigation with distinct query keys", () => {
    const params = new URLSearchParams(
      "lane=routine-2&seq=17&frame=1&terminal=1&inspectLane=main&inspectSeq=6&inspectFrame=2",
    )

    expect(traceNavigationFromParams(params)).toEqual(expect.objectContaining({
      focusLane: "routine-2",
      stepSeq: 17,
      selectedFrameIndex: 1,
      terminalStop: true,
    }))
    expect(traceNavigationFromParams(params, "inspect")).toEqual(expect.objectContaining({
      focusLane: "main",
      stepSeq: 6,
      selectedFrameIndex: 2,
      terminalStop: false,
    }))
  })

  it("round trips active and inspected Activity Inspector state", () => {
    const params = new URLSearchParams()
    writeTraceNavigationParams(params, {
      focusLane: "routine-3",
      cursorByLane: [],
      selectedFrameIndex: 1,
      breakpoints: [],
      stepSeq: 22,
      line: 14,
      frameName: "closure",
      activityInspector: {
        scheduledEventID: 40,
        attemptEventID: 43,
        attemptPhase: "outcome",
        query: "wallet-3",
        filter: "failed",
        sort: "duration-desc",
      },
    })
    writeTraceNavigationParams(params, {
      focusLane: "main",
      cursorByLane: [],
      selectedFrameIndex: 0,
      breakpoints: [],
      stepSeq: 7,
      line: 10,
      frameName: "parent",
      activityInspector: {
        scheduledEventID: 6,
        query: "",
        filter: "all",
        sort: "sequence",
      },
    }, "inspect")

    expect(traceNavigationFromParams(params).activityInspector).toEqual({
      scheduledEventID: 40,
      attemptEventID: 43,
      attemptPhase: "outcome",
      query: "wallet-3",
      filter: "failed",
      sort: "duration-desc",
    })
    expect(traceNavigationFromParams(params, "inspect").activityInspector).toEqual({
      scheduledEventID: 6,
      query: "",
      filter: "all",
      sort: "sequence",
    })
    expect(params.get("inspectCall")).toBe("6")
    expect(params.has("inspectCallFilter")).toBe(false)
    expect(params.get("callSort")).toBe("duration-desc")
  })

  it("ignores invalid Activity Inspector identifiers and phases", () => {
    const params = new URLSearchParams("call=abc&attempt=-1&attemptPhase=waiting")
    expect(traceNavigationFromParams(params).activityInspector).toBeUndefined()

    const invalidFilter = traceNavigationFromParams(new URLSearchParams("call=6&callFilter=unknown&callSort=random"))
    expect(invalidFilter.activityInspector?.filter).toBe("all")
    expect(invalidFilter.activityInspector?.sort).toBe("sequence")
  })
})

describe("traceLineageContainsRun", () => {
  it("root ContinueAsNew 的 next segment 已由 root lineage 覆盖", () => {
    expect(traceLineageContainsRun(
      ["root-run", "next-run"],
      [],
      "next-run",
    )).toBe(true)
  })

  it("finds a Continue-As-New segment inside a nested Child Workflow boundary", () => {
    expect(traceLineageContainsRun(
      ["root-run"],
      [["child-run-1"], ["grandchild-run-1", "grandchild-run-2"]],
      "grandchild-run-2",
    )).toBe(true)
    expect(traceLineageContainsRun(
      ["root-run"],
      [["child-run-1"], ["grandchild-run-1", "grandchild-run-2"]],
      "unrelated-run",
    )).toBe(false)
  })
})

describe("trace location", () => {
  it("defaults to the root run when the URL carries no run", () => {
    const location = traceLocationFromSearch("root-run", "")
    expect(location.activeRunID).toBe("root-run")
    expect(location.inspectedRunID).toBe("root-run")
    expect(location.inspectedNavigation).toBe(location.activeNavigation)
    expect(location.activeNavigation.stepSeq).toBe(-1)
  })

  it("omits the root run and an inspect equal to the active run", () => {
    expect(traceLocationQuery("root-run", "root-run", "root-run", undefined, undefined)).toBe("")
    const navigation = { ...emptyTraceNavigation(), focusLane: "routine-2", stepSeq: 17, selectedFrameIndex: 1 }
    expect(traceLocationQuery("root-run", "root-run", "root-run", navigation, navigation))
      .toBe("lane=routine-2&seq=17&frame=1")
    expect(traceLocationQuery("root-run", "child-run", "child-run", undefined, undefined)).toBe("run=child-run")
  })

  it("round trips an active child run inspected from its parent", () => {
    const activeNavigation = { ...emptyTraceNavigation(), focusLane: "main", stepSeq: 9, terminalStop: true }
    const inspectedNavigation = { ...emptyTraceNavigation(), focusLane: "routine-1", stepSeq: 4, selectedFrameIndex: 2 }
    const query = traceLocationQuery("root-run", "child-run", "root-run", activeNavigation, inspectedNavigation)
    const location = traceLocationFromSearch("root-run", `?${query}`)

    expect(location.activeRunID).toBe("child-run")
    expect(location.inspectedRunID).toBe("root-run")
    expect(location.activeNavigation).toEqual(expect.objectContaining({ focusLane: "main", stepSeq: 9, terminalStop: true }))
    expect(location.inspectedNavigation).toEqual(expect.objectContaining({
      focusLane: "routine-1",
      stepSeq: 4,
      selectedFrameIndex: 2,
      terminalStop: false,
    }))
  })
})
