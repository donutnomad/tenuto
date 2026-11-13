import { describe, it, expect } from "vitest"
import {
  splitLanes,
  continueToNextBreakpoint,
  findNextBreakpointStep,
  findBreakpointStepAtOrAfter,
  uniqueEnterableChildEffects,
  uniqueScheduledChildEffects,
  childResumeSeq,
  shouldStopAtParentBoundary,
  terminalStepAction,
  centeredScrollTop,
  nextCursorOnLaneSwitch,
  frontierSentinelText,
  laneCursorOfStep,
  hydrateTraceNavigation,
  childResumeSnapshot,
  restoreActivityInspector,
} from "./trace-debugger-logic"
import type { TraceEffect, TraceStep, LaneInfo } from "@/lib/wasm-loader"
import { emptyTraceNavigation } from "@/lib/trace-session"
import type { ActivityInvocationRecord, ActivityInvocationTraceAnchor } from "@/lib/activity-invocations"

function step(overrides: Partial<TraceStep>): TraceStep {
  return {
    seq: 0,
    lane_id: "main",
    lane_seq: 0,
    line: 0,
    path: "",
    vars: {},
    frames: [],
    ...overrides,
  }
}

function lane(overrides: Partial<LaneInfo>): LaneInfo {
  return {
    lane_id: "main",
    lane_kind: "main",
    end_kind: "completed",
    pending_frontier: false,
    start_line: 0,
    end_line: 0,
    step_count: 0,
    ...overrides,
  }
}

describe("splitLanes", () => {
  it("Happy Path: 按 lane_id 分组，组内按 lane_seq 升序", () => {
    // 输入乱序混排的多 lane 全局步序列，期望：每个 lane 独立成组且组内按 lane_seq 递增排列
    const steps: TraceStep[] = [
      step({ seq: 0, lane_id: "main", lane_seq: 0, line: 1 }),
      step({ seq: 1, lane_id: "routine-7", lane_seq: 1, line: 5 }),
      step({ seq: 2, lane_id: "main", lane_seq: 2, line: 3 }),
      step({ seq: 3, lane_id: "routine-7", lane_seq: 0, line: 4 }),
      step({ seq: 4, lane_id: "main", lane_seq: 1, line: 2 }),
    ]
    const result = splitLanes(steps)
    expect([...result.keys()]).toEqual(["main", "routine-7"])
    expect(result.get("main")!.map((s) => s.lane_seq)).toEqual([0, 1, 2])
    expect(result.get("routine-7")!.map((s) => s.lane_seq)).toEqual([0, 1])
    // 组内已按 lane_seq 升序，line 应随之为 main:[1,2,3]
    expect(result.get("main")!.map((s) => s.line)).toEqual([1, 2, 3])
  })

  it("Edge Case: 单 lane 程序（只有 main）", () => {
    // 只含 main lane 的程序，期望：只有一组 main，其余不产生
    const steps: TraceStep[] = [
      step({ seq: 0, lane_id: "main", lane_seq: 0, line: 0 }),
      step({ seq: 1, lane_id: "main", lane_seq: 1, line: 1 }),
    ]
    const result = splitLanes(steps)
    expect([...result.keys()]).toEqual(["main"])
    expect(result.get("main")!.length).toBe(2)
  })

  it("Edge Case: 空步序列返回空 Map", () => {
    // 无任何步（异常/空轨迹），期望：空 Map，不崩
    expect(splitLanes([]).size).toBe(0)
  })
})

describe("continueToNextBreakpoint", () => {
  const steps: TraceStep[] = [
    step({ lane_seq: 0, line: 1 }),
    step({ lane_seq: 1, line: 2 }),
    step({ lane_seq: 2, line: 5 }),
    step({ lane_seq: 3, line: 8 }),
  ]

  it("Happy Path: 命中断点，停在断点行对应的步", () => {
    // cursor=0，断点在 line 5；期望：从 cursor+1 向后扫，命中 index 2（line 5）
    expect(continueToNextBreakpoint(steps, 0, new Set([5]))).toBe(2)
  })

  it("Edge Case: 无命中，运行到 lane 末尾", () => {
    // cursor=0，断点行在本 lane 不存在（line 99）；期望：走到末尾 index 3
    expect(continueToNextBreakpoint(steps, 0, new Set([99]))).toBe(3)
  })

  it("Edge Case: 完全无断点，直达末尾", () => {
    // cursor=1，空断点集；期望：直接到末尾 index 3
    expect(continueToNextBreakpoint(steps, 1, new Set())).toBe(3)
  })

  it("Edge Case: 从 cursor+1 开始，不停在当前 cursor 所在断点行", () => {
    // cursor=0 且 line 1 是断点；期望：不停在自己，扫到下一个断点 line 8（index 3）
    expect(continueToNextBreakpoint(steps, 0, new Set([1, 8]))).toBe(3)
  })

  it("Edge Case: 不跨 lane —— 只在传入的 focusSteps 内推进", () => {
    // 传入的只有本 lane 的步，即使全局断点行存在于别的 lane 也无从命中；
    // 期望：line 5 只在本 lane index 2 命中，函数天然不接触其他 lane 的步
    const onlyThisLane: TraceStep[] = [
      step({ lane_id: "routine-1", lane_seq: 0, line: 10 }),
      step({ lane_id: "routine-1", lane_seq: 1, line: 11 }),
    ]
    // 断点 line 5 在别的 lane，此 lane 无此行 → 到末尾
    expect(continueToNextBreakpoint(onlyThisLane, 0, new Set([5]))).toBe(1)
  })

  it("Edge Case: 空步序列返回 0", () => {
    expect(continueToNextBreakpoint([], 0, new Set([1]))).toBe(0)
  })
})

describe("findNextBreakpointStep", () => {
  it("跨执行线命中协程断点", () => {
    const steps: TraceStep[] = [
      step({ seq: 0, lane_id: "main", lane_seq: 0, line: 1 }),
      step({ seq: 1, lane_id: "main", lane_seq: 1, line: 2 }),
      step({ seq: 2, lane_id: "routine-1", lane_seq: 0, line: 8 }),
      step({ seq: 3, lane_id: "routine-2", lane_seq: 0, line: 8 }),
    ]

    expect(findNextBreakpointStep(steps, 0, new Set([8]))).toEqual(steps[2])
  })

  it("从当前全局步之后继续查找", () => {
    const steps: TraceStep[] = [
      step({ seq: 4, lane_id: "routine-1", lane_seq: 0, line: 8 }),
      step({ seq: 5, lane_id: "main", lane_seq: 2, line: 3 }),
      step({ seq: 6, lane_id: "routine-2", lane_seq: 0, line: 8 }),
    ]

    expect(findNextBreakpointStep(steps, 4, new Set([8]))).toEqual(steps[2])
  })

  it("后续轨迹没有断点时返回 null", () => {
    const steps = [step({ seq: 0, line: 1 }), step({ seq: 1, line: 2 })]
    expect(findNextBreakpointStep(steps, 0, new Set([9]))).toBeNull()
  })
})

describe("findBreakpointStepAtOrAfter", () => {
  const steps = [
    step({ seq: 4, line: 10 }),
    step({ seq: 7, line: 20 }),
    step({ seq: 9, line: 30 }),
  ]

  it("跨 Child 返回 Parent 时允许命中 resume step", () => {
    expect(findBreakpointStepAtOrAfter(steps, 7, new Set([20]))).toEqual(steps[1])
  })

  it("当前 Parent 无命中时返回 null", () => {
    expect(findBreakpointStepAtOrAfter(steps, 7, new Set([10]))).toBeNull()
  })
})

describe("uniqueScheduledChildEffects", () => {
  const effect = (overrides: Partial<TraceEffect>): TraceEffect => ({
    kind: "child-workflow",
    phase: "scheduled",
    invocation_id: "child:1",
    activity_id: "child-1",
    status: "scheduled",
    ...overrides,
  })

  it("按 invocation_id 去重并保留不同 Child 调用", () => {
    const effects = uniqueScheduledChildEffects([
      effect({ child_run_id: "run-1" }),
      effect({ child_run_id: "run-1" }),
      effect({ invocation_id: "child:2", child_id: "two", child_run_id: "run-2" }),
    ])

    expect(effects.map((item) => item.child_run_id)).toEqual(["run-1", "run-2"])
  })

  it("过滤 activity 与 resolved effect", () => {
    expect(uniqueScheduledChildEffects([
      effect({ kind: "activity" }),
      effect({ phase: "resolved" }),
    ])).toEqual([])
  })

  it("Get 行的 awaited effect 可 Step Into 并优先作为 representative", () => {
    const scheduled = effect({ child_run_id: "run-1" })
    const awaited = effect({ phase: "awaited", child_run_id: "run-1", wait_seq: 7 })
    expect(uniqueEnterableChildEffects([scheduled, awaited])).toEqual([awaited])
  })
})

describe("childResumeSeq", () => {
  const effect: TraceEffect = {
    kind: "child-workflow",
    phase: "scheduled",
    invocation_id: "child:1",
    activity_id: "child-1",
    status: "running",
  }

  it("completed Child 优先恢复 resolved 后的 resume 锚点", () => {
    expect(childResumeSeq({ ...effect, call_seq: 2, wait_seq: 5, resume_seq: 8 })).toBe(8)
  })

  it("running Child 恢复 await frontier", () => {
    expect(childResumeSeq({ ...effect, call_seq: 2, wait_seq: 5 })).toBe(5)
  })

  it("异步且尚未 await 时恢复 schedule call site", () => {
    expect(childResumeSeq({ ...effect, call_seq: 2 })).toBe(2)
  })
})

describe("shouldStopAtParentBoundary", () => {
  it("运行中的 Parent 停在自己的 frontier", () => {
    expect(shouldStopAtParentBoundary("running", false, false)).toBe(true)
  })

  it("completed Parent 无断点时继续向祖先搜索", () => {
    expect(shouldStopAtParentBoundary("completed", false, false)).toBe(false)
  })

  it("Parent 命中断点或到达 root 时停下", () => {
    expect(shouldStopAtParentBoundary("completed", true, false)).toBe(true)
    expect(shouldStopAtParentBoundary("completed", false, true)).toBe(true)
  })

  it("Parent ContinueAsNew 停在该 Run 的 terminal boundary", () => {
    expect(shouldStopAtParentBoundary("continued_as_new", false, false)).toBe(true)
  })
})

describe("terminalStepAction", () => {
  it("Child 末步第一次 Step 进入 terminal stop", () => {
    expect(terminalStepAction({
      atLastStep: true,
      terminalStop: false,
      terminalStatus: true,
      canCrossBoundary: true,
    })).toBe("enter-terminal")
  })

  it("terminal stop 下一次 Step 返回 Parent", () => {
    expect(terminalStepAction({
      atLastStep: true,
      terminalStop: true,
      terminalStatus: true,
      canCrossBoundary: true,
    })).toBe("cross-boundary")
  })

  it("running Child 在末步保持普通轨迹推进语义", () => {
    expect(terminalStepAction({
      atLastStep: true,
      terminalStop: false,
      terminalStatus: false,
      canCrossBoundary: true,
    })).toBe("advance")
  })

  it("routine lane 末步仍有后续全局 step 时保持普通推进语义", () => {
    expect(terminalStepAction({
      atLastStep: false,
      terminalStop: false,
      terminalStatus: true,
      canCrossBoundary: true,
    })).toBe("advance")
  })
})

describe("centeredScrollTop", () => {
  it("把视口坐标转换为代码容器内部滚动坐标", () => {
    expect(
      centeredScrollTop({
        containerTop: 400,
        containerScrollTop: 200,
        containerHeight: 480,
        lineTop: 520,
        lineHeight: 24,
      }),
    ).toBe(92)
  })

  it("靠近代码开头时收敛到零", () => {
    expect(
      centeredScrollTop({
        containerTop: 400,
        containerScrollTop: 0,
        containerHeight: 480,
        lineTop: 424,
        lineHeight: 24,
      }),
    ).toBe(0)
  })
})

describe("nextCursorOnLaneSwitch", () => {
  it("Happy Path: 恢复已访问 lane 的上次 cursor", () => {
    // cursorMap 记录 routine-2 上次停在 cursor 3；切回去期望恢复 3
    const map = new Map<string, number>([
      ["main", 5],
      ["routine-2", 3],
    ])
    expect(nextCursorOnLaneSwitch(map, "routine-2")).toBe(3)
  })

  it("Edge Case: 首次访问的 lane 从 0 开始", () => {
    // cursorMap 无 routine-9 记录（首访）；期望从 0 开始
    const map = new Map<string, number>([["main", 5]])
    expect(nextCursorOnLaneSwitch(map, "routine-9")).toBe(0)
  })
})

describe("frontierSentinelText", () => {
  it("Happy Path: pending-frontier + waiting activity 产出哨兵文案", () => {
    // pending lane 等待 activity charge(step-3)；期望文案含 type、activity_id 和"执行前沿"
    const l = lane({
      end_kind: "pending-frontier",
      pending_frontier: true,
      waiting_activity: { activity_id: "step-3", type: "charge", kind: "activity" },
    })
    const text = frontierSentinelText(l)
    expect(text).toContain("charge")
    expect(text).toContain("step-3")
    expect(text).toContain("执行前沿")
    expect(text).toContain("activity")
  })

  it("Happy Path: kind=timer 时主体名词替换为定时器", () => {
    // 等待定时器；期望文案主体为"定时器"
    const l = lane({
      end_kind: "pending-frontier",
      pending_frontier: true,
      waiting_activity: { activity_id: "t-1", type: "sleep", kind: "timer" },
    })
    expect(frontierSentinelText(l)).toContain("定时器")
  })

  it("Happy Path: kind=signal / update / child 分别替换主体名词", () => {
    // 三种非 activity 等待对象，期望文案主体分别为 信号 / update / 子工作流
    const mk = (kind: string) =>
      frontierSentinelText(
        lane({
          end_kind: "pending-frontier",
          pending_frontier: true,
          waiting_activity: { activity_id: "x", type: "y", kind },
        }),
      )
    expect(mk("signal")).toContain("信号")
    expect(mk("update")).toContain("update")
    expect(mk("child")).toContain("子工作流")
  })

  it("Edge Case: pending-frontier 但缺 waiting_activity 仍产出通用哨兵", () => {
    // pending 但没有 waiting_activity（理论边界），期望产出通用"执行前沿"文案而非 null
    const l = lane({ end_kind: "pending-frontier", pending_frontier: true })
    const text = frontierSentinelText(l)
    expect(text).not.toBeNull()
    expect(text).toContain("执行前沿")
  })

  it("Error Case: completed lane 不产出哨兵（返回 null）", () => {
    // 正常结束的 lane 绝不能显示 pending 哨兵；期望 null
    expect(frontierSentinelText(lane({ end_kind: "completed" }))).toBeNull()
  })

  it("Error Case: error lane 不产出哨兵（返回 null）", () => {
    // 报错结束的 lane 也不是 pending 前沿；期望 null
    expect(frontierSentinelText(lane({ end_kind: "error", error_msg: "boom" }))).toBeNull()
  })
})

describe("laneCursorOfStep", () => {
  const lanes = splitLanes([
    step({ seq: 0, lane_id: "main", lane_seq: 0 }),
    step({ seq: 1, lane_id: "routine-1", lane_seq: 0 }),
    step({ seq: 2, lane_id: "main", lane_seq: 1 }),
  ])

  it("全局 seq 映射为该 lane 时间轴上的下标", () => {
    expect(laneCursorOfStep(lanes, "main", 2)).toBe(1)
    expect(laneCursorOfStep(lanes, "routine-1", 1)).toBe(0)
  })

  it("lane 不存在或该 lane 没有这一步时为 -1", () => {
    expect(laneCursorOfStep(lanes, "main", 1)).toBe(-1)
    expect(laneCursorOfStep(lanes, "routine-9", 0)).toBe(-1)
  })
})

describe("hydrateTraceNavigation", () => {
  const trace = {
    lanes: [lane({ lane_id: "routine-1" }), lane({ lane_id: "main" })],
    steps: [
      step({ seq: 0, lane_id: "main", lane_seq: 0 }),
      step({ seq: 1, lane_id: "routine-1", lane_seq: 0 }),
      step({ seq: 2, lane_id: "routine-1", lane_seq: 1 }),
      step({ seq: 3, lane_id: "main", lane_seq: 1 }),
    ],
  }

  it("没有快照时只定位 main lane", () => {
    expect(hydrateTraceNavigation(trace, undefined, "running")).toEqual({ focusLane: "main", restored: null })
  })

  it("没有 main lane 时落到第一条 lane，Trace 为空时回到 main", () => {
    expect(hydrateTraceNavigation({ lanes: [lane({ lane_id: "routine-1" })], steps: [] }, undefined, undefined).focusLane)
      .toBe("routine-1")
    expect(hydrateTraceNavigation({ lanes: [], steps: [] }, undefined, undefined).focusLane).toBe("main")
  })

  it("快照的 stepSeq 覆盖同一 lane 上记录的 cursor", () => {
    const hydrated = hydrateTraceNavigation(trace, {
      ...emptyTraceNavigation(),
      focusLane: "routine-1",
      cursorByLane: [["main", 1], ["routine-1", 0]],
      stepSeq: 2,
      selectedFrameIndex: 1,
      breakpoints: [4, 7],
    }, "completed")

    expect(hydrated.focusLane).toBe("routine-1")
    expect(hydrated.restored).toEqual({
      cursorByLane: new Map([["main", 1], ["routine-1", 1]]),
      selectedFrameIndex: 1,
      breakpoints: new Set([4, 7]),
      terminalStop: false,
    })
  })

  it("stepSeq 在该 lane 上找不到时保留快照里的 cursor", () => {
    const hydrated = hydrateTraceNavigation(trace, {
      ...emptyTraceNavigation(),
      cursorByLane: [["main", 1]],
      stepSeq: 99,
    }, "completed")
    expect(hydrated.restored?.cursorByLane).toEqual(new Map([["main", 1]]))
  })

  it("terminalStop 只对已结束的 Run 恢复", () => {
    const navigation = { ...emptyTraceNavigation(), terminalStop: true }
    expect(hydrateTraceNavigation(trace, navigation, "failed").restored?.terminalStop).toBe(true)
    expect(hydrateTraceNavigation(trace, navigation, "running").restored?.terminalStop).toBe(false)
  })
})

describe("childResumeSnapshot", () => {
  const steps = [
    step({ seq: 0, lane_id: "main", lane_seq: 0, line: 3 }),
    step({ seq: 1, lane_id: "routine-1", lane_seq: 0, line: 8 }),
    step({
      seq: 2,
      lane_id: "routine-1",
      lane_seq: 1,
      line: 9,
      frames: [{ name: "settle", line: 12, path: "settle", vars: {} }],
    }),
  ]
  const lanes = splitLanes(steps)
  const entry = {
    ...emptyTraceNavigation(),
    cursorByLane: [["main", 0]] as Array<[string, number]>,
    stepSeq: 0,
    line: 3,
    selectedFrameIndex: 2,
    breakpoints: [5],
  }
  const effect: TraceEffect = {
    kind: "child-workflow",
    phase: "scheduled",
    invocation_id: "child-1",
    activity_id: "#c1",
    status: "completed",
    child_run_id: "child-run",
  }

  it("返回点落在 resume_seq 对应的步、lane 与栈顶帧上", () => {
    expect(childResumeSnapshot(entry, { ...effect, resume_seq: 2 }, steps, lanes)).toEqual({
      ...entry,
      focusLane: "routine-1",
      cursorByLane: [["main", 0], ["routine-1", 1]],
      selectedFrameIndex: 0,
      stepSeq: 2,
      line: 12,
      frameName: "settle",
    })
  })

  it("effect 没有恢复位置时沿用进入时的位置，只重置栈帧选择", () => {
    expect(childResumeSnapshot(entry, effect, steps, lanes)).toEqual({ ...entry, selectedFrameIndex: 0 })
  })

  it("恢复 seq 不在 Trace 里时保留进入时的 lane 与行，只记录 seq", () => {
    expect(childResumeSnapshot(entry, { ...effect, resume_seq: 42 }, steps, lanes)).toEqual({
      ...entry,
      selectedFrameIndex: 0,
      stepSeq: 42,
    })
  })
})

describe("restoreActivityInspector", () => {
  const invocation: ActivityInvocationRecord = {
    invocationID: "inv-1",
    stepID: "#a1",
    scheduledEventID: 10,
    activityType: "payments.Charge",
    status: "completed",
    attempts: [
      { resetEpoch: 0, attempt: 1, status: "failed", startedEventID: 11, errorEventID: 12 },
      { resetEpoch: 0, attempt: 2, status: "completed", startedEventID: 13, completedEventID: 14 },
    ],
    resetEpochs: [],
  }
  const anchor: ActivityInvocationTraceAnchor = {
    scheduledEventID: 10,
    laneID: "main",
    line: 6,
    stepSeq: 3,
    laneSeq: 3,
    iteration: 1,
    waitSeq: 4,
    resumeSeq: 5,
  }
  const activityIndex = new Map([[10, invocation]])
  const traceIndex = new Map([[10, anchor]])
  const inspector = { scheduledEventID: 10, query: "charge", filter: "failed", sort: "duration-desc" }

  it("没有快照、或调用已不在历史 / Trace 里时不恢复", () => {
    expect(restoreActivityInspector(undefined, activityIndex, traceIndex)).toBeNull()
    expect(restoreActivityInspector(inspector, new Map(), traceIndex)).toBeNull()
    expect(restoreActivityInspector(inspector, activityIndex, new Map())).toBeNull()
  })

  it("恢复选中的调用及其搜索、筛选、排序", () => {
    expect(restoreActivityInspector(inspector, activityIndex, traceIndex)).toEqual({
      selectedActivity: { line: 6, eventID: 10 },
      selectedAttempt: null,
      query: "charge",
      filter: "failed",
      sort: "duration-desc",
    })
  })

  it("按任一 attempt 事件 ID 找回 attempt，并按快照的 phase 定位", () => {
    expect(restoreActivityInspector(
      { ...inspector, attemptEventID: 14, attemptPhase: "started" },
      activityIndex,
      traceIndex,
    )?.selectedAttempt).toEqual({ eventID: 13, laneID: "main", seq: 4, phase: "started" })
    expect(restoreActivityInspector(
      { ...inspector, attemptEventID: 12 },
      activityIndex,
      traceIndex,
    )?.selectedAttempt).toEqual({ eventID: 12, laneID: "main", seq: 4, phase: "outcome" })
  })

  it("attempt 事件已不存在时只恢复调用", () => {
    expect(restoreActivityInspector(
      { ...inspector, attemptEventID: 99 },
      activityIndex,
      traceIndex,
    )?.selectedAttempt).toBeNull()
  })
})
