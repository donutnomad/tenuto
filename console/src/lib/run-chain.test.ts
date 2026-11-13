import { describe, expect, it } from "vitest"
import { buildRunChainItems } from "./run-chain"

describe("buildRunChainItems", () => {
  it("renders a two-run Continue-As-New chain without a phantom gap", () => {
    expect(buildRunChainItems({
      viewingRunID: "019f6525-3fbc-738e-9144-eacd869f7dec",
      firstRunID: "019f6525-3f83-776a-b993-d09384a80585",
      previousRunID: "019f6525-3f83-776a-b993-d09384a80585",
      isActive: true,
    })).toEqual([
      {
        kind: "run",
        id: "019f6525-3f83-776a-b993-d09384a80585",
        badges: ["First", "Previous"],
      },
      {
        kind: "run",
        id: "019f6525-3fbc-738e-9144-eacd869f7dec",
        badges: ["Viewing", "Active"],
      },
    ])
  })

  it("places a known predecessor before the viewed run and a successor after it", () => {
    expect(buildRunChainItems({
      viewingRunID: "run-4",
      firstRunID: "run-1",
      previousRunID: "run-3",
      nextRunID: "run-5",
      isActive: false,
    })).toEqual([
      { kind: "run", id: "run-1", badges: ["First"] },
      { kind: "ellipsis" },
      { kind: "run", id: "run-3", badges: ["Previous"] },
      { kind: "run", id: "run-4", badges: ["Viewing"] },
      { kind: "run", id: "run-5", badges: ["Next"] },
    ])
  })
})
