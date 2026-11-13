import { describe, expect, it } from "vitest"
import { tokenizeWlangLine } from "./wlang-syntax"

function kinds(line: string) {
  return tokenizeWlangLine(line)
    .filter((token) => token.kind !== "plain" || token.text.trim() !== "")
    .map((token) => [token.text, token.kind])
}

describe("tokenizeWlangLine", () => {
  it("高亮 workflow 调用、类型和变量声明", () => {
    expect(kinds("let out = workflow.ExecuteActivity(ctx, symbol activities.CheckBalance, in)")).toEqual([
      ["let", "keyword"],
      ["out", "plain"],
      ["=", "operator"],
      ["workflow", "namespace"],
      [".", "operator"],
      ["ExecuteActivity", "function"],
      ["(", "operator"],
      ["ctx", "plain"],
      [",", "operator"],
      ["symbol", "keyword"],
      ["activities", "namespace"],
      [".", "operator"],
      ["CheckBalance", "function"],
      [",", "operator"],
      ["in", "plain"],
      [")", "operator"],
    ])
  })

  it("高亮容器类型、字符串、数字和常量", () => {
    expect(kinds('let input = map<string,any> { "amount": 42, "ok": true, "next": null }')).toContainEqual([
      "map",
      "type",
    ])
    expect(kinds('let input = map<string,any> { "amount": 42, "ok": true, "next": null }')).toEqual(
      expect.arrayContaining([
        ['"amount"', "string"],
        ["42", "number"],
        ["true", "constant"],
        ["null", "constant"],
      ]),
    )
  })

  it("高亮 workflow 限定类型", () => {
    expect(kinds("fn(ctx: workflow.Context) -> error {")).toEqual(
      expect.arrayContaining([
        ["fn", "keyword"],
        ["workflow", "namespace"],
        ["Context", "type"],
        ["error", "type"],
      ]),
    )
  })

  it("整段注释保持单一 token", () => {
    expect(tokenizeWlangLine("  // wait for lanes")).toEqual([
      { text: "  ", kind: "plain" },
      { text: "// wait for lanes", kind: "comment" },
    ])
  })
})
