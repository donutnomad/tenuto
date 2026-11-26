import { describe, expect, it } from "vitest"
import { FIRST_PAGE, nextPage, previousPage, runListPage } from "./run-list-page"
import type { RunListItem } from "./types"

const items = (n: number) => Array.from({ length: n }, (_, i) => ({ run_id: `run-${i}` }) as RunListItem)

describe("runListPage", () => {
  it("pages with next_page_token instead of a total", () => {
    expect(runListPage(0, 20, { items: items(20), total: 0, has_more: true, next_page_token: "t1" })).toEqual({ first: 1, last: 20, hasPrevious: false, hasNext: true })
    expect(runListPage(2, 20, { items: items(7), total: 0, has_more: false })).toEqual({ first: 41, last: 47, hasPrevious: true, hasNext: false })
  })

  it("handles an empty or loading page", () => {
    expect(runListPage(0, 20, { items: [], total: 0, has_more: false })).toEqual({ first: 0, last: 0, hasPrevious: false, hasNext: false })
    expect(runListPage(1, 20, undefined)).toEqual({ first: 0, last: 0, hasPrevious: true, hasNext: false })
  })
})

describe("page token stack", () => {
  it("moves forward with next_page_token and back by popping", () => {
    const second = nextPage(FIRST_PAGE, { items: items(2), total: 0, has_more: true, next_page_token: "t1" })
    const third = nextPage(second, { items: items(2), total: 0, has_more: true, next_page_token: "t2" })
    expect(third).toEqual(["", "t1", "t2"])
    expect(previousPage(third)).toEqual(["", "t1"])
    expect(previousPage(previousPage(third))).toEqual(FIRST_PAGE)
  })

  it("stays put at the last and the first page", () => {
    expect(nextPage(["", "t1"], { items: items(1), total: 0, has_more: false })).toEqual(["", "t1"])
    expect(nextPage(FIRST_PAGE, undefined)).toBe(FIRST_PAGE)
    expect(previousPage(FIRST_PAGE)).toBe(FIRST_PAGE)
  })
})
