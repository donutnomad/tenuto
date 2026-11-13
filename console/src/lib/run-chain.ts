export type RunChainBadge = "First" | "Previous" | "Viewing" | "Active" | "Next"

export type RunChainItem =
  | { kind: "run"; id: string; badges: RunChainBadge[] }
  | { kind: "ellipsis" }

export interface BuildRunChainItemsInput {
  viewingRunID: string
  firstRunID: string
  previousRunID?: string
  nextRunID?: string
  isActive: boolean
}

export function buildRunChainItems({
  viewingRunID,
  firstRunID,
  previousRunID,
  nextRunID,
  isActive,
}: BuildRunChainItemsInput): RunChainItem[] {
  const items: RunChainItem[] = []
  const firstID = firstRunID || viewingRunID

  const pushRun = (id: string, badges: RunChainBadge[]) => {
    if (!id) return
    const existing = items.find(
      (item): item is Extract<RunChainItem, { kind: "run" }> => item.kind === "run" && item.id === id,
    )
    if (existing) {
      for (const badge of badges) {
        if (!existing.badges.includes(badge)) existing.badges.push(badge)
      }
      return
    }
    items.push({ kind: "run", id, badges: [...badges] })
  }

  if (firstID !== viewingRunID) pushRun(firstID, ["First"])

  if (previousRunID && previousRunID !== viewingRunID) {
    if (previousRunID !== firstID && items.length > 0) items.push({ kind: "ellipsis" })
    pushRun(previousRunID, ["Previous"])
  } else if (firstID !== viewingRunID) {
    items.push({ kind: "ellipsis" })
  }

  const viewingBadges: RunChainBadge[] = []
  if (firstID === viewingRunID) viewingBadges.push("First")
  viewingBadges.push("Viewing")
  if (isActive) viewingBadges.push("Active")
  pushRun(viewingRunID, viewingBadges)

  if (nextRunID && nextRunID !== viewingRunID) pushRun(nextRunID, ["Next"])

  return items
}
