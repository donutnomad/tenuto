import { useState } from "react"
import { api, errorMessage } from "@/lib/api"
import type { Step } from "@/lib/types"

export type RunConfirmAction = "cancel" | "terminate"

// Run 详情页的操作：cancel / terminate / set-policy / reset / reset-activity。
// 所有操作共用一条错误提示和一条成功提示；成功后刷新 run 与事件历史。
// 刷新失败由页面的加载错误提示显示，不覆盖操作本身的成功结果。
export function useRunActions({
  namespace,
  runID,
  mutateRun,
  refreshHistory,
  ensureFullHistory,
}: {
  namespace: string
  runID: string
  mutateRun: () => Promise<unknown>
  refreshHistory: () => Promise<void>
  ensureFullHistory: () => Promise<unknown>
}) {
  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [actionSuccess, setActionSuccess] = useState<string | null>(null)
  const [confirmAction, setConfirmAction] = useState<RunConfirmAction | null>(null)
  const [terminateReason, setTerminateReason] = useState("")
  const [showResetPanel, setShowResetPanel] = useState(false)
  const [resetActivityTarget, setResetActivityTarget] = useState<Step | null>(null)
  const [resetActivityLoading, setResetActivityLoading] = useState(false)

  async function confirm(action: RunConfirmAction) {
    setActionLoading(action)
    setActionError(null)
    setActionSuccess(null)
    try {
      switch (action) {
        case "cancel":
          await api.cancelWorkflow(namespace, runID)
          break
        case "terminate":
          await api.terminateWorkflow(
            namespace,
            runID,
            terminateReason.trim() || "Terminated from Tenuto Console",
          )
          break
      }
      setConfirmAction(null)
      setTerminateReason("")
      setActionSuccess(action === "cancel" ? "Cancellation requested." : "Workflow terminated.")
    } catch (error) {
      setActionError(errorMessage(error, `Failed to ${action} workflow`))
      return
    } finally {
      setActionLoading(null)
    }
    await refreshAfterAction()
  }

  async function refreshAfterAction() {
    await Promise.allSettled([mutateRun(), refreshHistory()])
  }

  // 关闭确认弹窗；操作进行中保持打开。
  function dismissConfirm() {
    if (actionLoading !== null) return
    setActionError(null)
    setConfirmAction(null)
    setTerminateReason("")
  }

  async function setPolicy(policy: "auto_upgrade" | "pinned") {
    setActionLoading("set-policy")
    setActionError(null)
    setActionSuccess(null)
    try {
      await api.setRunPolicy(namespace, runID, policy)
      setActionSuccess(
        policy === "pinned"
          ? "Run pinned to its current content."
          : "AutoUpgrade resumed; the next workflow task will use the latest published content.",
      )
    } catch (error) {
      setActionError(errorMessage(error, "Failed to update run policy"))
      return
    } finally {
      setActionLoading(null)
    }
    await refreshAfterAction()
  }

  // Reset、Replay、Relationships 需要完整事件历史；加载失败显示在操作错误提示里。
  async function loadFullHistory(): Promise<boolean> {
    try {
      await ensureFullHistory()
      return true
    } catch (error) {
      setActionError(errorMessage(error, "Failed to load complete Event History"))
      return false
    }
  }

  async function openResetPanel() {
    if (await loadFullHistory()) setShowResetPanel(true)
  }

  async function resetActivity() {
    if (!resetActivityTarget) return
    setResetActivityLoading(true)
    setActionError(null)
    setActionSuccess(null)
    const target = resetActivityTarget
    try {
      await api.resetActivity(namespace, runID, target.stepID, { reason: "Reset from Tenuto Console" })
      setResetActivityTarget(null)
      setActionSuccess(`Activity ${target.stepID} reset to attempt 1 and rescheduled.`)
    } catch (error) {
      // 关闭弹窗，让错误显示在页面的操作错误提示里
      setResetActivityTarget(null)
      setActionError(errorMessage(error, "Failed to reset activity"))
      return
    } finally {
      setResetActivityLoading(false)
    }
    // 同时刷新 Run 状态和 events history——reset 产生的新事件在 events 里，mutateRun 不会刷它
    await refreshAfterAction()
  }

  return {
    actionLoading,
    actionError,
    actionSuccess,

    confirmAction,
    setConfirmAction,
    terminateReason,
    setTerminateReason,
    confirm,
    dismissConfirm,

    setPolicy,

    showResetPanel,
    setShowResetPanel,
    openResetPanel,
    loadFullHistory,

    resetActivityTarget,
    setResetActivityTarget,
    resetActivityLoading,
    resetActivity,
  }
}
