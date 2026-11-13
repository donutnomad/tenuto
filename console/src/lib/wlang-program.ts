import type { WorkflowConfig } from "@/lib/types"

// getWlangProgram 返回传给 WASM 回放的完整程序产物 JSON。
export function getWlangProgram(workflowConfig: WorkflowConfig): string {
  return JSON.stringify(workflowConfig.program)
}
