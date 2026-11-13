import { useId } from "react"
import type { ResetType } from "@/lib/types"

const RESET_TYPE_OPTIONS: { value: ResetType; label: string; description: string }[] = [
  {
    value: "last_workflow_task",
    label: "Last Decision Point",
    description: "Reset each run to its most recent workflow task completion.",
  },
  {
    value: "first_workflow_task",
    label: "First Decision Point",
    description: "Re-run each workflow from the beginning.",
  },
]

// ResetTypeOptions 是批量 Reset 的锚点选择；Server 要求批量 Reset 携带 reset_type。
export function ResetTypeOptions({ value, onChange }: { value: ResetType; onChange: (value: ResetType) => void }) {
  const name = useId()
  return (
    <fieldset className="space-y-1.5">
      <legend className="text-sm font-medium">Reset Strategy</legend>
      <div className="space-y-2">
        {RESET_TYPE_OPTIONS.map((option) => (
          <label key={option.value} className="flex items-start gap-2 text-sm cursor-pointer">
            <input
              type="radio"
              name={name}
              value={option.value}
              checked={value === option.value}
              onChange={() => onChange(option.value)}
              className="accent-primary mt-0.5"
            />
            <div>
              <span className="font-medium">{option.label}</span>
              <p className="text-muted-foreground mt-0.5">{option.description}</p>
            </div>
          </label>
        ))}
      </div>
    </fieldset>
  )
}
