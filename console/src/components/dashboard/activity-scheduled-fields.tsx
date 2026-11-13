import type { Step } from "@/lib/types"
import { formatTimeoutMs } from "@/lib/format"

const TIMEOUT_FIELDS: { label: string; key: keyof Step }[] = [
  { label: "Schedule To Start Timeout", key: "scheduleToStartTimeoutMs" },
  { label: "Schedule To Close Timeout", key: "scheduleToCloseTimeoutMs" },
  { label: "Start To Close Timeout", key: "startToCloseTimeoutMs" },
  { label: "Heartbeat Timeout", key: "heartbeatTimeoutMs" },
]

export function ActivityTimeoutFields({
  step,
  Detail,
}: {
  step: Step
  Detail: React.ComponentType<{ label: string; value: string }>
}) {
  const rp = step.retryPolicy
  return (
    <>
      {TIMEOUT_FIELDS.map(({ label, key }) => {
        const ms = step[key] as number | undefined
        return ms != null ? <Detail key={key} label={label} value={formatTimeoutMs(ms)} /> : null
      })}
      {/* maxAttempts: 0 在后端语义为"无限重试"，显式展示而非吞掉 */}
      {rp?.maxAttempts != null ? (
        <Detail label="Retry Policy Maximum Attempts" value={rp.maxAttempts === 0 ? "Unlimited" : String(rp.maxAttempts)} />
      ) : null}
      {rp?.initialIntervalMs != null ? <Detail label="Retry Policy Initial Interval" value={formatTimeoutMs(rp.initialIntervalMs)} /> : null}
      {rp?.backoffCoefficient != null ? <Detail label="Retry Policy Backoff Coefficient" value={String(rp.backoffCoefficient)} /> : null}
      {rp?.maxIntervalMs != null ? <Detail label="Retry Policy Maximum Interval" value={formatTimeoutMs(rp.maxIntervalMs)} /> : null}
    </>
  )
}
