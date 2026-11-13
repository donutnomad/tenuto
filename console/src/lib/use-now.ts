import { useEffect, useState } from "react"

// 当前时间（毫秒），active 时每秒更新一次；首次 effect 之前为 0。
// 只在真正显示流逝时间的叶子组件里调用，避免每秒重渲染整页。
export function useNow(active: boolean): number {
  const [nowMs, setNowMs] = useState(0)

  useEffect(() => {
    const update = () => setNowMs(Date.now())
    update()
    if (!active) return
    const timer = window.setInterval(update, 1_000)
    return () => window.clearInterval(timer)
  }, [active])

  return nowMs
}
