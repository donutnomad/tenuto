export const STATUS_COLORS: Record<string, string> = {
  running: 'bg-blue-500',
  completed: 'bg-green-500',
  failed: 'bg-red-500',
  cancelled: 'bg-amber-500',
  terminated: 'bg-red-600',
  continued_as_new: 'bg-purple-500',
  scheduled: 'bg-gray-300',
  timed_out: 'bg-orange-500',
}

export const STATUS_TEXT_COLORS: Record<string, string> = {
  running: 'text-blue-500',
  completed: 'text-green-500',
  failed: 'text-red-500',
  cancelled: 'text-amber-500',
  terminated: 'text-red-600',
  continued_as_new: 'text-purple-500',
  scheduled: 'text-gray-400',
  timed_out: 'text-orange-500',
}

export const STATUS_ICONS: Record<string, string> = {
  running: '🔄',
  completed: '✅',
  failed: '❌',
  cancelled: '🚫',
  terminated: '⛔',
  continued_as_new: '🔗',
  scheduled: '⏳',
  timed_out: '⏰',
}

export const DEFAULT_NAMESPACE = 'default'
export const REFRESH_INTERVAL_LIST = 5000
export const REFRESH_INTERVAL_DETAIL = 3000
