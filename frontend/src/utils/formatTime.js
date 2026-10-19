import { formatDistanceToNow, format } from 'date-fns'

export function relativeTime(dateStr) {
  if (!dateStr) return ''
  try {
    const date = new Date(dateStr)
    return formatDistanceToNow(date, { addSuffix: true })
  } catch {
    return dateStr
  }
}

export function fullTimestamp(dateStr) {
  if (!dateStr) return ''
  try {
    return format(new Date(dateStr), 'yyyy-MM-dd HH:mm:ss')
  } catch {
    return dateStr
  }
}
