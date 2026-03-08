import clsx from 'clsx'
import { ArrowRight, Terminal, CheckCircle, AlertTriangle, Info, Radio, Zap } from 'lucide-react'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

const typeConfig = {
  command: { icon: Terminal, color: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-500/20' },
  result: { icon: CheckCircle, color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
  alert: { icon: AlertTriangle, color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
  error: { icon: AlertTriangle, color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
  info: { icon: Info, color: 'text-amber-400', bg: 'bg-amber-500/10 border-amber-500/20' },
  broadcast: { icon: Radio, color: 'text-cyan-400', bg: 'bg-cyan-500/10 border-cyan-500/20' },
  task_assign: { icon: Zap, color: 'text-indigo-400', bg: 'bg-indigo-500/10 border-indigo-500/20' },
}

// Extract human-readable content from the message payload
function extractContent(payload) {
  if (!payload || typeof payload !== 'object') return null

  // Direct message/response/content text
  if (payload.message) return payload.message
  if (payload.content) return payload.content
  if (payload.response) return payload.response

  // Result object - format key details
  if (payload.result) {
    if (typeof payload.result === 'string') return payload.result
    if (typeof payload.result === 'object') {
      // Check for response text inside result
      if (payload.result.response) return payload.result.response
      return Object.entries(payload.result)
        .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${v}`)
        .join('\n')
    }
  }

  // Command details
  if (payload.command) {
    const parts = [payload.command]
    if (payload.payload) {
      if (typeof payload.payload === 'string') {
        parts.push(payload.payload)
      } else if (payload.payload.message) {
        parts.push(payload.payload.message)
      } else {
        const details = Object.entries(payload.payload)
          .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
          .join(', ')
        if (details) parts.push(details)
      }
    }
    return parts.join('\n')
  }

  // Status info
  if (payload.status && payload.status !== 'success') return `Status: ${payload.status}`

  return null
}

export default function MessageBubble({ message, highlighted, onClick }) {
  const type = message.message_type || 'unknown'
  const config = typeConfig[type] || { icon: Info, color: 'text-gray-400', bg: 'bg-surface-100 border-surface-200' }
  const Icon = config.icon
  const senderColor = getAgentColor(message.sender_id)
  const content = extractContent(message.payload)

  return (
    <div
      onClick={onClick}
      className={clsx(
        'rounded-lg border px-3 py-2 transition-all cursor-pointer',
        highlighted
          ? 'border-accent/50 bg-accent/5 shadow-sm shadow-accent/10'
          : config.bg,
        'hover:brightness-110'
      )}
    >
      {/* Header: sender -> receiver, type icon, timestamp */}
      <div className="flex items-center gap-2 min-w-0">
        <Icon size={13} className={clsx(config.color, 'shrink-0')} />
        <span className="font-medium text-xs truncate" style={{ color: senderColor }}>
          {message.sender_id || 'unknown'}
        </span>
        {message.receiver_id && (
          <>
            <ArrowRight size={10} className="text-gray-600 shrink-0" />
            <span
              className="text-xs truncate"
              style={{ color: getAgentColor(message.receiver_id) }}
            >
              {message.receiver_id}
            </span>
          </>
        )}
        <span className={clsx('text-[10px] font-medium uppercase shrink-0 ml-1', config.color)}>
          {type}
        </span>
        <span
          className="ml-auto text-[10px] text-gray-600 shrink-0"
          title={fullTimestamp(message.timestamp)}
        >
          {relativeTime(message.timestamp)}
        </span>
      </div>

      {/* Content */}
      {content && (
        <div className="mt-1.5 text-[13px] text-gray-300 leading-relaxed whitespace-pre-wrap break-words max-h-24 overflow-hidden">
          {content.length > 400 ? content.slice(0, 400) + '...' : content}
        </div>
      )}

      {/* Correlation badge */}
      {message.correlation_id && (
        <div className="mt-1.5 flex items-center">
          <span className="text-[10px] text-gray-600 bg-surface-200/50 px-1.5 py-0.5 rounded font-mono">
            {message.correlation_id.slice(0, 8)}
          </span>
        </div>
      )}
    </div>
  )
}
