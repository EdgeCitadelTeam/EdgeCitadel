import clsx from 'clsx'
import { ArrowRight, Terminal, CheckCircle, AlertTriangle, Info, Radio, Zap, Activity, XCircle } from 'lucide-react'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

const typeConfig = {
  command: { icon: Terminal, color: 'text-blue-400' },
  result: { icon: CheckCircle, color: 'text-emerald-400' },
  status: { icon: Info, color: 'text-amber-400' },
  log: { icon: Info, color: 'text-amber-400' },
  delegation: { icon: Zap, color: 'text-indigo-400' },
  cancel: { icon: XCircle, color: 'text-red-400' },
  broadcast: { icon: Radio, color: 'text-cyan-400' },
  'task.progress': { icon: Activity, color: 'text-yellow-400' },
  register: { icon: Info, color: 'text-gray-400' },
  heartbeat: { icon: Info, color: 'text-gray-400' },
}

// A2A task-state badge colors
const taskStateColors = {
  submitted: 'bg-gray-500/20 text-gray-300',
  working: 'bg-yellow-500/20 text-yellow-300',
  'input-required': 'bg-purple-500/20 text-purple-300',
  completed: 'bg-green-500/20 text-green-300',
  failed: 'bg-red-500/20 text-red-300',
  canceled: 'bg-gray-500/20 text-gray-400',
  rejected: 'bg-red-500/20 text-red-400',
  'auth-required': 'bg-orange-500/20 text-orange-300',
}

// Extract human-readable content from the message payload
function extractContent(payload) {
  if (!payload || typeof payload !== 'object') return null

  if (payload.message) return payload.message
  if (payload.content) return payload.content
  if (payload.response) return payload.response
  if (payload.body) return payload.body

  if (payload.result) {
    if (typeof payload.result === 'string') return payload.result
    if (typeof payload.result === 'object') {
      if (payload.result.response) return payload.result.response
      return Object.entries(payload.result)
        .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${v}`)
        .join('\n')
    }
  }

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

  if (payload.status && payload.status !== 'success') return `Status: ${payload.status}`

  return null
}

export default function MessageBubble({ message, highlighted, onClick }) {
  const type = message.type || 'unknown'
  const config = typeConfig[type] || { icon: Info, color: 'text-gray-400' }
  const Icon = config.icon
  const senderColor = getAgentColor(message.sender_id)
  const content = extractContent(message.payload)
  const taskState = message.task_state

  return (
    <div
      onClick={onClick}
      className={clsx(
        'rounded-lg border px-3 py-2 transition-all cursor-pointer border-l-[3px]',
        highlighted && 'ring-1 ring-accent/40 shadow-sm shadow-accent/10',
        'hover:brightness-125'
      )}
      style={{
        borderLeftColor: senderColor,
        backgroundColor: `${senderColor}18`,
        borderColor: highlighted ? undefined : `${senderColor}35`,
      }}
    >
      {/* Header: sender -> recipient, type icon, timestamp */}
      <div className="flex items-center gap-2 min-w-0">
        <Icon size={13} className={clsx(config.color, 'shrink-0')} />
        <span className="font-medium text-xs truncate" style={{ color: senderColor }}>
          {message.sender_id || 'unknown'}
        </span>
        {message.recipient_id && (
          <>
            <ArrowRight size={10} className="text-gray-600 shrink-0" />
            <span
              className="text-xs truncate"
              style={{ color: getAgentColor(message.recipient_id) }}
            >
              {message.recipient_id}
            </span>
          </>
        )}
        <span className={clsx('text-[10px] font-medium uppercase shrink-0 ml-1', config.color)}>
          {type}
        </span>
        {taskState && (
          <span
            className={clsx(
              'text-[10px] font-medium px-1.5 py-0.5 rounded shrink-0',
              taskStateColors[taskState] || 'bg-surface-200 text-gray-400'
            )}
          >
            {taskState}
          </span>
        )}
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

      {/* Task / context badges */}
      {(message.task_id || message.context_id) && (
        <div className="mt-1.5 flex items-center gap-1.5 flex-wrap">
          {message.task_id && (
            <span className="text-[10px] text-gray-600 bg-surface-200/50 px-1.5 py-0.5 rounded font-mono" title={`task_id: ${message.task_id}`}>
              t:{message.task_id.slice(0, 8)}
            </span>
          )}
          {message.context_id && (
            <span className="text-[10px] text-gray-600 bg-surface-200/30 px-1.5 py-0.5 rounded font-mono" title={`context_id: ${message.context_id}`}>
              c:{message.context_id.slice(0, 8)}
            </span>
          )}
        </div>
      )}
    </div>
  )
}
