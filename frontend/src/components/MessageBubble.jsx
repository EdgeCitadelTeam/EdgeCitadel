import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

const typeBadgeColors = {
  command: 'bg-blue-500/20 text-blue-400',
  result: 'bg-green-500/20 text-green-400',
  alert: 'bg-red-500/20 text-red-400',
  error: 'bg-red-500/20 text-red-400',
  info: 'bg-yellow-500/20 text-yellow-400',
  heartbeat: 'bg-gray-500/20 text-gray-400',
  register: 'bg-purple-500/20 text-purple-400',
  task_assign: 'bg-indigo-500/20 text-indigo-400',
  broadcast: 'bg-cyan-500/20 text-cyan-400',
}

export default function MessageBubble({ message, highlighted, onClick }) {
  const senderColor = getAgentColor(message.sender_id)
  const type = message.message_type || 'unknown'
  const badgeColor = typeBadgeColors[type] || 'bg-gray-500/20 text-gray-400'

  const payloadPreview = message.payload
    ? typeof message.payload === 'string'
      ? message.payload
      : JSON.stringify(message.payload, null, 2)
    : ''

  return (
    <div
      onClick={onClick}
      className={clsx(
        'px-3 py-2 rounded-lg border transition-colors cursor-pointer',
        'hover:border-surface-300',
        highlighted
          ? 'border-accent/40 bg-accent/5'
          : 'border-surface-200 bg-surface-50'
      )}
    >
      <div className="flex items-center gap-2 mb-1">
        <span className="font-medium text-sm" style={{ color: senderColor }}>
          {message.sender_id || 'unknown'}
        </span>
        {message.receiver_id && (
          <>
            <span className="text-gray-500 text-xs">&rarr;</span>
            <span
              className="text-sm"
              style={{ color: getAgentColor(message.receiver_id) }}
            >
              {message.receiver_id}
            </span>
          </>
        )}
        <span
          className={clsx(
            'px-1.5 py-0.5 rounded text-[10px] font-medium uppercase',
            badgeColor
          )}
        >
          {type}
        </span>
        <span className="ml-auto text-[10px] text-gray-500" title={fullTimestamp(message.timestamp)}>
          {relativeTime(message.timestamp)}
        </span>
      </div>

      {payloadPreview && (
        <pre className="text-xs text-gray-400 whitespace-pre-wrap break-words max-h-32 overflow-hidden mt-1">
          {payloadPreview.length > 500
            ? payloadPreview.slice(0, 500) + '...'
            : payloadPreview}
        </pre>
      )}

      <div className="flex items-center gap-2 mt-1">
        <span className="text-[10px] text-gray-600 truncate">
          {message.mqtt_topic}
        </span>
        {message.correlation_id && (
          <span className="text-[10px] text-gray-600 bg-surface-200 px-1 rounded">
            {message.correlation_id.slice(0, 8)}
          </span>
        )}
      </div>
    </div>
  )
}
