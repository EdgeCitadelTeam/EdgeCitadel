import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import StatusBadge from './StatusBadge'

export default function AgentCard({ agent, selected, onClick }) {
  const color = getAgentColor(agent.id)
  const isOnline = agent.status === 'online'

  return (
    <button
      onClick={onClick}
      className={clsx(
        'w-full text-left px-3 py-2.5 rounded-lg transition-colors',
        'hover:bg-surface-100',
        selected ? 'bg-surface-100 ring-1 ring-accent/50' : '',
        !isOnline && 'opacity-50'
      )}
    >
      <div className="flex items-center gap-2.5">
        <div
          className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white shrink-0"
          style={{ backgroundColor: color }}
        >
          {agent.id.slice(0, 2).toUpperCase()}
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-100 truncate">
              {agent.display_name || agent.id}
            </span>
            <StatusBadge status={agent.status} />
          </div>
          <div className="text-xs text-gray-500 truncate">
            {agent.role || agent.device_type || 'Agent'}
          </div>
        </div>
      </div>
    </button>
  )
}
