import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import StatusBadge from './StatusBadge'

// agent shape (from /api/agents):
//   { agent_id, card, agent_state, last_heartbeat, last_register,
//     deployment, heartbeat_interval_sec }
// card.metadata may include runtime.roles, runtime.deployment, ...
export default function AgentCard({ agent, selected, onClick }) {
  const id = agent.agent_id
  const color = getAgentColor(id)
  const state = agent.agent_state || 'offline'
  const isOnline = state === 'online'

  const meta = agent.card?.metadata || {}
  const roles = Array.isArray(meta['runtime.roles']) ? meta['runtime.roles'] : []
  const subtitle = roles.length
    ? roles.join(' · ')
    : agent.card?.description || agent.deployment || 'Agent'

  return (
    <button
      onClick={onClick}
      data-agent-id={id}
      aria-pressed={selected}
      aria-label={`Select agent ${id}`}
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
          {id.slice(0, 2).toUpperCase()}
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-100 truncate">
              {agent.card?.name || id}
            </span>
            <StatusBadge status={state} />
          </div>
          <div className="text-xs text-gray-500 truncate">{subtitle}</div>
        </div>
      </div>
    </button>
  )
}
