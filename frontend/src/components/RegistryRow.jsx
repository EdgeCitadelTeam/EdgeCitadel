import clsx from 'clsx'
import { AlertOctagon } from 'lucide-react'
import StatusBadge from './StatusBadge'
import { ageSec } from '../utils/heartbeatAge'

/**
 * Renders a single <tr> for the AgentRegistry table.
 * Props:
 *   row             — registry entry object
 *   onClick         — called with row.agent_id when the row is clicked
 *   showTestAgents  — controls whether the Deployment column cell is rendered
 */
export default function RegistryRow({ row, onClick, showTestAgents }) {
  const meta = row.card?.metadata || {}
  const roles = meta['runtime.roles'] || []
  const kind = meta['runtime.kind'] || ''

  const age = ageSec(row.last_heartbeat)
  const ageLabel =
    age === Infinity
      ? '—'
      : age < 60
      ? `${Math.round(age)}s`
      : age < 3600
      ? `${Math.round(age / 60)}m`
      : `${Math.round(age / 3600)}h`

  const poisonClass =
    row.poison_count > 0 ? 'text-red-400 font-medium' : 'text-gray-500'

  return (
    <tr
      key={row.agent_id}
      onClick={() => onClick(row.agent_id)}
      className="border-t border-surface-200 hover:bg-surface-100 cursor-pointer"
    >
      <td className="px-3 py-2 font-medium">{row.agent_id}</td>
      <td className="px-3 py-2 text-gray-400">{roles.join(', ')}</td>
      <td className="px-3 py-2 text-gray-400">{kind}</td>
      <td className="px-3 py-2">
        <StatusBadge state={row.agent_state} />
      </td>
      <td className="px-3 py-2 text-gray-400">{ageLabel}</td>
      <td className="px-3 py-2 text-gray-400">
        {(row.queue?.pending ?? 0)} / {(row.queue?.ack_pending ?? 0)}
      </td>
      <td className={clsx('px-3 py-2', poisonClass)}>
        {row.poison_count > 0 && (
          <AlertOctagon size={12} className="inline mr-1" />
        )}
        {row.poison_count}
      </td>
      {showTestAgents && (
        <td className="px-3 py-2 text-gray-500">
          {row.deployment || 'default'}
        </td>
      )}
    </tr>
  )
}
