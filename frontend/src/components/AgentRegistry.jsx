import { useEffect, useMemo, useRef, useState } from 'react'
import { ArrowDown, ArrowUp } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import toast from 'react-hot-toast'
import RegistryRow from './RegistryRow'
import { ageSec } from '../utils/heartbeatAge'

const REFRESH_MS = 5000
const TICK_MS = 1000

const COLUMNS = [
  { key: 'agent_id', label: 'Agent ID' },
  { key: 'roles', label: 'Roles' },
  { key: 'kind', label: 'Kind' },
  { key: 'agent_state', label: 'State' },
  { key: 'heartbeat_age', label: 'Heartbeat' },
  { key: 'queue', label: 'Queue (p / ap)' },
  { key: 'poison_count', label: 'Poison' },
  { key: 'deployment', label: 'Deployment' },
]

function compareRows(a, b, sortKey, sortDir) {
  const dir = sortDir === 'asc' ? 1 : -1
  const get = (r) => {
    if (sortKey === 'roles') return (r.card?.metadata?.['runtime.roles'] || []).join(',')
    if (sortKey === 'kind') return r.card?.metadata?.['runtime.kind'] || ''
    if (sortKey === 'heartbeat_age') return ageSec(r.last_heartbeat)
    if (sortKey === 'queue') return (r.queue?.pending || 0) + (r.queue?.ack_pending || 0)
    return r[sortKey] ?? ''
  }
  const av = get(a)
  const bv = get(b)
  if (av < bv) return -1 * dir
  if (av > bv) return 1 * dir
  return 0
}

export default function AgentRegistry() {
  const registry = useAppStore((s) => s.registry)
  const setRegistry = useAppStore((s) => s.setRegistry)
  const showTestAgents = useAppStore((s) => s.showTestAgents)
  const setSelectedAgent = useAppStore((s) => s.setSelectedAgent)
  const setActiveTab = useAppStore((s) => s.setActiveTab)

  const [sortKey, setSortKey] = useState('agent_state')
  const [sortDir, setSortDir] = useState('asc') // offline < online alphabetically; we override below
  const [tick, setTick] = useState(0)
  const tickTimer = useRef(null)
  const fetchTimer = useRef(null)

  // Initial + interval fetch
  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const rows = await api.getRegistry()
        if (!cancelled) setRegistry(rows || [])
      } catch (e) {
        if (!cancelled) toast.error('Failed to load registry')
      }
    }
    load()
    fetchTimer.current = setInterval(load, REFRESH_MS)
    return () => {
      cancelled = true
      clearInterval(fetchTimer.current)
    }
  }, [setRegistry])

  // Local clock tick for heartbeat-age column
  useEffect(() => {
    tickTimer.current = setInterval(() => setTick((t) => t + 1), TICK_MS)
    return () => clearInterval(tickTimer.current)
  }, [])

  const visibleRows = useMemo(() => {
    let rows = registry
    if (!showTestAgents) {
      rows = rows.filter((r) => (r.deployment || 'default') !== 'test')
    }
    // Default ordering: offline > error > busy > online, ties by heartbeat-age desc
    const stateRank = { offline: 0, error: 1, busy: 2, online: 3 }
    const sorted = [...rows].sort((a, b) => {
      if (sortKey === 'agent_state' && sortDir === 'asc') {
        const ra = stateRank[a.agent_state] ?? 99
        const rb = stateRank[b.agent_state] ?? 99
        if (ra !== rb) return ra - rb
        return ageSec(b.last_heartbeat) - ageSec(a.last_heartbeat)
      }
      return compareRows(a, b, sortKey, sortDir)
    })
    return sorted
  }, [registry, showTestAgents, sortKey, sortDir, tick])

  const handleSort = (key) => {
    if (key === sortKey) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const handleRowClick = (agentId) => {
    setSelectedAgent(agentId)
    setActiveTab('detail')
  }

  if (registry.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-gray-500">
          No agents registered. Start an adapter and refresh.
        </p>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <table className="w-full text-xs">
        <thead className="bg-surface-50 sticky top-0">
          <tr>
            {COLUMNS.map((col) => {
              if (col.key === 'deployment' && !showTestAgents) return null
              const active = col.key === sortKey
              return (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={clsx(
                    'px-3 py-2 text-left font-medium cursor-pointer select-none',
                    'hover:bg-surface-100',
                    active ? 'text-accent-light' : 'text-gray-400'
                  )}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {active && (sortDir === 'asc'
                      ? <ArrowUp size={12} />
                      : <ArrowDown size={12} />)}
                  </span>
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((r) => (
            <RegistryRow
              key={r.agent_id}
              row={r}
              onClick={handleRowClick}
              showTestAgents={showTestAgents}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}
