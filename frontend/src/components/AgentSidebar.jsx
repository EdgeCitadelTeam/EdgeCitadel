import { useEffect } from 'react'
import { Users, X } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import AgentCard from './AgentCard'

export default function AgentSidebar() {
  const agents = useAppStore((s) => s.agents)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const setAgents = useAppStore((s) => s.setAgents)
  const setSelectedAgent = useAppStore((s) => s.setSelectedAgent)
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen)
  const showTestAgents = useAppStore((s) => s.showTestAgents)

  useEffect(() => {
    const fetchAgents = async () => {
      try {
        const isOperatorAgent = (a) => {
          const roles = a.card?.metadata?.['runtime.roles'] || []
          return !roles.some((r) => r === 'aggregator')
        }
        const items = await api.listAgents()
        const operators = (items || []).filter(isOperatorAgent)
        const filtered = showTestAgents
          ? operators
          : operators.filter((a) => {
              const meta = a.card?.metadata || {}
              const deployment = meta['runtime.deployment'] || a.deployment
              return deployment !== 'test'
            })
        setAgents(filtered || [])
      } catch {
        // Will retry on next interval
      }
    }
    fetchAgents()
    const interval = setInterval(fetchAgents, 10000)
    return () => clearInterval(interval)
  }, [setAgents, showTestAgents])

  const handleSelect = (agentId) => {
    setSelectedAgent(selectedAgent === agentId ? null : agentId)
    setSidebarOpen(false)
  }

  const onlineAgents = agents.filter((a) => a.agent_state === 'online')
  const offlineAgents = agents.filter((a) => a.agent_state !== 'online')
  const sorted = [...onlineAgents, ...offlineAgents]

  return (
    <div className="w-full bg-surface-50 border-r border-surface-200 flex flex-col h-full">
      <div className="p-3 border-b border-surface-200">
        <div className="flex items-center gap-2 text-sm text-gray-400">
          <Users size={16} />
          <span>Agents</span>
          <span className="ml-auto bg-surface-200 text-gray-300 px-1.5 py-0.5 rounded text-xs">
            {agents.length}
          </span>
          {/* Close button - mobile only */}
          <button
            onClick={() => setSidebarOpen(false)}
            className="p-1 hover:bg-surface-200 rounded md:hidden"
          >
            <X size={14} />
          </button>
        </div>
      </div>

      <div className="p-2">
        <button
          onClick={() => handleSelect(null)}
          className={clsx(
            'w-full text-left px-3 py-2 rounded-lg text-sm transition-colors',
            'hover:bg-surface-100',
            selectedAgent === null
              ? 'bg-surface-100 text-accent-light font-medium'
              : 'text-gray-300'
          )}
        >
          All Agents
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {sorted.map((agent) => (
          <AgentCard
            key={agent.agent_id}
            agent={agent}
            selected={selectedAgent === agent.agent_id}
            onClick={() => handleSelect(agent.agent_id)}
          />
        ))}
        {agents.length === 0 && (
          <p className="text-xs text-gray-500 text-center py-8">
            No agents connected
          </p>
        )}
      </div>
    </div>
  )
}
