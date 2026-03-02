import { useEffect } from 'react'
import { Users } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { agentApi } from '../api/client'
import AgentCard from './AgentCard'

export default function AgentSidebar() {
  const agents = useAppStore((s) => s.agents)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const setAgents = useAppStore((s) => s.setAgents)
  const setSelectedAgent = useAppStore((s) => s.setSelectedAgent)

  useEffect(() => {
    const fetchAgents = async () => {
      try {
        const { data } = await agentApi.list()
        setAgents(data)
      } catch {
        // Will retry on next interval
      }
    }
    fetchAgents()
    const interval = setInterval(fetchAgents, 10000)
    return () => clearInterval(interval)
  }, [setAgents])

  const onlineAgents = agents.filter((a) => a.status === 'online')
  const offlineAgents = agents.filter((a) => a.status !== 'online')
  const sorted = [...onlineAgents, ...offlineAgents]

  return (
    <div className="w-60 bg-surface-50 border-r border-surface-200 flex flex-col h-full">
      <div className="p-3 border-b border-surface-200">
        <div className="flex items-center gap-2 text-sm text-gray-400">
          <Users size={16} />
          <span>Agents</span>
          <span className="ml-auto bg-surface-200 text-gray-300 px-1.5 py-0.5 rounded text-xs">
            {agents.length}
          </span>
        </div>
      </div>

      <div className="p-2">
        <button
          onClick={() => setSelectedAgent(null)}
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
            key={agent.id}
            agent={agent}
            selected={selectedAgent === agent.id}
            onClick={() =>
              setSelectedAgent(selectedAgent === agent.id ? null : agent.id)
            }
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
