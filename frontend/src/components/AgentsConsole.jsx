import { GitBranch, FileText, ListTodo, MessageSquare, Server } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import AgentSidebar from './AgentSidebar'
import ChatHistory from './ChatHistory'
import CommFlow from './CommFlow'
import LogViewer from './LogViewer'
import TaskBoard from './TaskBoard'
import AgentDetail from './AgentDetail'
import AgentRegistry from './AgentRegistry'

const AGENT_TABS = [
  { key: 'chat', label: 'Chat', icon: MessageSquare },
  { key: 'flow', label: 'Flow', icon: GitBranch },
  { key: 'logs', label: 'Logs', icon: FileText },
  { key: 'tasks', label: 'Tasks', icon: ListTodo },
  { key: 'registry', label: 'Registry', icon: Server },
]

export default function AgentsConsole({ activeSubTab, onSubTabChange }) {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const sidebarOpen = useAppStore((s) => s.sidebarOpen)
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen)
  const setActiveTab = useAppStore((s) => s.setActiveTab)

  const renderAgentContent = () => {
    if (activeSubTab === 'detail' && selectedAgent) {
      return <AgentDetail agentId={selectedAgent} onBack={() => setActiveTab('chat')} />
    }
    switch (activeSubTab) {
      case 'flow':
        return <CommFlow />
      case 'logs':
        return <LogViewer />
      case 'tasks':
        return <TaskBoard />
      case 'registry':
        return <AgentRegistry />
      case 'chat':
      default:
        return <ChatHistory />
    }
  }

  return (
    <div className="flex flex-1 min-h-0 overflow-hidden">
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-30 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}
      <div
        className={clsx(
          'fixed top-24 bottom-0 left-0 z-40 w-64 transition-transform duration-200 ease-in-out',
          'md:static md:w-60 md:translate-x-0 md:transition-none',
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        )}
      >
        <AgentSidebar />
      </div>
      <div className="flex-1 flex flex-col min-w-0 min-h-0">
        <div className="flex items-center gap-1 px-2 py-2 border-b border-surface-200 bg-surface/60 overflow-x-auto">
          <span className="px-2 text-[10px] uppercase tracking-[0.14em] text-gray-600 shrink-0">
            Agent Console
          </span>
          {AGENT_TABS.map((tab) => {
            const Icon = tab.icon
            return (
              <button
                key={tab.key}
                onClick={() => onSubTabChange(tab.key)}
                className={clsx(
                  'flex items-center gap-1.5 px-2.5 py-1.5 rounded text-xs transition-colors whitespace-nowrap',
                  activeSubTab === tab.key
                    ? 'bg-accent/20 text-accent-light'
                    : 'text-gray-500 hover:text-gray-300 hover:bg-surface-100'
                )}
              >
                <Icon size={13} />
                {tab.label}
              </button>
            )
          })}
        </div>
        <div className="flex-1 min-h-0 flex flex-col">{renderAgentContent()}</div>
      </div>
    </div>
  )
}
