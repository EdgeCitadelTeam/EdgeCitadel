import { MessageSquare, GitBranch, FileText, ListTodo } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from './stores/appStore'
import HeaderBar from './components/HeaderBar'
import AgentSidebar from './components/AgentSidebar'
import ChatHistory from './components/ChatHistory'
import CommFlow from './components/CommFlow'
import LogViewer from './components/LogViewer'
import TaskBoard from './components/TaskBoard'
import AgentDetail from './components/AgentDetail'

const TABS = [
  { key: 'chat', label: 'Chat', icon: MessageSquare, shortcut: '1' },
  { key: 'flow', label: 'Flow', icon: GitBranch, shortcut: '2' },
  { key: 'logs', label: 'Logs', icon: FileText, shortcut: '3' },
  { key: 'tasks', label: 'Tasks', icon: ListTodo, shortcut: '4' },
]

export default function Layout() {
  const activeTab = useAppStore((s) => s.activeTab)
  const setActiveTab = useAppStore((s) => s.setActiveTab)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const setSelectedAgent = useAppStore((s) => s.setSelectedAgent)
  const sidebarOpen = useAppStore((s) => s.sidebarOpen)
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen)

  const showDetail = activeTab === 'detail' && selectedAgent

  const renderContent = () => {
    if (showDetail) {
      return (
        <AgentDetail
          agentId={selectedAgent}
          onBack={() => setActiveTab('chat')}
        />
      )
    }
    switch (activeTab) {
      case 'chat':
        return <ChatHistory />
      case 'flow':
        return <CommFlow />
      case 'logs':
        return <LogViewer />
      case 'tasks':
        return <TaskBoard />
      default:
        return <ChatHistory />
    }
  }

  return (
    <div className="h-screen flex flex-col bg-surface">
      <HeaderBar />
      <div className="flex flex-1 min-h-0">
        {/* Mobile sidebar backdrop */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-black/50 z-30 md:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        {/* Sidebar: fixed overlay on mobile, static in flex on desktop */}
        <div
          className={clsx(
            'fixed top-12 bottom-0 left-0 z-40 w-64 transition-transform duration-200 ease-in-out',
            'md:static md:w-60 md:translate-x-0 md:transition-none',
            sidebarOpen ? 'translate-x-0' : '-translate-x-full'
          )}
        >
          <AgentSidebar />
        </div>

        {/* Main content */}
        <div className="flex-1 flex flex-col min-h-0 min-w-0">
          {/* Tab bar */}
          <div className="flex items-center border-b border-surface-200 bg-surface-50 overflow-x-auto">
            {TABS.map((tab) => {
              const Icon = tab.icon
              return (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={clsx(
                    'flex items-center gap-1.5 px-3 py-2.5 text-xs font-medium transition-colors border-b-2 whitespace-nowrap',
                    'md:px-4',
                    activeTab === tab.key
                      ? 'text-accent-light border-accent'
                      : 'text-gray-500 border-transparent hover:text-gray-300'
                  )}
                >
                  <Icon size={14} />
                  {tab.label}
                  <kbd className="ml-1 text-[10px] text-gray-600 bg-surface-200 px-1 rounded hidden sm:inline">
                    {tab.shortcut}
                  </kbd>
                </button>
              )
            })}
          </div>

          {/* Content */}
          <div className="flex-1 min-h-0 flex flex-col">{renderContent()}</div>
        </div>
      </div>
    </div>
  )
}
