import { lazy, Suspense } from 'react'
import { MessageSquare, GitBranch, Server } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from './stores/appStore'
import HeaderBar from './components/HeaderBar'
import AgentSidebar from './components/AgentSidebar'
import ChatHistory from './components/ChatHistory'
import CommFlow from './components/CommFlow'
import AgentDetail from './components/AgentDetail'
import AgentRegistry from './components/AgentRegistry'

const TraceExplorer = lazy(() => import('./traces/TraceExplorer'))

const TABS = [
  { key: 'chat', label: 'Chat', icon: MessageSquare, shortcut: '1' },
  { key: 'flow', label: 'Flow', icon: GitBranch, shortcut: '2' },
  { key: 'execution', label: 'Agent Flow', icon: GitBranch, shortcut: '3' },
  { key: 'registry', label: 'Registry', icon: Server, shortcut: '4' },
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
        return <><div className="flex items-center justify-between gap-3 px-4 py-2 text-xs text-gray-400"><span>Communication topology · broker links are illustrative</span><button className="text-accent-light" onClick={() => setActiveTab('execution')}>Open execution map</button></div><CommFlow /></>
      case 'execution':
        return <Suspense fallback={<p role="status" className="p-4">Loading execution map…</p>}><TraceExplorer /></Suspense>
      case 'registry':
        return <AgentRegistry />
      default:
        return <ChatHistory />
    }
  }

  return (
    <div className="h-screen flex flex-col bg-surface">
      <HeaderBar />
      <div className="flex flex-1 min-h-0">
        {/* Mobile sidebar backdrop */}
        {sidebarOpen && activeTab !== 'execution' && (
          <div
            className="fixed inset-0 bg-black/50 z-30 md:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        {/* Sidebar: fixed overlay on mobile, static in flex on desktop */}
        {activeTab !== 'execution' && <div
          className={clsx(
            'fixed top-16 bottom-0 left-0 z-40 w-64 transition-transform duration-200 ease-in-out',
            'md:static md:w-60 md:translate-x-0 md:transition-none',
            sidebarOpen ? 'translate-x-0' : '-translate-x-full'
          )}
        >
          <AgentSidebar />
        </div>}

        {/* Main content */}
        <div className="flex-1 flex flex-col min-h-0 min-w-0">
          {/* Tab bar */}
          <nav aria-label="Main navigation" className="flex items-center gap-1 px-3 border-b border-surface-200 bg-surface-50 overflow-x-auto">
            {TABS.map((tab) => {
              const Icon = tab.icon
              return (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  aria-current={activeTab === tab.key ? 'page' : undefined}
                  className={clsx(
                    'flex items-center gap-2 px-4 py-4 text-base font-medium transition-colors border-b-2 whitespace-nowrap',
                    'md:px-5',
                    activeTab === tab.key
                      ? 'text-accent-light border-accent'
                      : 'text-gray-500 border-transparent hover:text-gray-300'
                  )}
                >
                  <Icon size={18} />
                  {tab.label}

                </button>
              )
            })}
          </nav>

          {/* Content */}
          <div className="flex-1 min-h-0 flex flex-col">{renderContent()}</div>
        </div>
      </div>
    </div>
  )
}
