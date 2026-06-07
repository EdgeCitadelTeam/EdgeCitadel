import { useEffect, useState } from 'react'
import {
  Bell,
  Briefcase,
  LayoutDashboard,
  LineChart,
  MessageSquare,
  Search,
} from 'lucide-react'
import clsx from 'clsx'
import useAppStore from './stores/appStore'
import HeaderBar from './components/HeaderBar'
import StockWorkspace from './components/StockWorkspace'
import MarketOverview from './components/MarketOverview'
import PortfolioWorkspace from './components/PortfolioWorkspace'
import AlertCenter from './components/AlertCenter'
import AgentsConsole from './components/AgentsConsole'

const TABS = [
  { key: 'market', label: 'Market', icon: LayoutDashboard, shortcut: '1' },
  { key: 'research', label: 'Research', icon: Search, shortcut: '2' },
  { key: 'portfolio', label: 'Portfolio', icon: Briefcase, shortcut: '3' },
  { key: 'alerts', label: 'Alerts', icon: Bell, shortcut: '4' },
  { key: 'agents', label: 'Agents', icon: MessageSquare, shortcut: '5' },
]

export default function Layout() {
  const activeTab = useAppStore((s) => s.activeTab)
  const setActiveTab = useAppStore((s) => s.setActiveTab)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const sidebarOpen = useAppStore((s) => s.sidebarOpen)
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen)
  const [agentConsoleTab, setAgentConsoleTab] = useState('chat')

  // Legacy deep links still used by agent rows and keyboard regression tests.
  useEffect(() => {
    if (['chat', 'flow', 'logs', 'tasks', 'registry', 'detail'].includes(activeTab)) {
      setAgentConsoleTab(activeTab === 'detail' ? 'chat' : activeTab)
    }
  }, [activeTab])

  const normalizedActiveTab = ['chat', 'flow', 'logs', 'tasks', 'registry', 'detail'].includes(activeTab)
    ? 'agents'
    : activeTab

  const renderContent = () => {
    switch (normalizedActiveTab) {
      case 'market':
        return <MarketOverview />
      case 'research':
        return <StockWorkspace />
      case 'portfolio':
        return <PortfolioWorkspace />
      case 'alerts':
        return <AlertCenter />
      case 'agents':
        return (
          <AgentsConsole
            activeSubTab={activeTab === 'detail' && selectedAgent ? 'detail' : agentConsoleTab}
            onSubTabChange={(tab) => {
              setAgentConsoleTab(tab)
              setActiveTab(tab)
            }}
          />
        )
      default:
        return <MarketOverview />
    }
  }

  return (
    <div className="h-screen flex flex-col bg-surface">
      <HeaderBar />
      <div className="flex flex-1 min-h-0">
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-black/50 z-30 md:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        <div className="flex-1 flex flex-col min-h-0 min-w-0">
          <div className="flex items-center border-b border-surface-200 bg-surface-50 overflow-x-auto">
            <div className="hidden md:flex items-center gap-1 px-3 py-2 text-xs text-gray-500 border-r border-surface-200">
              <LineChart size={14} className="text-accent" />
              Research Terminal
            </div>
            {TABS.map((tab) => {
              const Icon = tab.icon
              return (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={clsx(
                    'flex items-center gap-1.5 px-3 py-2.5 text-xs font-medium transition-colors border-b-2 whitespace-nowrap',
                    'md:px-4',
                    normalizedActiveTab === tab.key
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

          <div className="flex-1 min-h-0 flex flex-col">{renderContent()}</div>
        </div>
      </div>
    </div>
  )
}
