import { useEffect } from 'react'
import { Radio, Activity, AlertTriangle, CheckCircle, Moon, Sun, Menu, FlaskConical } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { systemApi } from '../api/client'
import StatusBadge from './StatusBadge'

export default function HeaderBar() {
  const wsConnected = useAppStore((s) => s.wsConnected)
  const systemStatus = useAppStore((s) => s.systemStatus)
  const setSystemStatus = useAppStore((s) => s.setSystemStatus)
  const darkMode = useAppStore((s) => s.darkMode)
  const setDarkMode = useAppStore((s) => s.setDarkMode)
  const showTestAgents = useAppStore((s) => s.showTestAgents)
  const setShowTestAgents = useAppStore((s) => s.setShowTestAgents)
  const sidebarOpen = useAppStore((s) => s.sidebarOpen)
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen)

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const { data } = await systemApi.status()
        setSystemStatus(data)
      } catch {
        // Will retry
      }
    }
    fetchStatus()
    const interval = setInterval(fetchStatus, 30000)
    return () => clearInterval(interval)
  }, [setSystemStatus])

  const toggleDarkMode = () => {
    const next = !darkMode
    setDarkMode(next)
    document.documentElement.classList.toggle('dark', next)
  }

  return (
    <header className="h-12 bg-surface-50 border-b border-surface-200 px-3 md:px-4 flex items-center justify-between shrink-0">
      <div className="flex items-center gap-2 md:gap-3 min-w-0">
        {/* Hamburger menu - mobile only */}
        <button
          onClick={() => setSidebarOpen(!sidebarOpen)}
          className="p-1 hover:bg-surface-200 rounded transition-colors md:hidden"
        >
          <Menu size={18} className="text-gray-400" />
        </button>

        <Radio size={18} className="text-accent shrink-0" />
        <h1 className="text-sm font-semibold text-gray-100 truncate">
          <span className="hidden sm:inline">OpenClaw Swarm Control</span>
          <span className="sm:hidden">OpenClaw</span>
        </h1>
        <StatusBadge
          status={wsConnected ? 'online' : 'error'}
          label={wsConnected ? 'MQTT Connected' : 'Disconnected'}
        />
      </div>

      <div className="flex items-center gap-2 md:gap-4 text-xs text-gray-400 shrink-0">
        {systemStatus && (
          <div className="hidden md:flex items-center gap-4">
            <span className="flex items-center gap-1">
              <Activity size={12} />
              {systemStatus.agents_online}/{systemStatus.agents_total} online
            </span>
            <span className="flex items-center gap-1">
              <CheckCircle size={12} />
              {systemStatus.total_messages} msgs
            </span>
            <span className="flex items-center gap-1">
              <Activity size={12} />
              {systemStatus.active_tasks} tasks
            </span>
            {systemStatus.errors_today > 0 && (
              <span className="flex items-center gap-1 text-status-error">
                <AlertTriangle size={12} />
                {systemStatus.errors_today} errors
              </span>
            )}
          </div>
        )}

        {/* Test data toggle */}
        <button
          onClick={() => setShowTestAgents(!showTestAgents)}
          className={clsx(
            'flex items-center gap-1 px-2 py-1 rounded text-xs transition-colors',
            showTestAgents
              ? 'bg-yellow-500/20 text-yellow-400'
              : 'bg-surface-200 text-gray-500'
          )}
          title={showTestAgents ? 'Showing test data — click to hide' : 'Test data hidden — click to show'}
        >
          <FlaskConical size={12} />
          <span className="hidden sm:inline">{showTestAgents ? 'Test' : 'No Test'}</span>
        </button>

        <button
          onClick={toggleDarkMode}
          className="p-1 hover:bg-surface-200 rounded transition-colors"
        >
          {darkMode ? <Sun size={14} /> : <Moon size={14} />}
        </button>
      </div>
    </header>
  )
}
