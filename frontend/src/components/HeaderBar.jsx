import { useEffect } from 'react'
import { Radio, Activity, AlertTriangle, CheckCircle, Moon, Sun } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { systemApi } from '../api/client'
import StatusBadge from './StatusBadge'

export default function HeaderBar() {
  const wsConnected = useAppStore((s) => s.wsConnected)
  const systemStatus = useAppStore((s) => s.systemStatus)
  const setSystemStatus = useAppStore((s) => s.setSystemStatus)
  const darkMode = useAppStore((s) => s.darkMode)
  const setDarkMode = useAppStore((s) => s.setDarkMode)

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
    <header className="h-12 bg-surface-50 border-b border-surface-200 px-4 flex items-center justify-between shrink-0">
      <div className="flex items-center gap-3">
        <Radio size={18} className="text-accent" />
        <h1 className="text-sm font-semibold text-gray-100">
          OpenClaw Swarm Control
        </h1>
        <StatusBadge
          status={wsConnected ? 'online' : 'error'}
          label={wsConnected ? 'MQTT Connected' : 'Disconnected'}
        />
      </div>

      <div className="flex items-center gap-4 text-xs text-gray-400">
        {systemStatus && (
          <>
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
          </>
        )}
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
