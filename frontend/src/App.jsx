import { useEffect } from 'react'
import useAppStore from './stores/appStore'
import useWebSocket from './hooks/useWebSocket'
import Layout from './Layout'
import Toast from './components/Toast'
import toast from 'react-hot-toast'

export default function App() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const activeTab = useAppStore((s) => s.activeTab)
  const setActiveTab = useAppStore((s) => s.setActiveTab)
  const notifications = useAppStore((s) => s.notifications)

  // WebSocket connection
  useWebSocket(selectedAgent)

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e) => {
      // Tab switching with 1-5
      if (!e.ctrlKey && !e.metaKey && !e.altKey) {
        const target = e.target
        if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.tagName === 'SELECT') {
          return
        }
        switch (e.key) {
          case '1':
            setActiveTab('chat')
            break
          case '2':
            setActiveTab('flow')
            break
          case '3':
            setActiveTab('logs')
            break
          case '4':
            setActiveTab('tasks')
            break
          case '5':
            setActiveTab('registry')
            break
        }
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [setActiveTab])

  // Show toast for notifications
  useEffect(() => {
    if (notifications.length === 0) return
    const latest = notifications[0]
    if (!latest._toasted) {
      latest._toasted = true
      switch (latest.type) {
        case 'error':
          toast.error(`${latest.title}: ${latest.message}`)
          break
        case 'warning':
          toast(latest.message, { icon: '⚠️' })
          break
        default:
          toast(latest.message)
      }
    }
  }, [notifications])

  return (
    <>
      <Layout />
      <Toast />
    </>
  )
}
