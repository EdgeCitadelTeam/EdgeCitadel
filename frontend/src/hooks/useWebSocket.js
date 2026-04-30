import { useEffect, useRef, useCallback } from 'react'
import useAppStore from '../stores/appStore'

const MAX_RECONNECT_DELAY = 30000
const PING_INTERVAL = 15000

export default function useWebSocket(agentName = null) {
  const wsRef = useRef(null)
  const reconnectDelay = useRef(1000)
  const pingTimer = useRef(null)
  const reconnectTimer = useRef(null)

  const setWsConnected = useAppStore((s) => s.setWsConnected)
  const addRealtimeMessage = useAppStore((s) => s.addRealtimeMessage)
  const updateAgentStatus = useAppStore((s) => s.updateAgentStatus)
  const addNotification = useAppStore((s) => s.addNotification)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const path = agentName ? `/ws/agent/${agentName}` : '/ws/stream'
    const url = `${protocol}//${host}${path}`

    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setWsConnected(true)
      reconnectDelay.current = 1000

      // Start heartbeat ping
      pingTimer.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send('ping')
        }
      }, PING_INTERVAL)
    }

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)

        if (data.event === 'message') {
          // Skip heartbeat/register messages from chat display
          const msgType = data.data?.type
          if (msgType !== 'heartbeat' && msgType !== 'register') {
            addRealtimeMessage(data.data)
          }
        } else if (data.event === 'agent_status_change') {
          updateAgentStatus(data.data.agent_id, data.data.agent_state || data.data.status)
          if (data.data.status === 'offline') {
            addNotification({
              type: 'warning',
              title: 'Agent Offline',
              message: `${data.data.agent_id} went offline`,
            })
          }
        } else if (data.event === 'agent_registered') {
          addNotification({
            type: 'info',
            title: 'Agent Registered',
            message: `${data.data.agent_id} connected`,
          })
        } else if (data.event === 'agent_deleted') {
          // Registry tab listens to this via the store
          const removeRow = useAppStore.getState().removeRegistryRow
          removeRow(data.data.agent_id)
        } else if (data.event === 'log') {
          if (data.data?.level === 'ERROR') {
            addNotification({
              type: 'error',
              title: 'Error',
              message: data.data.message,
            })
          }
        }
      } catch {
        // Ignore non-JSON messages
      }
    }

    ws.onclose = () => {
      setWsConnected(false)
      clearInterval(pingTimer.current)

      // Exponential backoff reconnect
      reconnectTimer.current = setTimeout(() => {
        reconnectDelay.current = Math.min(
          reconnectDelay.current * 2,
          MAX_RECONNECT_DELAY
        )
        connect()
      }, reconnectDelay.current)
    }

    ws.onerror = () => {
      ws.close()
    }
  }, [agentName, setWsConnected, addRealtimeMessage, updateAgentStatus, addNotification])

  useEffect(() => {
    connect()

    // Reconnect on tab visibility change and network recovery
    const handleVisibility = () => {
      if (document.visibilityState === 'visible' && wsRef.current?.readyState !== WebSocket.OPEN) {
        connect()
      }
    }
    const handleOnline = () => {
      if (wsRef.current?.readyState !== WebSocket.OPEN) {
        connect()
      }
    }
    document.addEventListener('visibilitychange', handleVisibility)
    window.addEventListener('online', handleOnline)

    return () => {
      document.removeEventListener('visibilitychange', handleVisibility)
      window.removeEventListener('online', handleOnline)
      clearInterval(pingTimer.current)
      clearTimeout(reconnectTimer.current)
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [connect])

  const sendMessage = useCallback((data) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data))
    }
  }, [])

  return { sendMessage }
}
