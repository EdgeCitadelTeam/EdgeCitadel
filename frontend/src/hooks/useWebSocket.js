import { useCallback, useEffect, useMemo, useRef } from 'react'
import useAppStore from '../stores/appStore'
import { applyRealtimeEvent } from './realtimeEvents'

const MAX_RECONNECT_DELAY = 30000
const PING_INTERVAL = 15000

export default function useWebSocket() {
  const wsRef = useRef(null)
  const reconnectDelay = useRef(1000)
  const pingTimer = useRef(null)
  const reconnectTimer = useRef(null)

  const setWsConnected = useAppStore((s) => s.setWsConnected)
  const addRealtimeMessage = useAppStore((s) => s.addRealtimeMessage)
  const appendStreamDelta = useAppStore((s) => s.appendStreamDelta)
  const finalizeStream = useAppStore((s) => s.finalizeStream)
  const updateAgentStatus = useAppStore((s) => s.updateAgentStatus)
  const upsertAgent = useAppStore((s) => s.upsertAgent)
  const upsertRegistryRow = useAppStore((s) => s.upsertRegistryRow)
  const removeAgent = useAppStore((s) => s.removeAgent)
  const addNotification = useAppStore((s) => s.addNotification)
  const actions = useMemo(() => ({
    addRealtimeMessage,
    appendStreamDelta,
    finalizeStream,
    updateAgentStatus,
    upsertAgent,
    upsertRegistryRow,
    removeAgent,
    addNotification,
  }), [
    addRealtimeMessage,
    appendStreamDelta,
    finalizeStream,
    updateAgentStatus,
    upsertAgent,
    upsertRegistryRow,
    removeAgent,
    addNotification,
  ])

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const url = `${protocol}//${window.location.host}/ws/stream`
    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setWsConnected(true)
      reconnectDelay.current = 1000
      pingTimer.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send('ping')
      }, PING_INTERVAL)
    }

    ws.onmessage = (event) => {
      try {
        const frame = JSON.parse(event.data)
        applyRealtimeEvent(frame, actions)
        if (frame.event === 'log' && frame.data?.level === 'ERROR') {
          actions.addNotification({
            type: 'error',
            title: 'Error',
            message: frame.data.message,
          })
        }
      } catch {
        // Ignore non-JSON messages.
      }
    }

    ws.onclose = () => {
      setWsConnected(false)
      clearInterval(pingTimer.current)
      reconnectTimer.current = setTimeout(() => {
        reconnectDelay.current = Math.min(reconnectDelay.current * 2, MAX_RECONNECT_DELAY)
        connect()
      }, reconnectDelay.current)
    }

    ws.onerror = () => ws.close()
  }, [actions, setWsConnected])

  useEffect(() => {
    connect()
    const handleVisibility = () => {
      if (document.visibilityState === 'visible' && wsRef.current?.readyState !== WebSocket.OPEN) connect()
    }
    const handleOnline = () => {
      if (wsRef.current?.readyState !== WebSocket.OPEN) connect()
    }
    document.addEventListener('visibilitychange', handleVisibility)
    window.addEventListener('online', handleOnline)
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility)
      window.removeEventListener('online', handleOnline)
      clearInterval(pingTimer.current)
      clearTimeout(reconnectTimer.current)
      if (wsRef.current) wsRef.current.close()
    }
  }, [connect])

  const sendMessage = useCallback((data) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) wsRef.current.send(JSON.stringify(data))
  }, [])

  return { sendMessage }
}
