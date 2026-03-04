import { useState, useEffect, useRef, useCallback } from 'react'
import { ArrowDown, Filter } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { messageApi } from '../api/client'
import MessageBubble from './MessageBubble'
import ConversationThread from './ConversationThread'
import CommandInput from './CommandInput'

const TYPE_OPTIONS = [
  'command',
  'result',
  'alert',
  'info',
  'broadcast',
  'task_assign',
]

export default function ChatHistory() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const realtimeMessages = useAppStore((s) => s.realtimeMessages)
  const messageTypeFilter = useAppStore((s) => s.messageTypeFilter)
  const setMessageTypeFilter = useAppStore((s) => s.setMessageTypeFilter)

  const [historicalMessages, setHistoricalMessages] = useState([])
  const [loading, setLoading] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const [offset, setOffset] = useState(0)
  const [search, setSearch] = useState('')
  const [selectedCorrelation, setSelectedCorrelation] = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)

  const scrollRef = useRef(null)
  const bottomRef = useRef(null)

  // Calculate how many messages fit the visible area (~90px per message bubble)
  const getPageSize = () => {
    const el = scrollRef.current
    if (!el) return 20
    return Math.max(10, Math.ceil(el.clientHeight / 90) + 5)
  }

  const fetchMessages = useCallback(
    async (reset = false) => {
      setLoading(true)
      const newOffset = reset ? 0 : offset
      const pageSize = getPageSize()
      try {
        const params = { limit: pageSize, offset: newOffset }
        if (selectedAgent) params.agent = selectedAgent
        if (messageTypeFilter) params.type = messageTypeFilter
        if (search) params.search = search

        const { data } = await messageApi.list(params)
        const items = data.items || []

        if (reset) {
          setHistoricalMessages(items.reverse())
          setOffset(items.length)
        } else {
          setHistoricalMessages((prev) => [...items.reverse(), ...prev])
          setOffset(newOffset + items.length)
        }
        setHasMore(items.length === pageSize)
      } catch {
        // ignore
      }
      setLoading(false)
    },
    [selectedAgent, messageTypeFilter, search, offset]
  )

  useEffect(() => {
    setOffset(0)
    setHistoricalMessages([])
    fetchMessages(true)
  }, [selectedAgent, messageTypeFilter])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [realtimeMessages, autoScroll])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 100
    setAutoScroll(nearBottom)

    // Load more on scroll to top
    if (el.scrollTop < 50 && hasMore && !loading) {
      fetchMessages(false)
    }
  }

  // Combine historical + realtime, filter for selected agent
  const allMessages = [...historicalMessages]
  const realtimeFiltered = realtimeMessages.filter((m) => {
    if (selectedAgent) {
      return m.sender_id === selectedAgent || m.receiver_id === selectedAgent
    }
    return true
  })
  // Dedupe by id
  const seen = new Set(allMessages.map((m) => m.id))
  for (const m of realtimeFiltered) {
    if (!seen.has(m.id)) {
      allMessages.push(m)
      seen.add(m.id)
    }
  }

  return (
    <div className="flex flex-1 min-h-0">
      <div className="flex flex-col flex-1 min-h-0">
        {/* Filter bar */}
        <div className="flex items-center gap-2 p-2 border-b border-surface-200">
          <Filter size={14} className="text-gray-500" />
          <select
            value={messageTypeFilter || ''}
            onChange={(e) => setMessageTypeFilter(e.target.value || null)}
            className="bg-surface-100 border border-surface-200 rounded px-2 py-1 text-xs text-gray-300"
          >
            <option value="">All types</option>
            {TYPE_OPTIONS.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') fetchMessages(true)
            }}
            placeholder="Search messages..."
            className="bg-surface-100 border border-surface-200 rounded px-2 py-1 text-xs text-gray-300 placeholder:text-gray-600 w-48 focus:outline-none focus:ring-1 focus:ring-accent/50"
          />
        </div>

        {/* Messages */}
        <div
          ref={scrollRef}
          onScroll={handleScroll}
          className="flex-1 overflow-y-auto p-3 space-y-2"
        >
          {loading && historicalMessages.length === 0 && (
            <p className="text-xs text-gray-500 text-center py-4">
              Loading messages...
            </p>
          )}
          {allMessages.map((msg) => (
            <MessageBubble
              key={msg.id}
              message={msg}
              highlighted={
                selectedCorrelation &&
                msg.correlation_id === selectedCorrelation
              }
              onClick={() => {
                if (msg.correlation_id) {
                  setSelectedCorrelation(
                    selectedCorrelation === msg.correlation_id
                      ? null
                      : msg.correlation_id
                  )
                }
              }}
            />
          ))}
          {allMessages.length === 0 && !loading && (
            <p className="text-xs text-gray-500 text-center py-8">
              No messages yet. Waiting for MQTT traffic...
            </p>
          )}
          <div ref={bottomRef} />
        </div>

        {/* Jump to latest */}
        {!autoScroll && (
          <button
            onClick={() => {
              setAutoScroll(true)
              bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
            }}
            className="absolute bottom-20 right-8 bg-accent text-white px-3 py-1.5 rounded-full text-xs shadow-lg flex items-center gap-1"
          >
            <ArrowDown size={12} />
            Latest
          </button>
        )}

        <CommandInput />
      </div>

      {/* Trace panel */}
      {selectedCorrelation && (
        <ConversationThread
          correlationId={selectedCorrelation}
          onClose={() => setSelectedCorrelation(null)}
        />
      )}
    </div>
  )
}
