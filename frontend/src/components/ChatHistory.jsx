import { useState, useEffect, useRef, useCallback } from 'react'
import { ArrowDown, Filter, Search, Loader2 } from 'lucide-react'
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
  const showTestAgents = useAppStore((s) => s.showTestAgents)
  const pendingCommands = useAppStore((s) => s.pendingCommands)

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
  }, [selectedAgent, messageTypeFilter, showTestAgents])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [realtimeMessages, pendingCommands, autoScroll])

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
  const merged = [...historicalMessages]
  const realtimeFiltered = realtimeMessages.filter((m) => {
    if (selectedAgent) {
      return m.sender_id === selectedAgent || m.receiver_id === selectedAgent
    }
    return true
  })
  // Dedupe by id, and also dedupe optimistic commands vs server-confirmed ones
  const seen = new Set(merged.map((m) => m.id))
  const seenCorrCmd = new Set(
    merged
      .filter((m) => m.message_type === 'command' && m.correlation_id)
      .map((m) => m.correlation_id)
  )
  for (const m of realtimeFiltered) {
    if (seen.has(m.id)) continue
    // Skip if we already have a command with this correlation (optimistic vs server)
    if (m.message_type === 'command' && m.correlation_id && seenCorrCmd.has(m.correlation_id)) continue
    merged.push(m)
    seen.add(m.id)
    if (m.message_type === 'command' && m.correlation_id) seenCorrCmd.add(m.correlation_id)
  }

  // Group by correlation_id so command-reply pairs stay together,
  // then sort groups by the command (earliest) timestamp.
  const grouped = new Map()
  const ungrouped = []
  for (const m of merged) {
    if (m.correlation_id) {
      if (!grouped.has(m.correlation_id)) grouped.set(m.correlation_id, [])
      grouped.get(m.correlation_id).push(m)
    } else {
      ungrouped.push(m)
    }
  }
  // Sort within each group: commands first, then by timestamp
  for (const msgs of grouped.values()) {
    msgs.sort((a, b) => {
      if (a.message_type === 'command' && b.message_type !== 'command') return -1
      if (a.message_type !== 'command' && b.message_type === 'command') return 1
      return new Date(a.timestamp) - new Date(b.timestamp)
    })
  }
  // Build final list: sort groups by their first message timestamp, interleave ungrouped
  const allEntries = [
    ...Array.from(grouped.values()).map((msgs) => ({ ts: new Date(msgs[0].timestamp), msgs })),
    ...ungrouped.map((m) => ({ ts: new Date(m.timestamp), msgs: [m] })),
  ]
  allEntries.sort((a, b) => a.ts - b.ts)
  const allMessages = allEntries.flatMap((e) => e.msgs)

  return (
    <div className="flex flex-1 min-h-0 overflow-hidden">
      {/* Main chat column */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Filter bar */}
        <div className="flex items-center gap-2 px-3 md:px-4 py-2 border-b border-surface-200 bg-surface-50 shrink-0 flex-wrap">
          <Filter size={14} className="text-gray-500 shrink-0" />
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
          <div className="relative flex-1 min-w-[120px] max-w-[200px]">
            <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-500" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') fetchMessages(true)
              }}
              placeholder="Search..."
              className="w-full bg-surface-100 border border-surface-200 rounded pl-7 pr-2 py-1 text-xs text-gray-300 placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-accent/50"
            />
          </div>
          {(messageTypeFilter || search) && (
            <button
              onClick={() => {
                setMessageTypeFilter(null)
                setSearch('')
              }}
              className="text-[10px] text-gray-500 hover:text-gray-300 ml-1"
            >
              Clear
            </button>
          )}
        </div>

        {/* Messages area */}
        <div className="relative flex-1 min-h-0">
          <div
            ref={scrollRef}
            onScroll={handleScroll}
            className="absolute inset-0 overflow-y-auto px-3 md:px-4 py-3 space-y-1.5"
          >
            {loading && historicalMessages.length === 0 && (
              <div className="flex items-center justify-center py-8">
                <div className="text-xs text-gray-500">Loading messages...</div>
              </div>
            )}
            {hasMore && historicalMessages.length > 0 && (
              <div className="text-center py-2">
                <button
                  onClick={() => fetchMessages(false)}
                  disabled={loading}
                  className="text-[10px] text-gray-500 hover:text-accent transition-colors"
                >
                  {loading ? 'Loading...' : 'Load older messages'}
                </button>
              </div>
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
              <div className="flex flex-col items-center justify-center py-16 text-gray-500">
                <div className="text-sm">No messages yet</div>
                <div className="text-xs mt-1">Waiting for agent traffic...</div>
              </div>
            )}
            {/* Thinking indicators for pending commands */}
            {Object.entries(pendingCommands).map(([corrId, { target }]) => (
              <div
                key={`pending-${corrId}`}
                className="rounded-lg border border-surface-200 bg-surface-100/50 px-3 py-2.5 flex items-center gap-2.5 animate-pulse"
              >
                <Loader2 size={14} className="text-accent animate-spin" />
                <span className="text-xs text-gray-400">
                  <span className="text-gray-300 font-medium">{target}</span> is thinking...
                </span>
                <span className="text-[10px] text-gray-600 ml-auto">
                  {corrId.slice(0, 8)}
                </span>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>

          {/* Jump to latest */}
          {!autoScroll && (
            <button
              onClick={() => {
                setAutoScroll(true)
                bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
              }}
              className="absolute bottom-4 right-4 bg-accent hover:bg-accent-dark text-white px-3 py-1.5 rounded-full text-xs shadow-lg flex items-center gap-1 transition-colors z-10"
            >
              <ArrowDown size={12} />
              Latest
            </button>
          )}
        </div>

        {/* Command input */}
        <div className="shrink-0">
          <CommandInput />
        </div>
      </div>

      {/* Trace panel - hidden on mobile, shown as overlay */}
      {selectedCorrelation && (
        <ConversationThread
          correlationId={selectedCorrelation}
          onClose={() => setSelectedCorrelation(null)}
        />
      )}
    </div>
  )
}
