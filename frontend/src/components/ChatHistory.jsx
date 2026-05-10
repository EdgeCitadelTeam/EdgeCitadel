import { useState, useEffect, useRef, useCallback } from 'react'
import { ArrowDown, Filter, Search, Loader2 } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import MessageBubble from './MessageBubble'
import ConversationThread from './ConversationThread'
import CommandInput from './CommandInput'

const TYPE_OPTIONS = [
  'command',
  'result',
  'status',
  'log',
  'broadcast',
  'delegation',
  'cancel',
  'task.progress',
]

export default function ChatHistory() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const realtimeMessages = useAppStore((s) => s.realtimeMessages)
  const messageTypeFilter = useAppStore((s) => s.messageTypeFilter)
  const setMessageTypeFilter = useAppStore((s) => s.setMessageTypeFilter)
  const showTestAgents = useAppStore((s) => s.showTestAgents)
  const pendingCommands = useAppStore((s) => s.pendingCommands)
  const removePendingCommand = useAppStore((s) => s.removePendingCommand)
  const seedStreamFromHistory = useAppStore((s) => s.seedStreamFromHistory)

  const [historicalMessages, setHistoricalMessages] = useState([])
  const [loading, setLoading] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const [search, setSearch] = useState('')
  const [selectedTask, setSelectedTask] = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)

  const scrollRef = useRef(null)
  const bottomRef = useRef(null)

  // Calculate how many messages fit the visible area (~90px per message bubble)
  const getPageSize = () => {
    const el = scrollRef.current
    if (!el) return 20
    return Math.max(10, Math.ceil(el.clientHeight / 90) + 5)
  }

  const fetchMessages = useCallback(async () => {
    setLoading(true)
    const limit = getPageSize() * 4
    try {
      const params = { limit }
      if (selectedAgent) params.agent_id = selectedAgent
      if (messageTypeFilter) params.type = messageTypeFilter
      // Hide test-deployment messages by default. Aggregator tags rows
      // with the sender/recipient agent's runtime.deployment from the
      // cached A2A card (see MessageRouter._deployment_for). Pairs with
      // the showTestAgents toggle that already filters AgentSidebar.
      // Server-side filter — pre-page-size LIMIT only sees prod rows.
      if (!showTestAgents) params.exclude_deployment = 'test'

      let items = (await api.queryMessages(params)) || []
      if (search) {
        const needle = search.toLowerCase()
        items = items.filter((m) =>
          JSON.stringify(m.payload || {}).toLowerCase().includes(needle)
        )
      }
      // API returns newest-first; reverse for chronological display.
      setHistoricalMessages(items.slice().reverse())
      setHasMore(items.length === limit)
    } catch {
      // ignore
    }
    setLoading(false)
  }, [selectedAgent, messageTypeFilter, search, showTestAgents])

  useEffect(() => {
    setHistoricalMessages([])
    fetchMessages()
  }, [selectedAgent, messageTypeFilter, showTestAgents, fetchMessages])

  // Auto-poll every 3s while the panel is mounted. A proper fix is the
  // /ws/* endpoint family (Phase 1 follow-up in docs/roadmap.md); polling
  // is the v0.1 stopgap so command results actually appear without the
  // user clicking "Refresh".
  useEffect(() => {
    const id = setInterval(fetchMessages, 3000)
    return () => clearInterval(id)
  }, [fetchMessages])

  // Clear pending-command "thinking..." indicators once the polled history
  // contains a terminal envelope (result/task.progress with a terminal
  // task_state) for that task_id. Without this, the indicator never goes
  // away because the dedicated WebSocket clear path in addRealtimeMessage
  // only fires for live-pushed envelopes (and /ws/* endpoints aren't shipped
  // yet — see docs/roadmap.md Phase 1 follow-ups).
  useEffect(() => {
    const pendingIds = Object.keys(pendingCommands)
    if (pendingIds.length === 0) return
    const terminalStates = new Set([
      'completed', 'failed', 'canceled', 'rejected',
    ])
    for (const m of historicalMessages) {
      if (!m.task_id || !pendingIds.includes(m.task_id)) continue
      if (m.type === 'result' && terminalStates.has(m.task_state)) {
        removePendingCommand(m.task_id)
      }
    }
  }, [historicalMessages, pendingCommands, removePendingCommand])

  // Page-refresh recovery: scan polled history for tasks with persisted
  // task.progress chunks but no result envelope, and seed a synthetic
  // streaming bubble from the concatenated chunks. The seed runs through
  // the same reducer that live deltas use, so subsequent WebSocket frames
  // for the same task extend the same bubble (no separate reconstructed
  // view racing the live one).
  useEffect(() => {
    if (messageTypeFilter === 'task.progress') return
    const tasksWithResult = new Set()
    const progressByTask = new Map()
    for (const m of historicalMessages) {
      if (!m.task_id) continue
      if (m.type === 'result') tasksWithResult.add(m.task_id)
      else if (m.type === 'task.progress') {
        if (!progressByTask.has(m.task_id)) progressByTask.set(m.task_id, [])
        progressByTask.get(m.task_id).push(m)
      }
    }
    for (const [taskId, chunks] of progressByTask) {
      if (tasksWithResult.has(taskId)) continue
      const sorted = chunks.slice().sort(
        (a, b) => new Date(a.timestamp) - new Date(b.timestamp))
      const content = sorted.map((c) => c.payload?.message || '').join('')
      if (!content) continue
      const last = sorted[sorted.length - 1]
      seedStreamFromHistory(
        taskId,
        sorted[0].sender_id,
        content,
        sorted[0].payload?.skill_id,
        last.timestamp,
      )
    }
  }, [historicalMessages, messageTypeFilter, seedStreamFromHistory])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [realtimeMessages, pendingCommands, autoScroll])

  // Stall detection: mark streaming bubbles as stalled if no delta for 60s
  useEffect(() => {
    const interval = setInterval(() => {
      const STALL_MS = 60_000
      const now = Date.now()
      useAppStore.setState((state) => ({
        realtimeMessages: state.realtimeMessages.map((m) =>
          m.streaming && !m.stalled && m.last_delta_at &&
          (now - m.last_delta_at) > STALL_MS
            ? { ...m, stalled: true }
            : m
        ),
      }))
    }, 5000)
    return () => clearInterval(interval)
  }, [])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 100
    setAutoScroll(nearBottom)
  }

  // task.progress envelopes are streaming chunks, not standalone chat
  // messages. The synthetic streaming bubble (built from live WS frames in
  // appStore.appendStreamDelta) is the user-visible representation; rendering
  // every persisted progress envelope as its own MessageBubble would produce
  // tens-to-hundreds of fragmented cards per task. Persistence stays intact
  // for ConversationThread / MessageInspector. Operators who want to inspect
  // the raw progress stream can pick task.progress from the type filter.
  const showProgressEnvelopes = messageTypeFilter === 'task.progress'
  const dropProgress = (m) => showProgressEnvelopes || m.type !== 'task.progress'

  // Combine historical + realtime, filter for selected agent.
  // Page-refresh recovery for in-flight streams is handled in a useEffect
  // below — it seeds a single synthetic streaming bubble in zustand from
  // any persisted task.progress chunks, so subsequent live deltas extend
  // the same bubble (rather than racing a separate reconstructed view).
  const merged = historicalMessages.filter(dropProgress)
  const realtimeFiltered = realtimeMessages.filter((m) => {
    if (!dropProgress(m)) return false
    if (selectedAgent) {
      return m.sender_id === selectedAgent || m.recipient_id === selectedAgent
    }
    return true
  })
  // Dedupe by id, and dedupe optimistic commands vs server-confirmed ones by task_id.
  const seen = new Set(merged.map((m) => m.id))
  const seenTaskCmd = new Set(
    merged
      .filter((m) => m.type === 'command' && m.task_id)
      .map((m) => m.task_id)
  )
  for (const m of realtimeFiltered) {
    if (seen.has(m.id)) continue
    if (m.type === 'command' && m.task_id && seenTaskCmd.has(m.task_id)) continue
    merged.push(m)
    seen.add(m.id)
    if (m.type === 'command' && m.task_id) seenTaskCmd.add(m.task_id)
  }

  // Group by task_id so command-result pairs stay together,
  // then sort groups by the command (earliest) timestamp.
  const grouped = new Map()
  const ungrouped = []
  for (const m of merged) {
    if (m.task_id) {
      if (!grouped.has(m.task_id)) grouped.set(m.task_id, [])
      grouped.get(m.task_id).push(m)
    } else {
      ungrouped.push(m)
    }
  }
  for (const msgs of grouped.values()) {
    msgs.sort((a, b) => {
      if (a.type === 'command' && b.type !== 'command') return -1
      if (a.type !== 'command' && b.type === 'command') return 1
      return new Date(a.timestamp) - new Date(b.timestamp)
    })
  }
  const allEntries = [
    ...Array.from(grouped.values()).map((msgs) => ({
      ts: new Date(msgs[0].timestamp),
      msgs,
    })),
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
                if (e.key === 'Enter') fetchMessages()
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
                  onClick={fetchMessages}
                  disabled={loading}
                  className="text-[10px] text-gray-500 hover:text-accent transition-colors"
                >
                  {loading ? 'Loading...' : 'Refresh'}
                </button>
              </div>
            )}
            {allMessages.map((msg) => {
              // For result bubbles without their own skill_id, inherit
              // skill_id from the matching command envelope (same task_id).
              let commandSkillId
              if (msg.type === 'result' && !msg.skill_id && msg.task_id) {
                const cmd = allMessages.find(
                  (m) => m.type === 'command' && m.task_id === msg.task_id && m.skill_id
                )
                commandSkillId = cmd?.skill_id
              }
              return (
                <MessageBubble
                  key={msg.id}
                  message={msg}
                  highlighted={selectedTask && msg.task_id === selectedTask}
                  commandSkillId={commandSkillId}
                  onClick={() => {
                    if (msg.task_id) {
                      setSelectedTask(
                        selectedTask === msg.task_id ? null : msg.task_id
                      )
                    }
                  }}
                />
              )
            })}
            {allMessages.length === 0 && !loading && (
              <div className="flex flex-col items-center justify-center py-16 text-gray-500">
                <div className="text-sm">No messages yet</div>
                <div className="text-xs mt-1">Waiting for agent traffic...</div>
              </div>
            )}
            {/* Thinking indicators for pending commands */}
            {Object.entries(pendingCommands).map(([taskId, { target }]) => (
              <div
                key={`pending-${taskId}`}
                className="rounded-lg border border-surface-200 bg-surface-100/50 px-3 py-2.5 flex items-center gap-2.5 animate-pulse"
              >
                <Loader2 size={14} className="text-accent animate-spin" />
                <span className="text-xs text-gray-400">
                  <span className="text-gray-300 font-medium">{target}</span> is thinking...
                </span>
                <span className="text-[10px] text-gray-600 ml-auto font-mono">
                  {taskId.slice(0, 8)}
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

      {/* Trace panel */}
      {selectedTask && (
        <ConversationThread
          taskId={selectedTask}
          onClose={() => setSelectedTask(null)}
        />
      )}
    </div>
  )
}
