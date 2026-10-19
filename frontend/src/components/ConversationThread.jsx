import { useState, useEffect } from 'react'
import { X } from 'lucide-react'
import { api } from '../api/client'
import MessageBubble from './MessageBubble'

// Collapse multiple persisted task.progress envelopes for the same task_id
// into a single synthetic STREAMING bubble. Without this, a streamed task
// renders one tiny bubble per chunk in the trace panel — useless for
// reading the assistant's reply. The synthetic bubble's content is the
// concatenation of payload.message across chunks (chronological order),
// keyed off the earliest chunk's metadata. If the task has a result
// envelope (terminal), the progress chunks are dropped entirely — the
// result carries the full text.
function collapseProgressChunks(messages) {
  const tasksWithResult = new Set()
  for (const m of messages) {
    if (m.task_id && m.type === 'result') tasksWithResult.add(m.task_id)
  }
  const progressByTask = new Map()
  for (const m of messages) {
    if (m.task_id && m.type === 'task.progress' && !tasksWithResult.has(m.task_id)) {
      if (!progressByTask.has(m.task_id)) progressByTask.set(m.task_id, [])
      progressByTask.get(m.task_id).push(m)
    }
  }
  const collapsed = []
  const insertedTaskBubble = new Set()
  for (const m of messages) {
    if (m.type === 'task.progress' && m.task_id) {
      // Drop progress envelopes for tasks that have a result (the result
      // carries the full text already).
      if (tasksWithResult.has(m.task_id)) continue
      // For in-flight tasks, emit one merged bubble at the position of the
      // earliest progress chunk; skip subsequent chunks for the same task.
      if (insertedTaskBubble.has(m.task_id)) continue
      const chunks = progressByTask.get(m.task_id)
      const sorted = chunks.slice().sort(
        (a, b) => new Date(a.timestamp) - new Date(b.timestamp))
      const content = sorted.map((c) => c.payload?.message || '').join('')
      const first = sorted[0]
      const last = sorted[sorted.length - 1]
      collapsed.push({
        id: `trace-stream-${m.task_id}`,
        task_id: m.task_id,
        sender_id: first.sender_id,
        recipient_id: first.recipient_id,
        type: 'result',
        streaming: true,
        skill_id: first.payload?.skill_id,
        content: content || '(no chunks captured)',
        payload: { message: content },
        timestamp: first.timestamp,
        last_delta_at: new Date(last.timestamp).getTime(),
      })
      insertedTaskBubble.add(m.task_id)
      continue
    }
    collapsed.push(m)
  }
  return collapsed
}

// Renders a single trace. Pass either taskId (preferred — single A2A task)
// or contextId (multi-task chain). Falls back to taskId if both supplied.
export default function ConversationThread({ taskId, contextId, onClose }) {
  const [messages, setMessages] = useState([])
  const [loading, setLoading] = useState(true)

  const tracingId = taskId || contextId
  const tracingKind = taskId ? 'task_id' : 'context_id'

  useEffect(() => {
    if (!tracingId) return
    let cancelled = false
    const fetch = async () => {
      setLoading(true)
      try {
        const params = { limit: 200 }
        if (taskId) params.task_id = taskId
        else if (contextId) params.context_id = contextId
        const items = await api.queryMessages(params)
        if (!cancelled) {
          setMessages((items || []).slice().reverse())
        }
      } catch {
        // ignore
      }
      if (!cancelled) setLoading(false)
    }
    fetch()
    return () => {
      cancelled = true
    }
  }, [taskId, contextId, tracingId])

  if (!tracingId) return null

  return (
    <>
      {/* Mobile: full-screen overlay */}
      <div className="fixed inset-0 bg-black/50 z-40 md:hidden" onClick={onClose} />
      <div className="fixed inset-0 z-50 md:static md:z-auto bg-surface border-l border-surface-200 w-full md:w-80 flex flex-col">
        <div className="flex items-center justify-between p-3 border-b border-surface-200">
          <div className="min-w-0">
            <h3 className="text-sm font-medium text-gray-200">Trace</h3>
            <p className="text-[10px] text-gray-500 truncate">
              {tracingKind}: {tracingId}
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-surface-200 rounded shrink-0"
          >
            <X size={14} />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-2">
          {loading ? (
            <p className="text-xs text-gray-500 text-center py-4">Loading...</p>
          ) : messages.length === 0 ? (
            <p className="text-xs text-gray-500 text-center py-4">
              No messages found
            </p>
          ) : (
            collapseProgressChunks(messages).map((msg) => (
              <MessageBubble key={msg.id} message={msg} highlighted />
            ))
          )}
        </div>
      </div>
    </>
  )
}
