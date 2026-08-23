import { useState } from 'react'
import { Send } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import toast from 'react-hot-toast'

export default function CommandInput() {
  const agents = useAppStore((s) => s.agents)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const [target, setTarget] = useState('')
  const [text, setText] = useState('')
  const [sending, setSending] = useState(false)
  const lastTaskId = useAppStore((s) => s.trackedTaskId)

  const effectiveTarget = target || selectedAgent || ''
  const selectedRow = agents.find((row) => row.agent_id === effectiveTarget)
  const selectedState = selectedRow?.agent_state || 'offline'

  const addPendingCommand = useAppStore((s) => s.addPendingCommand)
  const addRealtimeMessage = useAppStore((s) => s.addRealtimeMessage)
  const setTrackedTaskId = useAppStore((s) => s.setTrackedTaskId)

  const handleSend = async () => {
    if (!effectiveTarget || !text.trim()) return
    setSending(true)
    const message = text.trim()
    try {
      const res = await api.sendCommand(effectiveTarget, message)
      const taskId = res?.task_id
      const acceptedAt = res?.accepted_at || new Date().toISOString()
      setTrackedTaskId(taskId || null)

      if (taskId) {
        // Optimistically add to chat and mark as pending
        addRealtimeMessage({
          id: `optimistic-${taskId}`,
          v: 1,
          type: 'command',
          sender_id: 'aggregator',
          recipient_id: effectiveTarget,
          task_id: taskId,
          timestamp: acceptedAt,
          payload: { body: message },
        })
        addPendingCommand(taskId, effectiveTarget)
      }
      setText('')
    } catch {
      toast.error('Failed to send command')
    }
    setSending(false)
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="border-t border-surface-200 p-2 md:p-3 flex flex-col gap-1.5">
      <div className="flex gap-2">
        <select
          value={effectiveTarget}
          onChange={(e) => setTarget(e.target.value)}
          aria-label="Command target"
          className="bg-surface-100 border border-surface-200 rounded px-2 py-1.5 text-sm text-gray-300 w-24 md:w-36 shrink-0"
        >
          <option value="">Target...</option>
          {agents.map((a) => (
            <option key={a.agent_id} value={a.agent_id}>
              {a.card?.name || a.agent_id}
            </option>
          ))}
        </select>
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          aria-label="Command body"
          placeholder="Send command..."
          className="flex-1 min-w-0 bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200 placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-accent/50"
        />
        <button
          onClick={handleSend}
          disabled={sending || !effectiveTarget || !text.trim()}
          aria-label="Send command"
          className="bg-accent hover:bg-accent-dark disabled:opacity-40 text-white px-3 py-1.5 rounded text-sm flex items-center gap-1 transition-colors shrink-0"
        >
          <Send size={14} />
        </button>
      </div>
      {effectiveTarget && (
        <span
          data-selected-agent-status
          role="status"
          aria-label={`Selected agent ${effectiveTarget}: ${selectedState}`}
          className="inline-flex min-w-0 items-center gap-1.5 text-xs text-gray-400"
        >
          <span
            aria-hidden="true"
            className={selectedState === 'online'
              ? 'h-2 w-2 shrink-0 rounded-full bg-status-online'
              : 'h-2 w-2 shrink-0 rounded-full bg-status-offline'}
          />
          <span className="truncate font-mono">{effectiveTarget}</span>
          <span>{selectedState}</span>
        </span>
      )}
      {lastTaskId && (
        <div className="text-[10px] text-gray-500 font-mono pl-1">
          Tracking task: {lastTaskId}
        </div>
      )}
    </div>
  )
}
