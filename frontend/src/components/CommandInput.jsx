import { useState } from 'react'
import { Send } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { commandApi } from '../api/client'
import toast from 'react-hot-toast'

export default function CommandInput() {
  const agents = useAppStore((s) => s.agents)
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const [target, setTarget] = useState('')
  const [text, setText] = useState('')
  const [sending, setSending] = useState(false)

  const effectiveTarget = target || selectedAgent || ''

  const addPendingCommand = useAppStore((s) => s.addPendingCommand)
  const addRealtimeMessage = useAppStore((s) => s.addRealtimeMessage)

  const handleSend = async () => {
    if (!effectiveTarget || !text.trim()) return
    setSending(true)
    const correlationId = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
    const message = text.trim()
    try {
      await commandApi.send(effectiveTarget, {
        message_type: 'command',
        sender_id: 'dashboard',
        receiver_id: effectiveTarget,
        correlation_id: correlationId,
        payload: { message },
      })
      // Optimistically add message to chat and mark as pending
      addRealtimeMessage({
        id: `optimistic-${correlationId}`,
        sender_id: 'dashboard',
        receiver_id: effectiveTarget,
        message_type: 'command',
        correlation_id: correlationId,
        payload: { message },
        timestamp: new Date().toISOString(),
      })
      addPendingCommand(correlationId, effectiveTarget)
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
    <div className="border-t border-surface-200 p-2 md:p-3 flex gap-2">
      <select
        value={effectiveTarget}
        onChange={(e) => setTarget(e.target.value)}
        className="bg-surface-100 border border-surface-200 rounded px-2 py-1.5 text-sm text-gray-300 w-24 md:w-36 shrink-0"
      >
        <option value="">Target...</option>
        {agents.map((a) => (
          <option key={a.id} value={a.id}>
            {a.display_name || a.id}
          </option>
        ))}
      </select>
      <input
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Send command..."
        className="flex-1 min-w-0 bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200 placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-accent/50"
      />
      <button
        onClick={handleSend}
        disabled={sending || !effectiveTarget || !text.trim()}
        className="bg-accent hover:bg-accent-dark disabled:opacity-40 text-white px-3 py-1.5 rounded text-sm flex items-center gap-1 transition-colors shrink-0"
      >
        <Send size={14} />
      </button>
    </div>
  )
}
