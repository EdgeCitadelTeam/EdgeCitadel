import { useState, useEffect } from 'react'
import { X } from 'lucide-react'
import { messageApi } from '../api/client'
import MessageBubble from './MessageBubble'

export default function ConversationThread({ correlationId, onClose }) {
  const [messages, setMessages] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!correlationId) return
    const fetch = async () => {
      setLoading(true)
      try {
        const { data } = await messageApi.list({
          correlation_id: correlationId,
          limit: 200,
        })
        setMessages(data.items?.reverse() || [])
      } catch {
        // ignore
      }
      setLoading(false)
    }
    fetch()
  }, [correlationId])

  if (!correlationId) return null

  return (
    <>
      {/* Mobile: full-screen overlay */}
      <div className="fixed inset-0 bg-black/50 z-40 md:hidden" onClick={onClose} />
      <div className="fixed inset-0 z-50 md:static md:z-auto bg-surface border-l border-surface-200 w-full md:w-80 flex flex-col">
        <div className="flex items-center justify-between p-3 border-b border-surface-200">
          <div className="min-w-0">
            <h3 className="text-sm font-medium text-gray-200">Trace</h3>
            <p className="text-[10px] text-gray-500 truncate">{correlationId}</p>
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
            messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} highlighted />
            ))
          )}
        </div>
      </div>
    </>
  )
}
