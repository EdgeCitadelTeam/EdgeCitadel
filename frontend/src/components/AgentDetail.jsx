import { useState, useEffect } from 'react'
import { ArrowLeft, Send, AlertOctagon, Inbox } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'
import StatusBadge from './StatusBadge'
import toast from 'react-hot-toast'

const QUEUE_POLL_MS = 5000

export default function AgentDetail({ agentId, onBack }) {
  const [agent, setAgent] = useState(null)
  const [messages, setMessages] = useState([])
  const [tasks, setTasks] = useState([])
  const [command, setCommand] = useState('')
  const [lastTaskId, setLastTaskId] = useState(null)

  const agentQueue = useAppStore((s) => s.agentQueue)
  const poisonEvents = useAppStore((s) => s.poisonEvents)
  const fetchAgentQueue = useAppStore((s) => s.fetchAgentQueue)
  const fetchPoisonEvents = useAppStore((s) => s.fetchPoisonEvents)

  // Initial agent + history load
  useEffect(() => {
    if (!agentId) return
    let cancelled = false
    const load = async () => {
      try {
        const [agentRes, msgRes, taskRes] = await Promise.all([
          api.getAgent(agentId),
          api.queryMessages({ agent_id: agentId, limit: 50 }),
          api.queryMessages({ agent_id: agentId, type: 'result', limit: 20 }),
        ])
        if (cancelled) return
        setAgent(agentRes)
        setMessages(msgRes || [])
        setTasks(taskRes || [])
      } catch {
        // ignore
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [agentId])

  // Poll queue depth + poison events while panel is open.
  useEffect(() => {
    if (!agentId) return
    fetchAgentQueue(agentId)
    fetchPoisonEvents(agentId)
    const queueTimer = setInterval(() => fetchAgentQueue(agentId), QUEUE_POLL_MS)
    const poisonTimer = setInterval(() => fetchPoisonEvents(agentId), QUEUE_POLL_MS * 6)
    return () => {
      clearInterval(queueTimer)
      clearInterval(poisonTimer)
    }
  }, [agentId, fetchAgentQueue, fetchPoisonEvents])

  const handleSendCommand = async () => {
    if (!command.trim()) return
    try {
      const res = await api.sendCommand(agentId, command.trim())
      setLastTaskId(res?.task_id || null)
      setCommand('')
      toast.success(`Sent to ${agentId}`)
    } catch {
      toast.error('Failed to send command')
    }
  }

  if (!agent) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-gray-500">Loading agent details...</p>
      </div>
    )
  }

  const id = agent.agent_id
  const color = getAgentColor(id)
  const meta = agent.card?.metadata || {}
  const roles = Array.isArray(meta['runtime.roles']) ? meta['runtime.roles'] : []
  const heartbeatInterval =
    agent.heartbeat_interval_sec ?? meta['runtime.heartbeat_interval_sec']
  const queue = agentQueue[id]
  const poison = poisonEvents[id] || []
  const skills = agent.card?.skills || []

  return (
    <div className="h-full overflow-y-auto p-3 md:p-4">
      {/* Back button */}
      <button
        onClick={onBack}
        className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-200 mb-4"
      >
        <ArrowLeft size={14} />
        Back to all
      </button>

      {/* Profile header */}
      <div className="flex items-start gap-3 md:gap-4 mb-6">
        <div
          className="w-10 h-10 md:w-14 md:h-14 rounded-full flex items-center justify-center text-sm md:text-lg font-bold text-white shrink-0"
          style={{ backgroundColor: color }}
        >
          {id.slice(0, 2).toUpperCase()}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-base md:text-lg font-semibold text-gray-100 truncate">
              {agent.card?.name || id}
            </h2>
            <StatusBadge status={agent.agent_state || 'offline'} size="md" />
          </div>
          <div className="text-sm text-gray-400 mt-0.5">
            {roles.length > 0 && <span>{roles.join(' · ')}</span>}
            {agent.deployment && <span> &middot; {agent.deployment}</span>}
            {agent.card?.version && <span> &middot; v{agent.card.version}</span>}
          </div>
          <div className="text-xs text-gray-500 mt-1 break-all">
            {heartbeatInterval && <span>HB: {heartbeatInterval}s</span>}
            {agent.last_heartbeat && (
              <span
                className="ml-3"
                title={fullTimestamp(agent.last_heartbeat)}
              >
                Last heartbeat: {relativeTime(agent.last_heartbeat)}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Queue depth panel */}
      <div className="mb-4 bg-surface-50 border border-surface-200 rounded-lg p-3">
        <h3 className="text-xs font-medium text-gray-400 mb-2 flex items-center gap-1.5">
          <Inbox size={12} />
          Queue depth
        </h3>
        {queue ? (
          <div className="grid grid-cols-3 gap-2 text-xs">
            <div>
              <div className="text-gray-500">Pending</div>
              <div className="text-lg font-mono text-gray-200">
                {queue.pending ?? 0}
              </div>
            </div>
            <div>
              <div className="text-gray-500">Ack pending</div>
              <div className="text-lg font-mono text-gray-200">
                {queue.ack_pending ?? 0}
              </div>
            </div>
            <div>
              <div className="text-gray-500">Waiting</div>
              <div className="text-lg font-mono text-gray-200">
                {queue.num_waiting ?? 0}
              </div>
            </div>
          </div>
        ) : (
          <p className="text-xs text-gray-500">
            Consumer not yet bootstrapped (agent must register first).
          </p>
        )}
      </div>

      {/* Poison events panel */}
      <div className="mb-4 bg-surface-50 border border-surface-200 rounded-lg p-3">
        <h3 className="text-xs font-medium text-gray-400 mb-2 flex items-center gap-1.5">
          <AlertOctagon size={12} className="text-red-400" />
          Poison events
          <span className="ml-auto text-[10px] text-gray-500">{poison.length}</span>
        </h3>
        {poison.length > 0 ? (
          <div className="space-y-1.5 max-h-40 overflow-y-auto">
            {poison.map((evt, idx) => (
              <div
                key={`${evt.detected_at}-${idx}`}
                className="bg-surface-100 border border-surface-200 rounded p-2 text-xs"
              >
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-red-400">
                    {relativeTime(evt.detected_at)}
                  </span>
                  {evt.task_id && (
                    <span className="text-gray-500 font-mono">
                      task: {String(evt.task_id).slice(0, 8)}
                    </span>
                  )}
                  {evt.original_sender && (
                    <span className="text-gray-500">
                      from {evt.original_sender}
                    </span>
                  )}
                </div>
                {evt.consumer && (
                  <div className="text-[10px] text-gray-600 mt-0.5">
                    consumer: {evt.consumer}
                  </div>
                )}
              </div>
            ))}
          </div>
        ) : (
          <p className="text-xs text-gray-500">No poison advisories.</p>
        )}
      </div>

      {/* Skills */}
      {skills.length > 0 && (
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-400 mb-1">Skills</h3>
          <div className="flex flex-wrap gap-1">
            {skills.map((s) => (
              <span
                key={s.id || s.name}
                className="bg-surface-100 text-gray-300 px-2 py-0.5 rounded text-xs"
                title={s.description || ''}
              >
                {s.name || s.id}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Send command */}
      <div className="mb-4">
        <h3 className="text-xs font-medium text-gray-400 mb-1">Send Command</h3>
        <div className="flex gap-2">
          <input
            value={command}
            onChange={(e) => setCommand(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSendCommand()}
            placeholder="Enter command body..."
            className="flex-1 min-w-0 bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200"
          />
          <button
            onClick={handleSendCommand}
            disabled={!command.trim()}
            className="bg-accent hover:bg-accent-dark disabled:opacity-40 text-white px-3 py-1.5 rounded shrink-0"
          >
            <Send size={14} />
          </button>
        </div>
        {lastTaskId && (
          <p className="text-[10px] text-gray-500 mt-1 font-mono">
            Tracking task: {lastTaskId}
          </p>
        )}
      </div>

      {/* Recent messages */}
      <div className="mb-4">
        <h3 className="text-xs font-medium text-gray-400 mb-2">
          Recent Messages ({messages.length})
        </h3>
        <div className="space-y-1 max-h-60 overflow-y-auto">
          {messages.map((msg) => (
            <div
              key={msg.id}
              className="bg-surface-50 border border-surface-200 rounded p-2 text-xs"
            >
              <span className="text-gray-300">{msg.sender_id}</span>
              {msg.recipient_id && (
                <span className="text-gray-500"> &rarr; {msg.recipient_id}</span>
              )}
              <span className="text-gray-600 ml-2">[{msg.type}]</span>
              {msg.task_state && (
                <span className="text-gray-500 ml-2">{msg.task_state}</span>
              )}
              <span className="text-gray-600 ml-2">
                {relativeTime(msg.timestamp)}
              </span>
            </div>
          ))}
          {messages.length === 0 && (
            <p className="text-gray-500 text-xs">No messages</p>
          )}
        </div>
      </div>

      {/* Recent task results */}
      <div>
        <h3 className="text-xs font-medium text-gray-400 mb-2">
          Recent Results ({tasks.length})
        </h3>
        <div className="space-y-1 max-h-60 overflow-y-auto">
          {tasks.map((t) => (
            <div
              key={t.id}
              className="bg-surface-50 border border-surface-200 rounded p-2 text-xs flex items-center gap-2"
            >
              <span className="text-gray-400 font-mono shrink-0">
                {(t.task_id || '').slice(0, 8) || '—'}
              </span>
              <span className="text-gray-500 flex-1 truncate">
                {t.sender_id} &rarr; {t.recipient_id || '—'}
              </span>
              <span
                className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${
                  t.task_state === 'completed'
                    ? 'bg-green-500/20 text-green-400'
                    : t.task_state === 'failed' || t.task_state === 'rejected'
                      ? 'bg-red-500/20 text-red-400'
                      : 'bg-gray-500/20 text-gray-400'
                }`}
              >
                {t.task_state || 'unknown'}
              </span>
            </div>
          ))}
          {tasks.length === 0 && (
            <p className="text-gray-500 text-xs">No results</p>
          )}
        </div>
      </div>
    </div>
  )
}
